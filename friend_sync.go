package broadcaster

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

type FriendAPI interface {
	Friends(ctx context.Context) ([]Person, error)
	Follow(ctx context.Context, xuid string) error
	Unfollow(ctx context.Context, xuid string) error
}

type pendingFriendRequestAccepter interface {
	AcceptPendingFriendRequests(ctx context.Context) ([]Person, error)
}

// forceUnfollower drops a follower whose account restrictions prevent a
// friendship, so the person is not retried on every sync pass.
type forceUnfollower interface {
	ForceUnfollow(ctx context.Context, xuid string) error
}

type Inviter interface {
	Invite(ctx context.Context, xuid, titleID string) error
}

type HistoryStore interface {
	LastSeen(ctx context.Context, xuid string) (time.Time, bool, error)
	Clear(ctx context.Context, xuid string) error
}

type HistoryRecorder interface {
	Seen(ctx context.Context, xuid string, when time.Time) error
}

// HistoryLister enumerates all recorded XUIDs so history can be reconciled
// against the current friend list.
type HistoryLister interface {
	XUIDs(ctx context.Context) ([]string, error)
}

type FriendSyncer struct {
	Client  FriendAPI
	Config  FriendSyncConfig
	Inviter Inviter
	History HistoryStore
	// Notifier receives operator-facing notifications such as friend
	// restriction removals. It may be nil.
	Notifier Notifier
	// PruneHistory removes history entries for people missing from this
	// syncer's friend list. Enable it on exactly one syncer per shared
	// HistoryStore, or entries maintained by other accounts get dropped.
	PruneHistory bool
	Log          *slog.Logger
}

const friendListFullBackoff = time.Hour

type friendSyncOptions struct {
	expire       bool
	autoFollow   bool
	autoUnfollow bool
}

// friendSyncRunState tracks per-operation rate-limit backoff between runs.
// Follow and unfollow limits are tracked separately because Xbox applies them
// independently; a rate-limited add pass must not stall removals or scans.
type friendSyncRunState struct {
	followRetryUntil   time.Time
	unfollowRetryUntil time.Time
	autoFollowUntil    time.Time
}

func (s *friendSyncRunState) options(now time.Time, expire bool) friendSyncOptions {
	return friendSyncOptions{
		expire:       expire,
		autoFollow:   !now.Before(s.followRetryUntil) && !now.Before(s.autoFollowUntil),
		autoUnfollow: !now.Before(s.unfollowRetryUntil),
	}
}

func (s *friendSyncRunState) record(now time.Time, result friendSyncResult) {
	if result.followRetryAfter > 0 {
		s.followRetryUntil = now.Add(result.followRetryAfter)
	}
	if result.unfollowRetryAfter > 0 {
		s.unfollowRetryUntil = now.Add(result.unfollowRetryAfter)
	}
	if result.friendListFull {
		s.autoFollowUntil = now.Add(friendListFullBackoff)
	}
}

// friendSyncResult carries the rate-limit outcomes of a sync pass.
type friendSyncResult struct {
	followRetryAfter   time.Duration
	unfollowRetryAfter time.Duration
	friendListFull     bool
}

func (s FriendSyncer) Sync(ctx context.Context) error {
	_, err := s.syncWithOptions(ctx, friendSyncOptions{
		expire:       true,
		autoFollow:   true,
		autoUnfollow: true,
	})
	return err
}

func (s FriendSyncer) syncWithOptions(ctx context.Context, opts friendSyncOptions) (friendSyncResult, error) {
	var result friendSyncResult
	if s.Client == nil {
		return result, nil
	}
	if s.Config.InitialInvite && s.Inviter == nil {
		s.debug(ctx, "initial invite unavailable", "reason", "session inviter is not configured")
	}
	if s.Config.AutoFollow && opts.autoFollow {
		s.acceptPending(ctx, &result)
	}
	people, err := s.Client.Friends(ctx)
	if err != nil {
		return result, err
	}
	stats := s.friendSyncStats(people, opts)
	s.debug(ctx, "friend sync scan",
		"people", stats.people,
		"followers", stats.followers,
		"following", stats.following,
		"auto_follow_candidates", stats.autoFollowCandidates,
		"auto_unfollow_candidates", stats.autoUnfollowCandidates,
		"expire", opts.expire,
	)
	if stats.autoFollowCandidates > 0 {
		s.debug(ctx, "adding friends", "count", stats.autoFollowCandidates)
	}
	if stats.autoUnfollowCandidates > 0 {
		s.debug(ctx, "removing friends", "count", stats.autoUnfollowCandidates)
	}
	added := 0
	removed := 0
	for _, p := range people {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if isGuestXUID(p.XUID) {
			continue
		}
		if s.Config.AutoFollow && opts.autoFollow && !result.followBlocked() && p.IsFollowingCaller && !p.IsFollowedByCaller {
			if s.follow(ctx, p, &result) {
				added++
			}
		}
		if s.Config.AutoUnfollow && opts.autoUnfollow && !result.unfollowBlocked() && !p.IsFollowingCaller && p.IsFollowedByCaller {
			if s.unfollow(ctx, p, "", &result) {
				removed++
			}
			continue
		}
		if opts.expire && opts.autoUnfollow && !result.unfollowBlocked() && s.Config.ExpiryEnabled && p.IsFollowedByCaller && s.History != nil {
			if s.expire(ctx, p, &result) {
				removed++
			}
		}
	}
	if opts.expire && s.Config.ExpiryEnabled && s.PruneHistory {
		s.pruneHistory(ctx, people)
	}
	if stats.autoFollowCandidates > 0 {
		s.debug(ctx, "added friends", "count", added)
	}
	if stats.autoUnfollowCandidates > 0 {
		s.debug(ctx, "removed friends", "count", removed)
	}
	return result, nil
}

func (r friendSyncResult) followBlocked() bool {
	return r.followRetryAfter > 0 || r.friendListFull
}

func (r friendSyncResult) unfollowBlocked() bool {
	return r.unfollowRetryAfter > 0
}

// acceptPending accepts incoming friend requests, sending initial invites for
// each accepted person when configured.
func (s FriendSyncer) acceptPending(ctx context.Context, result *friendSyncResult) {
	accepter, ok := s.Client.(pendingFriendRequestAccepter)
	if !ok {
		return
	}
	s.debug(ctx, "accepting pending friend requests")
	accepted, err := accepter.AcceptPendingFriendRequests(ctx)
	for _, p := range accepted {
		s.info(ctx, "added friend", "xuid", p.XUID, "gamertag", p.Gamertag, "source", "pending_requests")
	}
	if s.Config.InitialInvite && s.Inviter != nil {
		for _, p := range accepted {
			s.sendInitialInvite(ctx, p, "pending_requests")
		}
	}
	if err != nil {
		if delay := retryDelay(err); delay > 0 {
			result.followRetryAfter = delay
		}
		s.logPendingFriendAcceptError(err)
	}
}

// follow follows p back and reports whether the friendship was established.
// Restricted accounts are force-unfollowed so they are not retried forever.
func (s FriendSyncer) follow(ctx context.Context, p Person, result *friendSyncResult) bool {
	err := s.Client.Follow(ctx, p.XUID)
	if err == nil {
		s.info(ctx, "added friend", "xuid", p.XUID, "gamertag", p.Gamertag)
		if s.Config.InitialInvite && s.Inviter != nil {
			s.sendInitialInvite(ctx, p, "auto_follow")
		}
		return true
	}
	s.logFriendSyncError("follow", p, err)
	s.debug(ctx, "failed to add friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	switch {
	case errors.Is(err, xblsocial.ErrFriendRestricted):
		s.dropRestrictedFollower(ctx, p)
	case errors.Is(err, xblsocial.ErrFriendListFull):
		result.friendListFull = true
	default:
		if delay := retryDelay(err); delay > 0 {
			result.followRetryAfter = delay
		}
	}
	return false
}

// dropRestrictedFollower removes a privacy-restricted follower so the account
// stops showing up as an auto-follow candidate on every pass.
func (s FriendSyncer) dropRestrictedFollower(ctx context.Context, p Person) {
	unfollower, ok := s.Client.(forceUnfollower)
	if !ok {
		return
	}
	if err := unfollower.ForceUnfollow(ctx, p.XUID); err != nil {
		if s.Log != nil {
			s.Log.Error("force unfollow restricted account", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		}
		return
	}
	if s.History != nil {
		_ = s.History.Clear(ctx, p.XUID)
	}
	s.warn(ctx, "removed friend due to restrictions on their account", "xuid", p.XUID, "gamertag", p.Gamertag)
	s.notify(ctx, "Removed "+p.Gamertag+" ("+p.XUID+") as a friend due to restrictions on their account.")
}

// unfollow removes p and reports whether the removal succeeded.
func (s FriendSyncer) unfollow(ctx context.Context, p Person, reason string, result *friendSyncResult) bool {
	if err := s.Client.Unfollow(ctx, p.XUID); err != nil {
		s.debug(ctx, "failed to remove friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		if delay := retryDelay(err); delay > 0 {
			result.unfollowRetryAfter = delay
		}
		return false
	}
	if reason == "" {
		s.info(ctx, "removed friend", "xuid", p.XUID, "gamertag", p.Gamertag)
	} else {
		s.info(ctx, "removed friend", "xuid", p.XUID, "gamertag", p.Gamertag, "reason", reason)
	}
	if s.History != nil {
		_ = s.History.Clear(ctx, p.XUID)
	}
	return true
}

// expire removes p when they have not been seen within the expiry window and
// reports whether a removal happened.
func (s FriendSyncer) expire(ctx context.Context, p Person, result *friendSyncResult) bool {
	lastSeen, ok, err := s.History.LastSeen(ctx, p.XUID)
	if err != nil {
		if s.Log != nil {
			s.Log.Error("read player history", "xuid", p.XUID, "err", err)
		}
		return false
	}
	if !ok {
		if recorder, ok := s.History.(HistoryRecorder); ok {
			if err := recorder.Seen(ctx, p.XUID, time.Now()); err != nil && s.Log != nil {
				s.Log.Error("record player history", "xuid", p.XUID, "err", err)
			}
		}
		return false
	}
	expiryDays := s.Config.ExpiryDays
	if expiryDays <= 0 {
		expiryDays = 15
	}
	if !lastSeen.Before(time.Now().Add(-time.Duration(expiryDays) * 24 * time.Hour)) {
		return false
	}
	s.info(ctx, "removing inactive friend", "xuid", p.XUID, "gamertag", p.Gamertag, "last_seen", lastSeen)
	return s.unfollow(ctx, p, "inactive", result)
}

// pruneHistory drops history entries for people who are no longer on the
// friend list so the store does not grow forever.
func (s FriendSyncer) pruneHistory(ctx context.Context, people []Person) {
	lister, ok := s.History.(HistoryLister)
	if !ok {
		return
	}
	xuids, err := lister.XUIDs(ctx)
	if err != nil {
		if s.Log != nil {
			s.Log.Error("list player history", "err", err)
		}
		return
	}
	current := make(map[string]struct{}, len(people))
	for _, p := range people {
		current[p.XUID] = struct{}{}
	}
	for _, xuid := range xuids {
		if _, ok := current[xuid]; ok {
			continue
		}
		if err := s.History.Clear(ctx, xuid); err != nil {
			if s.Log != nil {
				s.Log.Error("prune player history", "xuid", xuid, "err", err)
			}
			continue
		}
		s.debug(ctx, "pruned player history for ex-friend", "xuid", xuid)
	}
}

func (s FriendSyncer) sendInitialInvite(ctx context.Context, p Person, source string) {
	s.debug(ctx, "sending initial invite", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source)
	if err := s.Inviter.Invite(ctx, p.XUID, strconv.FormatInt(TitleID, 10)); err != nil {
		if s.Log != nil {
			s.Log.Warn("send initial invite", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source, "err", err)
		}
		return
	}
	s.debug(ctx, "sent initial invite", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source)
}

type friendSyncStats struct {
	people                 int
	followers              int
	following              int
	autoFollowCandidates   int
	autoUnfollowCandidates int
}

func (s FriendSyncer) friendSyncStats(people []Person, opts friendSyncOptions) friendSyncStats {
	stats := friendSyncStats{people: len(people)}
	for _, p := range people {
		if isGuestXUID(p.XUID) {
			continue
		}
		if p.IsFollowingCaller {
			stats.followers++
		}
		if p.IsFollowedByCaller {
			stats.following++
		}
		if s.Config.AutoFollow && opts.autoFollow && p.IsFollowingCaller && !p.IsFollowedByCaller {
			stats.autoFollowCandidates++
		}
		if s.Config.AutoUnfollow && opts.autoUnfollow && !p.IsFollowingCaller && p.IsFollowedByCaller {
			stats.autoUnfollowCandidates++
		}
	}
	return stats
}

func (s FriendSyncer) logFriendSyncError(op string, p Person, err error) {
	if s.Log == nil {
		return
	}
	switch {
	case errors.Is(err, xblsocial.ErrFriendListFull):
		s.Log.Warn("friend list full while syncing friends", "op", op, "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	case errors.Is(err, xblsocial.ErrFriendRestricted):
		s.Log.Warn("friend restricted while syncing friends", "op", op, "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	}
}

func (s FriendSyncer) logPendingFriendAcceptError(err error) {
	if s.Log != nil {
		s.Log.Warn("accept pending friend requests", "err", err)
	}
}

func (s FriendSyncer) notify(ctx context.Context, message string) {
	if s.Notifier == nil {
		return
	}
	notifyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := s.Notifier.Notify(notifyCtx, message); err != nil && s.Log != nil {
		s.Log.Error("send notification", "err", err)
	}
}

func (s FriendSyncer) info(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.InfoContext(ctx, msg, args...)
	}
}

func (s FriendSyncer) warn(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.WarnContext(ctx, msg, args...)
	}
}

func (s FriendSyncer) debug(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.DebugContext(ctx, msg, args...)
	}
}

func retryDelay(err error) time.Duration {
	var responseErr *xblsocial.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.RetryAfter
	}
	return 0
}

func (s FriendSyncer) Run(ctx context.Context) {
	interval := s.Config.UpdateInterval
	if interval < 20*time.Second {
		interval = 20 * time.Second
	}
	state := friendSyncRunState{}
	s.runSync(ctx, &state, true)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var expiryTicker *time.Ticker
	if s.Config.ExpiryEnabled {
		expiryInterval := s.Config.ExpiryCheck
		if expiryInterval < 20*time.Second {
			expiryInterval = 20 * time.Second
		}
		expiryTicker = time.NewTicker(expiryInterval)
		defer expiryTicker.Stop()
	}
	var expiryC <-chan time.Time
	if expiryTicker != nil {
		expiryC = expiryTicker.C
	}
	for {
		select {
		case <-ticker.C:
			s.runSync(ctx, &state, false)
		case <-expiryC:
			s.runSync(ctx, &state, true)
		case <-ctx.Done():
			return
		}
	}
}

// runSync executes one sync pass. Rate-limit backoff only suppresses the
// affected mutation type; scans and the other mutation type keep running.
func (s FriendSyncer) runSync(ctx context.Context, state *friendSyncRunState, expire bool) {
	if state == nil {
		state = &friendSyncRunState{}
	}
	opts := state.options(time.Now(), expire)
	s.debug(ctx, "friend sync tick", "expire", expire, "auto_follow", opts.autoFollow, "auto_unfollow", opts.autoUnfollow)
	result, err := s.syncWithOptions(ctx, opts)
	state.record(time.Now(), result)
	if err != nil && s.Log != nil && !errors.Is(err, context.Canceled) {
		s.Log.Error("sync friends", "err", err)
	}
}

func isGuestXUID(xuid string) bool {
	n, err := strconv.ParseUint(xuid, 10, 64)
	if err != nil {
		return false
	}
	return n>>52 == 1
}

type sessionInviter struct {
	session *mpsd.Session
}

func (i sessionInviter) Invite(ctx context.Context, xuid, titleID string) error {
	_, err := i.session.Invite(ctx, xuid, titleID)
	return err
}
