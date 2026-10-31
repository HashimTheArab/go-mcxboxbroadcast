package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// FriendAPI is the account's friend list as the syncer sees it.
type FriendAPI interface {
	Friends(ctx context.Context) ([]Person, error)
	// AcceptPendingFriendRequests returns who was accepted; failed people are
	// reported through *AcceptFriendRequestsError.
	AcceptPendingFriendRequests(ctx context.Context) ([]Person, error)
	AddFriend(ctx context.Context, xuid string) error
	RemoveFriend(ctx context.Context, xuid string) error
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
	// OwnXUIDs returns the broadcaster's own accounts, which never join and
	// so are never expired.
	OwnXUIDs func() []string
	// Trigger requests an early sync, subject to scan spacing and Retry-After.
	// A nil Trigger leaves the syncer purely periodic.
	Trigger <-chan struct{}
	Log     *slog.Logger

	friendCount *accountHealthState // nil outside a Broadcaster
}

const (
	friendListFullBackoff = time.Hour
	friendSyncMinInterval = 20 * time.Second
)

type friendSyncOptions struct {
	expire bool
	add    bool
	remove bool
}

// friendSyncRunState tracks per-operation rate-limit backoff between runs.
// Add and remove limits are tracked separately because Xbox applies them
// independently; a rate-limited add pass must not stall expiry.
type friendSyncRunState struct {
	addRetryUntil    time.Time
	removeRetryUntil time.Time
	listFullUntil    time.Time
}

func (s *friendSyncRunState) options(now time.Time, expire bool) friendSyncOptions {
	return friendSyncOptions{
		expire: expire,
		add:    !now.Before(s.addRetryUntil) && !now.Before(s.listFullUntil),
		remove: !now.Before(s.removeRetryUntil),
	}
}

func (s *friendSyncRunState) record(now time.Time, result friendSyncResult) {
	if result.addRetryAfter > 0 {
		s.addRetryUntil = now.Add(result.addRetryAfter)
	}
	if result.removeRetryAfter > 0 {
		s.removeRetryUntil = now.Add(result.removeRetryAfter)
	}
	if result.friendListFull {
		s.listFullUntil = now.Add(friendListFullBackoff)
	}
}

// friendSyncResult carries the rate-limit outcomes of a sync pass.
type friendSyncResult struct {
	readRetryAfter   time.Duration
	addRetryAfter    time.Duration
	removeRetryAfter time.Duration
	friendListFull   bool
}

func (r friendSyncResult) addBlocked() bool {
	return r.addRetryAfter > 0 || r.friendListFull
}

// Sync accepts pending friend requests and expires inactive friends once.
func (s FriendSyncer) Sync(ctx context.Context) error {
	_, err := s.syncWithOptions(ctx, friendSyncOptions{expire: true, add: true, remove: true})
	return err
}

func (s FriendSyncer) syncWithOptions(ctx context.Context, opts friendSyncOptions) (friendSyncResult, error) {
	var result friendSyncResult
	if s.Client == nil {
		return result, nil
	}
	if s.Config.AutoFriend && opts.add {
		s.acceptPending(ctx, &result)
		if result.readRetryAfter > 0 {
			return result, nil
		}
	}
	expire := opts.expire && s.Config.ExpiryEnabled && s.History != nil
	// Lists friends for the count even without expiry, inside this serialized loop.
	if !expire && !s.friendCount.needsFriends() {
		return result, nil
	}
	operationCtx, cancel := xboxOperationContext(ctx)
	friends, err := s.Client.Friends(operationCtx)
	cancel()
	if err != nil {
		result.readRetryAfter = retryDelay(err)
		return result, err
	}
	s.friendCount.setFriends(len(friends))
	if !expire {
		return result, nil
	}
	s.debug(ctx, "friend expiry scan", "friends", fmt.Sprintf("%d/%d", len(friends), MaxFriends))
	own := make(map[string]struct{})
	if s.OwnXUIDs != nil {
		for _, xuid := range s.OwnXUIDs() {
			own[xuid] = struct{}{}
		}
	}
	expired := 0
	for _, p := range friends {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if _, ok := own[p.XUID]; ok || isGuestXUID(p.XUID) {
			continue
		}
		if !opts.remove || result.removeRetryAfter > 0 {
			break
		}
		if s.expire(ctx, p, &result) {
			expired++
		}
	}
	if s.PruneHistory {
		s.pruneHistory(ctx, friends)
	}
	s.debug(ctx, "friend expiry scan done", "expired", expired)
	return result, nil
}

// acceptPending accepts incoming friend requests. Requests the bulk accept
// could not update are retried one by one to find restricted accounts.
func (s FriendSyncer) acceptPending(ctx context.Context, result *friendSyncResult) {
	s.debug(ctx, "accepting pending friend requests")
	operationCtx, cancel := xboxOperationContext(ctx)
	accepted, err := s.Client.AcceptPendingFriendRequests(operationCtx)
	cancel()
	for _, p := range accepted {
		s.added(ctx, p, "pending_requests")
	}
	var failed *AcceptFriendRequestsError
	switch {
	case err == nil:
	case errors.As(err, &failed):
		for _, p := range failed.Failed {
			if result.addBlocked() {
				break
			}
			s.addFriend(ctx, p, result)
		}
	default:
		if delay := retryDelay(err); delay > 0 {
			var responseErr *xblsocial.ResponseError
			if errors.As(err, &responseErr) && responseErr.Method == http.MethodGet {
				// The pending list shares PeopleHub's read quota with Friends.
				result.readRetryAfter = delay
			} else {
				result.addRetryAfter = delay
			}
		}
		if s.Log != nil {
			s.Log.Warn("accept pending friend requests", "err", err)
		}
	}
}

// addFriend accepts one request. Restricted accounts have their request
// declined so they are not retried forever, like MCXboxBroadcast.
func (s FriendSyncer) addFriend(ctx context.Context, p Person, result *friendSyncResult) {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.AddFriend(operationCtx, p.XUID)
	cancel()
	switch {
	case err == nil:
		s.added(ctx, p, "retry")
	case errors.Is(err, xblsocial.ErrFriendRestricted):
		s.declineRestricted(ctx, p)
	case errors.Is(err, xblsocial.ErrFriendListFull):
		if s.Log != nil {
			s.Log.Error("friend list full; unable to add friend", "xuid", p.XUID, "gamertag", p.Gamertag)
		}
		result.friendListFull = true
	default:
		if delay := retryDelay(err); delay > 0 {
			result.addRetryAfter = delay
		}
		if s.Log != nil {
			s.Log.Warn("add friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		}
	}
}

// added starts p's expiry clock and sends the initial invite.
func (s FriendSyncer) added(ctx context.Context, p Person, source string) {
	s.friendCount.addFriends(1)
	s.info(ctx, "added friend", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source)
	if recorder, ok := s.History.(HistoryRecorder); ok {
		if err := recorder.Seen(ctx, p.XUID, time.Now()); err != nil && s.Log != nil {
			s.Log.Error("record player history", "xuid", p.XUID, "err", err)
		}
	}
	if s.Config.InitialInvite && s.Inviter != nil {
		s.sendInitialInvite(ctx, p, source)
	}
}

func (s FriendSyncer) declineRestricted(ctx context.Context, p Person) {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.RemoveFriend(operationCtx, p.XUID)
	cancel()
	if err != nil && s.Log != nil {
		s.Log.Debug("decline restricted friend request", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	}
	s.warn(ctx, "unable to add friend due to restrictions on their account", "xuid", p.XUID, "gamertag", p.Gamertag)
	s.notify(ctx, p.Gamertag+" ("+p.XUID+") has restrictions in place that prevent them from being friends with our account.")
}

// expire removes p when they have not been seen within the expiry window and
// reports whether they were removed.
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
	operationCtx, cancel := xboxOperationContext(ctx)
	err = s.Client.RemoveFriend(operationCtx, p.XUID)
	cancel()
	if err != nil {
		s.debug(ctx, "failed to remove friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		if delay := retryDelay(err); delay > 0 {
			result.removeRetryAfter = delay
		}
		return false
	}
	s.friendCount.addFriends(-1)
	s.info(ctx, "removed friend due to inactivity", "xuid", p.XUID, "gamertag", p.Gamertag, "last_seen", lastSeen)
	if err := s.History.Clear(ctx, p.XUID); err != nil && s.Log != nil {
		s.Log.Error("clear player history", "xuid", p.XUID, "err", err)
	}
	return true
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
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Inviter.Invite(operationCtx, p.XUID, strconv.FormatInt(TitleID, 10))
	cancel()
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("send initial invite", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source, "err", err)
		}
		return
	}
	s.debug(ctx, "sent initial invite", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source)
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
		if responseErr.RetryAfter > 0 {
			return responseErr.RetryAfter
		}
		if errors.Is(err, xblsocial.ErrRateLimited) {
			return friendSyncMinInterval
		}
	}
	return 0
}

// Run combines events, polling, and expiry into spaced sync passes.
func (s FriendSyncer) Run(ctx context.Context) {
	interval := max(s.Config.UpdateInterval, friendSyncMinInterval)
	expiryInterval := max(s.Config.ExpiryCheck, friendSyncMinInterval)
	state := friendSyncRunState{}
	nextPoll, nextExpiry := time.Now(), time.Now()
	var nextScan time.Time
	pending, expire := true, true
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case _, ok := <-s.Trigger:
			if !ok {
				s.Trigger = nil
			} else {
				pending = true
			}
		}
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		pending = pending || !now.Before(nextPoll)
		if s.Config.ExpiryEnabled && !now.Before(nextExpiry) {
			pending, expire = true, true
		}
		if pending && !now.Before(nextScan) {
			retryAfter := s.runSync(ctx, &state, expire)
			now = time.Now()
			nextScan = now.Add(max(friendSyncMinInterval, retryAfter))
			nextPoll = now.Add(interval)
			pending = retryAfter > 0
			if !pending {
				if expire {
					nextExpiry = now.Add(expiryInterval)
				}
				expire = false
			}
		}

		nextWake := nextPoll
		if s.Config.ExpiryEnabled && nextExpiry.Before(nextWake) {
			nextWake = nextExpiry
		}
		if pending || nextWake.Before(nextScan) {
			nextWake = nextScan
		}
		timer.Reset(time.Until(nextWake))
	}
}

// runSync updates mutation backoff and returns any delay needed before reading again.
func (s FriendSyncer) runSync(ctx context.Context, state *friendSyncRunState, expire bool) time.Duration {
	opts := state.options(time.Now(), expire)
	s.debug(ctx, "friend sync tick", "expire", expire, "add", opts.add, "remove", opts.remove)
	result, err := s.syncWithOptions(ctx, opts)
	state.record(time.Now(), result)
	if err != nil && s.Log != nil && !errors.Is(err, context.Canceled) {
		s.Log.Error("sync friends", "err", err)
	}
	return result.readRetryAfter
}

func isGuestXUID(xuid string) bool {
	n, err := strconv.ParseUint(xuid, 10, 64)
	if err != nil {
		return false
	}
	return n>>52 == 1
}
