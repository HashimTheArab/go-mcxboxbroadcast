package broadcaster

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// FriendAPI is the Xbox social surface a FriendSyncer drives.
type FriendAPI interface {
	Friends(ctx context.Context) ([]Person, error)
	Follow(ctx context.Context, xuid string) error
	// Unfollow drops the account's own follow of xuid.
	Unfollow(ctx context.Context, xuid string) error
	// RemoveFriend ends a friendship with xuid or declines their pending request.
	RemoveFriend(ctx context.Context, xuid string) error
	// RemoveFollower drops xuid's follow of the account.
	RemoveFollower(ctx context.Context, xuid string) error
}

// friendRequestAccepter accepts incoming friend requests, leaving those for
// which skip reports true untouched.
type friendRequestAccepter interface {
	AcceptPendingFriendRequests(ctx context.Context, skip func(xuid string) bool) (FriendRequestResult, error)
}

type Inviter interface {
	Invite(ctx context.Context, xuid, titleID string) error
}

// HistoryStore records when each account's friends were last seen, so cleanup
// can remove inactive friends and those seen least recently first.
type HistoryStore interface {
	// LastSeen returns the friends tracked for account and when each was last seen.
	LastSeen(ctx context.Context, account string) (map[string]time.Time, error)
	// Track starts tracking xuids for account at when; tracked XUIDs keep their time.
	Track(ctx context.Context, account string, when time.Time, xuids ...string) error
	// Seen records activity by xuid for every account tracking it.
	Seen(ctx context.Context, xuid string, when time.Time) error
	// MarkRemoving records at when that account ended its friendship with
	// xuids while their follow of it remains, replacing their LastSeen entries.
	MarkRemoving(ctx context.Context, account string, when time.Time, xuids ...string) error
	// Removing returns the XUIDs account marked with MarkRemoving and when.
	Removing(ctx context.Context, account string) (map[string]time.Time, error)
	// Forget stops tracking xuids for account, including removal marks.
	Forget(ctx context.Context, account string, xuids ...string) error
}

// FriendSyncer keeps one account's friend list in sync. Its methods keep
// short-lived state between passes, so use one FriendSyncer per account.
type FriendSyncer struct {
	Client  FriendAPI
	Config  FriendSyncConfig
	Inviter Inviter
	History HistoryStore
	// Account is the syncing account's XUID; it keys this syncer's History entries.
	Account string
	// OwnAccounts lists the broadcaster's own XUIDs, which cleanup and
	// auto-unfollow never remove.
	OwnAccounts []string
	// Notifier receives operator-facing notifications such as friend
	// restriction removals. It may be nil.
	Notifier Notifier
	// Trigger requests an early sync, subject to scan spacing and Retry-After.
	// A nil Trigger leaves the syncer purely periodic.
	Trigger <-chan struct{}
	Log     *slog.Logger

	state friendSyncRunState
}

const (
	friendListFullBackoff = time.Hour
	friendSyncMinInterval = 20 * time.Second
	// friendRequestRetryDelay spaces retries of a request Xbox rejected.
	friendRequestRetryDelay = 15 * time.Minute
	// friendAcceptConfirmWindow bounds how long an accepted request may take
	// to show up on the friend list before it is retried.
	friendAcceptConfirmWindow = time.Hour
	// friendInviteMemory stops repeat initial invites to the same person.
	friendInviteMemory = time.Hour
)

type friendSyncOptions struct {
	cleanup      bool
	autoFollow   bool
	autoUnfollow bool
}

// friendSyncRunState is kept between passes. Follow and unfollow limits are
// tracked separately because Xbox applies them independently; a rate-limited
// add pass must not stall removals or scans.
type friendSyncRunState struct {
	followRetryUntil   time.Time
	unfollowRetryUntil time.Time
	autoFollowUntil    time.Time

	invited  map[string]time.Time             // XUID -> initial invite sent
	accepted map[string]acceptedFriendRequest // accepted, not yet on the friend list
	rejected map[string]time.Time             // XUID -> request retry allowed
}

type acceptedFriendRequest struct {
	person Person
	at     time.Time
}

func (s *friendSyncRunState) options(now time.Time, cleanup bool) friendSyncOptions {
	return friendSyncOptions{
		cleanup:      cleanup,
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
	if result.friendListFull && !result.madeRoom {
		s.autoFollowUntil = now.Add(friendListFullBackoff)
	}
}

// forget drops expired memory and makes sure the maps exist.
func (s *friendSyncRunState) forget(now time.Time) {
	if s.invited == nil {
		s.invited = map[string]time.Time{}
		s.accepted = map[string]acceptedFriendRequest{}
		s.rejected = map[string]time.Time{}
	}
	for xuid, at := range s.invited {
		if now.Sub(at) >= friendInviteMemory {
			delete(s.invited, xuid)
		}
	}
	for xuid, req := range s.accepted {
		if now.Sub(req.at) >= friendAcceptConfirmWindow {
			delete(s.accepted, xuid)
		}
	}
	for xuid, until := range s.rejected {
		if !now.Before(until) {
			delete(s.rejected, xuid)
		}
	}
}

// friendSyncResult carries the outcomes of a sync pass.
type friendSyncResult struct {
	readRetryAfter     time.Duration
	followRetryAfter   time.Duration
	unfollowRetryAfter time.Duration
	friendListFull     bool
	waiting            int  // incoming friend requests left pending
	madeRoom           bool // cleanup removed friends after the list was full
}

// Sync runs one full pass, ignoring backoff from earlier passes.
func (s *FriendSyncer) Sync(ctx context.Context) error {
	_, err := s.syncWithOptions(ctx, friendSyncOptions{
		cleanup:      true,
		autoFollow:   true,
		autoUnfollow: true,
	})
	return err
}

func (s *FriendSyncer) syncWithOptions(ctx context.Context, opts friendSyncOptions) (friendSyncResult, error) {
	var result friendSyncResult
	if s.Client == nil {
		return result, nil
	}
	s.state.forget(time.Now())
	if s.Config.InitialInvite && s.Inviter == nil {
		s.debug(ctx, "initial invite unavailable", "reason", "session inviter is not configured")
	}
	// Accept requests before following anyone back.
	if s.Config.AutoFollow && opts.autoFollow {
		s.acceptPending(ctx, opts, &result)
		if result.readRetryAfter > 0 {
			return result, nil
		}
	}
	operationCtx, cancel := xboxOperationContext(ctx)
	people, err := s.Client.Friends(operationCtx)
	cancel()
	if err != nil {
		result.readRetryAfter = retryDelay(err)
		return result, err
	}
	s.confirmAccepted(ctx, people)
	own := s.ownAccounts()
	removing := s.removing(ctx)
	stats := s.friendSyncStats(people, opts)
	s.debug(ctx, "friend sync scan",
		"people", stats.people,
		"followers", stats.followers,
		"following", stats.following,
		"followers_only", stats.followersOnly,
		"following_only", stats.followingOnly,
		"mutual", stats.mutual,
		"neither", stats.neither,
		"auto_follow_candidates", stats.autoFollowCandidates,
		"auto_unfollow_candidates", stats.autoUnfollowCandidates,
		"cleanup", opts.cleanup,
	)
	if stats.autoFollowCandidates > 0 {
		s.debug(ctx, "adding friends", "count", stats.autoFollowCandidates)
	}
	if stats.autoUnfollowCandidates > 0 {
		s.debug(ctx, "removing friends", "count", stats.autoUnfollowCandidates)
	}
	added := 0
	removed := 0
	unfollowed := make(map[string]struct{})
	for _, p := range people {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if isGuestXUID(p.XUID) {
			continue
		}
		if _, ok := removing[p.XUID]; ok && p.IsFollowingCaller && !p.IsFollowedByCaller {
			// A removed friend who still follows must not be followed back.
			if opts.autoUnfollow && !result.unfollowBlocked() {
				s.finishRemoval(ctx, p, &result)
			}
			continue
		}
		if s.Config.AutoFollow && opts.autoFollow && !result.followBlocked() && p.IsFollowingCaller && !p.IsFollowedByCaller {
			if s.follow(ctx, p, opts, &result) {
				added++
			}
		}
		if _, isOwn := own[p.XUID]; isOwn {
			continue
		}
		if s.Config.AutoUnfollow && opts.autoUnfollow && !result.unfollowBlocked() && !p.IsFollowingCaller && p.IsFollowedByCaller {
			if s.unfollow(ctx, p, &result) {
				removed++
				unfollowed[p.XUID] = struct{}{}
			}
		}
	}
	removed += s.cleanup(ctx, people, own, unfollowed, removing, opts, &result)
	if stats.autoFollowCandidates > 0 {
		s.debug(ctx, "added friends", "count", added)
	}
	if stats.autoUnfollowCandidates > 0 || removed > 0 {
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

// ownAccounts returns the XUIDs cleanup and auto-unfollow must never remove.
func (s *FriendSyncer) ownAccounts() map[string]struct{} {
	own := make(map[string]struct{}, len(s.OwnAccounts)+1)
	for _, xuid := range append([]string{s.Account}, s.OwnAccounts...) {
		if xuid != "" {
			own[xuid] = struct{}{}
		}
	}
	return own
}

// acceptPending accepts incoming friend requests. Accepted people are
// announced once they appear on the friend list; see confirmAccepted.
func (s *FriendSyncer) acceptPending(ctx context.Context, opts friendSyncOptions, result *friendSyncResult) {
	accepter, ok := s.Client.(friendRequestAccepter)
	if !ok {
		return
	}
	now := time.Now()
	skip := func(xuid string) bool {
		_, awaiting := s.state.accepted[xuid]
		return awaiting || now.Before(s.state.rejected[xuid])
	}
	s.debug(ctx, "accepting pending friend requests")
	operationCtx, cancel := xboxOperationContext(ctx)
	requests, err := accepter.AcceptPendingFriendRequests(operationCtx, skip)
	cancel()
	for _, p := range requests.Accepted {
		s.state.accepted[p.XUID] = acceptedFriendRequest{person: p, at: now}
		s.debug(ctx, "accepted friend request", "xuid", p.XUID, "gamertag", p.Gamertag)
	}
	result.waiting = requests.Waiting
	for _, rejected := range requests.Rejected {
		if s.rejectRequest(ctx, rejected, opts, result) {
			result.waiting--
		}
	}
	if requests.ListFull {
		s.warn(ctx, "friend list full while accepting friend requests", "waiting", result.waiting)
		result.friendListFull = true
	}
	if err != nil {
		if delay := retryDelay(err); delay > 0 {
			var responseErr *xblsocial.ResponseError
			if errors.As(err, &responseErr) && responseErr.Method == http.MethodGet {
				// The pending list shares PeopleHub's read quota with Friends.
				result.readRetryAfter = delay
			} else {
				result.followRetryAfter = delay
			}
		}
		s.warn(ctx, "accept pending friend requests", "err", err)
	}
}

// rejectRequest handles a request Xbox refused and reports whether it is no
// longer pending. Restricted requests are declined like restricted followers;
// others are retried after friendRequestRetryDelay.
func (s *FriendSyncer) rejectRequest(ctx context.Context, rejected RejectedFriendRequest, opts friendSyncOptions, result *friendSyncResult) bool {
	p := rejected.Person
	if errors.Is(rejected.Err, xblsocial.ErrFriendRestricted) && opts.autoUnfollow && !result.unfollowBlocked() {
		operationCtx, cancel := xboxOperationContext(ctx)
		err := s.Client.RemoveFriend(operationCtx, p.XUID)
		cancel()
		if err == nil {
			s.warn(ctx, "declined friend request due to restrictions on their account", "xuid", p.XUID, "gamertag", p.Gamertag)
			s.notify(ctx, "Declined a friend request from "+p.Gamertag+" ("+p.XUID+") due to restrictions on their account.")
			return true
		}
		result.unfollowRetryAfter = max(result.unfollowRetryAfter, retryDelay(err))
		s.warn(ctx, "decline restricted friend request", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	}
	s.state.rejected[p.XUID] = time.Now().Add(friendRequestRetryDelay)
	s.warn(ctx, "friend request not accepted; retrying later", "xuid", p.XUID, "gamertag", p.Gamertag, "retry_in", friendRequestRetryDelay, "err", rejected.Err)
	return false
}

// confirmAccepted announces accepted requests once the person is on the friend
// list. Xbox can report a request as accepted while it stays pending.
func (s *FriendSyncer) confirmAccepted(ctx context.Context, people []Person) {
	if len(s.state.accepted) == 0 {
		return
	}
	for _, p := range people {
		req, ok := s.state.accepted[p.XUID]
		if !ok || !p.IsFollowedByCaller {
			continue
		}
		delete(s.state.accepted, p.XUID)
		s.forget(ctx, p.XUID) // a new friendship starts a fresh clock
		s.info(ctx, "added friend", "xuid", p.XUID, "gamertag", req.person.Gamertag, "source", "pending_requests")
		s.sendInitialInvite(ctx, req.person, "pending_requests")
	}
}

// follow follows p back and reports whether the friendship was established.
// Restricted accounts are dropped as followers so they are not retried forever.
func (s *FriendSyncer) follow(ctx context.Context, p Person, opts friendSyncOptions, result *friendSyncResult) bool {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.Follow(operationCtx, p.XUID)
	cancel()
	if err == nil {
		s.info(ctx, "added friend", "xuid", p.XUID, "gamertag", p.Gamertag)
		s.sendInitialInvite(ctx, p, "auto_follow")
		return true
	}
	s.logFriendSyncError("follow", p, err)
	s.debug(ctx, "failed to add friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	switch {
	case errors.Is(err, xblsocial.ErrFriendRestricted):
		if opts.autoUnfollow && !result.unfollowBlocked() {
			s.dropRestrictedFollower(ctx, p, result)
		}
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
func (s *FriendSyncer) dropRestrictedFollower(ctx context.Context, p Person, result *friendSyncResult) {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.RemoveFollower(operationCtx, p.XUID)
	cancel()
	if err != nil {
		result.unfollowRetryAfter = max(result.unfollowRetryAfter, retryDelay(err))
		if s.Log != nil {
			s.Log.Error("remove restricted follower", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		}
		return
	}
	s.forget(ctx, p.XUID)
	s.warn(ctx, "removed friend due to restrictions on their account", "xuid", p.XUID, "gamertag", p.Gamertag)
	s.notify(ctx, "Removed "+p.Gamertag+" ("+p.XUID+") as a friend due to restrictions on their account.")
}

// unfollow drops the account's follow of p, who no longer follows back.
func (s *FriendSyncer) unfollow(ctx context.Context, p Person, result *friendSyncResult) bool {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.Unfollow(operationCtx, p.XUID)
	cancel()
	if err != nil {
		s.debug(ctx, "failed to remove friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		result.unfollowRetryAfter = max(result.unfollowRetryAfter, retryDelay(err))
		return false
	}
	s.info(ctx, "removed friend", "xuid", p.XUID, "gamertag", p.Gamertag)
	s.forget(ctx, p.XUID)
	return true
}

// removeFriend ends the friendship with p and reports whether it ended. When
// p's follow of the account could not be dropped too, p is marked in History
// so a later pass finishes the removal instead of following p back.
func (s *FriendSyncer) removeFriend(ctx context.Context, p Person, reason string, lastSeen time.Time, result *friendSyncResult) bool {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.RemoveFriend(operationCtx, p.XUID)
	cancel()
	if err != nil {
		s.debug(ctx, "failed to remove friend", "xuid", p.XUID, "gamertag", p.Gamertag, "reason", reason, "err", err)
		result.unfollowRetryAfter = max(result.unfollowRetryAfter, retryDelay(err))
		return false
	}
	s.info(ctx, "removed friend", "xuid", p.XUID, "gamertag", p.Gamertag, "reason", reason, "last_seen", lastSeen)
	if result.unfollowBlocked() || !s.finishRemoval(ctx, p, result) {
		if err := s.History.MarkRemoving(ctx, s.Account, time.Now(), p.XUID); err != nil && s.Log != nil {
			s.Log.Error("record pending friend removal", "xuid", p.XUID, "err", err)
		}
	}
	return true
}

// finishRemoval drops a removed friend's follow of the account and reports
// whether it succeeded.
func (s *FriendSyncer) finishRemoval(ctx context.Context, p Person, result *friendSyncResult) bool {
	operationCtx, cancel := xboxOperationContext(ctx)
	err := s.Client.RemoveFollower(operationCtx, p.XUID)
	cancel()
	if err != nil {
		s.debug(ctx, "failed to remove follower", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		result.unfollowRetryAfter = max(result.unfollowRetryAfter, retryDelay(err))
		return false
	}
	s.forget(ctx, p.XUID)
	return true
}

// removing returns the people this account is still finishing removing.
func (s *FriendSyncer) removing(ctx context.Context) map[string]time.Time {
	if s.History == nil || !s.Config.Cleanup.enabled() {
		return nil
	}
	removing, err := s.History.Removing(ctx, s.Account)
	if err != nil && s.Log != nil {
		s.Log.Error("read pending friend removals", "err", err)
	}
	return removing
}

// friendCleanupCandidate is a friend cleanup has chosen to remove.
type friendCleanupCandidate struct {
	person   Person
	lastSeen time.Time
	reason   string
}

// cleanup removes inactive friends on cleanup passes and, on every pass, the
// least recently seen friends needed to keep the list within MaxFriends. It
// returns how many friends it removed.
func (s *FriendSyncer) cleanup(ctx context.Context, people []Person, own, unfollowed map[string]struct{}, removing map[string]time.Time, opts friendSyncOptions, result *friendSyncResult) int {
	conf := s.Config.Cleanup
	if s.History == nil || !conf.enabled() || (!opts.cleanup && conf.MaxFriends == 0) {
		return 0
	}
	now := time.Now()
	lastSeen, err := s.History.LastSeen(ctx, s.Account)
	if err != nil {
		if s.Log != nil {
			s.Log.Error("read player history", "err", err)
		}
		return 0
	}
	if lastSeen == nil {
		lastSeen = map[string]time.Time{}
	}
	// Xbox caps the people the account follows, so that is what counts.
	count := 0
	var friends []Person
	var untracked []string
	for _, p := range people {
		if _, ok := unfollowed[p.XUID]; ok || !p.IsFollowedByCaller || isGuestXUID(p.XUID) {
			continue
		}
		count++
		if _, ok := own[p.XUID]; ok {
			continue
		}
		if _, ok := lastSeen[p.XUID]; !ok {
			lastSeen[p.XUID] = now
			untracked = append(untracked, p.XUID)
		}
		friends = append(friends, p)
	}
	if len(untracked) > 0 {
		if err := s.History.Track(ctx, s.Account, now, untracked...); err != nil && s.Log != nil {
			s.Log.Error("record player history", "err", err)
		}
	}
	if opts.cleanup {
		s.pruneHistory(ctx, lastSeen, removing, people, friends)
	}

	slices.SortFunc(friends, func(a, b Person) int {
		return cmp.Or(lastSeen[a.XUID].Compare(lastSeen[b.XUID]), cmp.Compare(a.XUID, b.XUID))
	})
	var candidates []friendCleanupCandidate
	if opts.cleanup && conf.InactiveDays > 0 {
		cutoff := now.Add(-time.Duration(conf.InactiveDays) * 24 * time.Hour)
		for _, p := range friends {
			if !lastSeen[p.XUID].Before(cutoff) {
				break
			}
			candidates = append(candidates, friendCleanupCandidate{p, lastSeen[p.XUID], "inactive"})
		}
	}
	inactive := len(candidates)
	if conf.MaxFriends > 0 {
		// Only ever down to MaxFriends: a full-list error alone never forces
		// removals, so a mismatch with Xbox's count cannot drain the list.
		need := count - inactive + result.waiting - conf.MaxFriends
		for _, p := range friends[inactive:min(len(friends), inactive+max(need, 0))] {
			candidates = append(candidates, friendCleanupCandidate{p, lastSeen[p.XUID], "over_capacity"})
		}
	}
	s.debug(ctx, "friend cleanup scan",
		"friends", count,
		"waiting_requests", result.waiting,
		"max_friends", conf.MaxFriends,
		"inactive", inactive,
		"over_capacity", len(candidates)-inactive,
		"newly_tracked", len(untracked),
	)
	if len(candidates) == 0 {
		return 0
	}
	if !opts.autoUnfollow || result.unfollowBlocked() {
		s.debug(ctx, "friend cleanup deferred by rate limit", "candidates", len(candidates))
		return 0
	}
	removed := 0
	for _, c := range candidates {
		if ctx.Err() != nil || result.unfollowBlocked() {
			break
		}
		if s.removeFriend(ctx, c.person, c.reason, c.lastSeen, result) {
			removed++
		}
	}
	if result.friendListFull && removed > 0 {
		result.madeRoom = true
	}
	return removed
}

// pruneHistory drops this account's history for people no longer its friends,
// and removal marks for people who no longer follow it.
func (s *FriendSyncer) pruneHistory(ctx context.Context, lastSeen, removing map[string]time.Time, people, friends []Person) {
	current := make(map[string]struct{}, len(friends))
	for _, p := range friends {
		current[p.XUID] = struct{}{}
	}
	followers := make(map[string]struct{}, len(people))
	for _, p := range people {
		if p.IsFollowingCaller {
			followers[p.XUID] = struct{}{}
		}
	}
	var gone []string
	for xuid := range lastSeen {
		if _, ok := current[xuid]; !ok {
			gone = append(gone, xuid)
			delete(lastSeen, xuid)
		}
	}
	for xuid := range removing {
		if _, ok := followers[xuid]; !ok {
			gone = append(gone, xuid)
		}
	}
	if len(gone) > 0 {
		s.forget(ctx, gone...)
		s.debug(ctx, "pruned player history for ex-friends", "count", len(gone))
	}
}

// forget drops xuids from this account's history.
func (s *FriendSyncer) forget(ctx context.Context, xuids ...string) {
	if s.History == nil || len(xuids) == 0 {
		return
	}
	if err := s.History.Forget(ctx, s.Account, xuids...); err != nil && s.Log != nil {
		s.Log.Error("forget player history", "count", len(xuids), "err", err)
	}
}

// sendInitialInvite invites p to the session at most once per friendInviteMemory.
func (s *FriendSyncer) sendInitialInvite(ctx context.Context, p Person, source string) {
	if !s.Config.InitialInvite || s.Inviter == nil {
		return
	}
	if _, ok := s.state.invited[p.XUID]; ok {
		s.debug(ctx, "skipping repeat initial invite", "xuid", p.XUID, "gamertag", p.Gamertag, "source", source)
		return
	}
	s.state.invited[p.XUID] = time.Now()
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

type friendSyncStats struct {
	people                 int
	followers              int
	following              int
	followersOnly          int
	followingOnly          int
	mutual                 int
	neither                int
	autoFollowCandidates   int
	autoUnfollowCandidates int
}

func (s *FriendSyncer) friendSyncStats(people []Person, opts friendSyncOptions) friendSyncStats {
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
		switch {
		case p.IsFollowingCaller && p.IsFollowedByCaller:
			stats.mutual++
		case p.IsFollowingCaller:
			stats.followersOnly++
		case p.IsFollowedByCaller:
			stats.followingOnly++
		default:
			stats.neither++
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

func (s *FriendSyncer) logFriendSyncError(op string, p Person, err error) {
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

func (s *FriendSyncer) notify(ctx context.Context, message string) {
	if s.Notifier == nil {
		return
	}
	notifyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := s.Notifier.Notify(notifyCtx, message); err != nil && s.Log != nil {
		s.Log.Error("send notification", "err", err)
	}
}

func (s *FriendSyncer) info(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.InfoContext(ctx, msg, args...)
	}
}

func (s *FriendSyncer) warn(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.WarnContext(ctx, msg, args...)
	}
}

func (s *FriendSyncer) debug(ctx context.Context, msg string, args ...any) {
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

// Run combines events, polling, and cleanup into spaced sync passes.
func (s *FriendSyncer) Run(ctx context.Context) {
	interval := max(s.Config.UpdateInterval, friendSyncMinInterval)
	cleanupInterval := max(s.Config.Cleanup.Interval, friendSyncMinInterval)
	cleanupEnabled := s.Config.Cleanup.enabled()
	nextPoll, nextCleanup := time.Now(), time.Now()
	var nextScan time.Time
	pending, cleanup := true, true
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
		if cleanupEnabled && !now.Before(nextCleanup) {
			pending, cleanup = true, true
		}
		if pending && !now.Before(nextScan) {
			retryAfter, again := s.runSync(ctx, cleanup)
			now = time.Now()
			nextScan = now.Add(max(friendSyncMinInterval, retryAfter))
			nextPoll = now.Add(interval)
			if retryAfter == 0 {
				if cleanup {
					nextCleanup = now.Add(cleanupInterval)
				}
				cleanup = false
			}
			pending = retryAfter > 0 || again
		}

		nextWake := nextPoll
		if cleanupEnabled && nextCleanup.Before(nextWake) {
			nextWake = nextCleanup
		}
		if pending || nextWake.Before(nextScan) {
			nextWake = nextScan
		}
		timer.Reset(time.Until(nextWake))
	}
}

// runSync updates backoff and returns any delay needed before reading again,
// and whether another pass should follow as soon as spacing allows.
func (s *FriendSyncer) runSync(ctx context.Context, cleanup bool) (time.Duration, bool) {
	opts := s.state.options(time.Now(), cleanup)
	s.debug(ctx, "friend sync tick", "cleanup", cleanup, "auto_follow", opts.autoFollow, "auto_unfollow", opts.autoUnfollow)
	result, err := s.syncWithOptions(ctx, opts)
	s.state.record(time.Now(), result)
	if err != nil && s.Log != nil && !errors.Is(err, context.Canceled) {
		s.Log.Error("sync friends", "err", err)
	}
	return result.readRetryAfter, result.madeRoom
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
