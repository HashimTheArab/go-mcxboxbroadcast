package broadcaster

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// FriendAPI is the account's friend list as the syncer sees it.
type FriendAPI interface {
	Friends(ctx context.Context) ([]Person, error)
	// AcceptPendingFriendRequests accepts the requests skip does not report and returns who was
	// accepted; failed people are reported through *AcceptFriendRequestsError.
	AcceptPendingFriendRequests(ctx context.Context, skip func(xuid string) bool) ([]Person, error)
	AddFriend(ctx context.Context, xuid string) error
	RemoveFriend(ctx context.Context, xuid string) error
}

// Inviter sends a newly added friend a Minecraft game invite.
type Inviter interface {
	Invite(ctx context.Context, xuid string) error
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
	// Trigger requests an early sync, subject to scan spacing and Retry-After.
	// A nil Trigger leaves the syncer purely periodic.
	Trigger <-chan struct{}
	Log     *slog.Logger

	friendCount *friendCount // nil outside a Broadcaster
}

const (
	fullListHold   = time.Hour // how long a player refused for a full list waits; also how long their refusal counts
	refusedBackoff = time.Hour // how long a player Xbox refuses with no reason or delay waits
	// fullListPlayers is how many different players refused for the first time in the last hour,
	// with no accept since, make the bot report its own list as full. One refusal is that player's own list.
	fullListPlayers = 2
)

type friendSyncOptions struct {
	expire bool
	add    bool
	remove bool
	// skip holds players whose requests wait, for a full list, a refusal or an Xbox delay.
	skip          map[string]struct{}
	refusedBefore map[string]struct{} // players refused for a full list on an earlier pass; read only
}

// friendSyncRunState tracks per-operation rate-limit backoff between runs.
// Add and remove limits are tracked separately because Xbox applies them
// independently; a rate-limited add pass must not stall expiry.
type friendSyncRunState struct {
	addRetryUntil    time.Time
	removeRetryUntil time.Time
	// fullPlayers maps players refused for a full list to when; a player's request waits an hour.
	fullPlayers map[string]time.Time
	waiting     map[string]time.Time // players Xbox asked to retry later, to when
	// refusedBefore holds every player refused for a full list since start. Their repeat refusals,
	// and refusals at the first pass, say nothing new about the bot's own list.
	refusedBefore map[string]struct{}
	// fresh maps first refusals since the last accept, after the first pass, to when. They expire
	// so a false full clears even if the app stops sending the bot new players.
	fresh   map[string]time.Time
	started bool // a pass has read the pending list
	full    bool // the bot's own list looks full: Xbox has started refusing new players
}

func (s *friendSyncRunState) options(now time.Time, expire bool) friendSyncOptions {
	return friendSyncOptions{
		expire:        expire,
		add:           !now.Before(s.addRetryUntil),
		remove:        !now.Before(s.removeRetryUntil),
		skip:          s.skipped(now),
		refusedBefore: s.refusedBefore,
	}
}

func (s *friendSyncRunState) skipped(now time.Time) map[string]struct{} {
	skip := make(map[string]struct{})
	for xuid, until := range s.waiting {
		if now.Before(until) {
			skip[xuid] = struct{}{}
		}
	}
	for xuid, at := range s.fullPlayers {
		if now.Before(at.Add(fullListHold)) {
			skip[xuid] = struct{}{}
		}
	}
	return skip
}

// record applies a pass's backoffs and updates whether the bot's own list looks full. That
// only reports the list; per-player holds already bound the cost of asking while it is full.
func (s *friendSyncRunState) record(now time.Time, result friendSyncResult) {
	if result.addRetryAfter > 0 {
		s.addRetryUntil = now.Add(result.addRetryAfter)
	}
	if result.removeRetryAfter > 0 {
		s.removeRetryUntil = now.Add(result.removeRetryAfter)
	}
	if s.fullPlayers == nil {
		s.fullPlayers = make(map[string]time.Time)
		s.refusedBefore = make(map[string]struct{})
		s.fresh = make(map[string]time.Time)
	}
	for xuid, at := range s.fullPlayers {
		if now.Sub(at) >= fullListHold {
			delete(s.fullPlayers, xuid)
		}
	}
	for xuid, at := range s.fresh {
		if now.Sub(at) >= fullListHold {
			delete(s.fresh, xuid)
		}
	}
	if result.accepted > 0 {
		clear(s.fresh)
	}
	for _, xuid := range result.fullPlayers {
		s.fullPlayers[xuid] = now
		if _, seen := s.refusedBefore[xuid]; seen {
			continue
		}
		s.refusedBefore[xuid] = struct{}{}
		if s.started && result.accepted == 0 {
			s.fresh[xuid] = now
		}
	}
	s.started = s.started || result.pendingRead
	s.full = len(s.fresh) >= fullListPlayers
	for xuid, until := range s.waiting {
		if !now.Before(until) {
			delete(s.waiting, xuid)
		}
	}
	for xuid, delay := range result.waits {
		if s.waiting == nil {
			s.waiting = make(map[string]time.Time)
		}
		s.waiting[xuid] = now.Add(delay)
	}
}

// friendSyncResult carries the rate-limit outcomes of a sync pass.
type friendSyncResult struct {
	readRetryAfter   time.Duration
	addRetryAfter    time.Duration
	removeRetryAfter time.Duration
	accepted         int                      // friend requests accepted
	fullPlayers      []string                 // players refused because a friend list was full
	waits            map[string]time.Duration // players Xbox asked to retry later, and how long
	pendingRead      bool                     // the pending list was read this pass
}

func (r *friendSyncResult) wait(xuid string, delay time.Duration) {
	if r.waits == nil {
		r.waits = make(map[string]time.Duration)
	}
	r.waits[xuid] = delay
}

// addBlocked stops a pass on rate limits, and once enough players refused for the first time,
// with no accept, suggest the bot is full. Repeat refusals are those players' own lists.
func (r friendSyncResult) addBlocked(refusedBefore map[string]struct{}) bool {
	if r.addRetryAfter > 0 {
		return true
	}
	if r.accepted > 0 {
		return false
	}
	var first int
	for _, xuid := range r.fullPlayers {
		if _, seen := refusedBefore[xuid]; !seen {
			first++
		}
	}
	return first >= fullListPlayers
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
		s.acceptPending(ctx, opts, &result)
		if result.readRetryAfter > 0 {
			return result, nil
		}
	}
	expiryOn := s.Config.ExpiryEnabled && s.History != nil
	expire := opts.expire && expiryOn
	// Lists friends for the count even without expiry, inside this serialized loop.
	if !expire && !s.friendCount.needsList(expiryOn) {
		return result, nil
	}
	operationCtx, cancel := xboxOperationContext(ctx)
	friends, err := s.Client.Friends(operationCtx)
	cancel()
	if err != nil {
		result.readRetryAfter = retryDelay(err)
		if !expire {
			s.friendCount.listFailed()
			s.debug(ctx, "count friends", "err", err)
			return result, nil
		}
		return result, err
	}
	s.friendCount.set(len(friends))
	if !expire {
		return result, nil
	}
	s.debug(ctx, "friend expiry scan", "friends", len(friends))
	expired := 0
	for _, p := range friends {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if isGuestXUID(p.XUID) {
			continue
		}
		if !opts.remove || result.removeRetryAfter > 0 {
			break
		}
		if s.expire(ctx, p, &result) {
			expired++
		}
	}
	s.pruneHistory(ctx, friends)
	s.debug(ctx, "friend expiry scan done", "expired", expired)
	return result, nil
}

// acceptPending accepts incoming friend requests. Requests the bulk accept
// could not update are retried one by one to find restricted accounts.
func (s FriendSyncer) acceptPending(ctx context.Context, opts friendSyncOptions, result *friendSyncResult) {
	s.debug(ctx, "accepting pending friend requests")
	skip := func(xuid string) bool {
		_, ok := opts.skip[xuid]
		return ok
	}
	operationCtx, cancel := xboxOperationContext(ctx)
	accepted, err := s.Client.AcceptPendingFriendRequests(operationCtx, skip)
	cancel()
	for _, p := range accepted {
		s.added(ctx, p, "pending_requests")
	}
	result.accepted += len(accepted)
	var failed *AcceptFriendRequestsError
	var responseErr *xblsocial.ResponseError
	result.pendingRead = err == nil || errors.As(err, &failed) || (errors.As(err, &responseErr) && responseErr.Method != http.MethodGet)
	if errors.As(err, &failed) {
		var held int
		var longest time.Duration
		for _, p := range failed.Failed {
			if delay := failed.Waits[p.XUID]; delay > 0 {
				s.debug(ctx, "friend request waits", "xuid", p.XUID, "gamertag", p.Gamertag, "retry_after", delay)
				result.wait(p.XUID, delay)
				held, longest = held+1, max(longest, delay)
			}
		}
		if held > 0 {
			s.warn(ctx, "xbox refused friend requests; they wait", "players", held, "retry_after", longest)
		}
	}
	switch {
	case err == nil:
	case failed != nil && failed.Err == nil:
		for _, p := range failed.Failed {
			if _, waits := failed.Waits[p.XUID]; waits {
				continue
			}
			if result.addBlocked(opts.refusedBefore) {
				break
			}
			s.addFriend(ctx, p, result)
		}
	default:
		if delay := retryDelay(err); delay > 0 {
			if responseErr != nil && responseErr.Method == http.MethodGet {
				// The pending list shares PeopleHub's read quota with Friends.
				result.readRetryAfter = delay
			} else if pausesAllAdds(err) {
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
		result.accepted++
	case errors.Is(err, xblsocial.ErrFriendRestricted):
		s.declineRestricted(ctx, p)
	case errors.Is(err, xblsocial.ErrFriendListFull):
		// Usually the player's own list; the run state decides when it is the bot's.
		s.warn(ctx, "friend list full; their request waits an hour", "xuid", p.XUID, "gamertag", p.Gamertag)
		result.fullPlayers = append(result.fullPlayers, p.XUID)
	default:
		switch delay := retryDelay(err); {
		case delay > 0 && pausesAllAdds(err):
			result.addRetryAfter = delay
		case delay > 0 && aboutPlayers(err):
			result.wait(p.XUID, delay)
		case aboutPlayers(err):
			// Asking again every pass is what earns long delays from Xbox.
			result.wait(p.XUID, refusedBackoff)
		}
		if s.Log != nil {
			s.Log.Warn("add friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
		}
	}
}

// added starts p's expiry clock and sends the initial invite.
func (s FriendSyncer) added(ctx context.Context, p Person, source string) {
	s.friendCount.add(1)
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
		expiryDays = defaultFriendExpiryDays
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
	s.friendCount.add(-1)
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
	err := s.Inviter.Invite(operationCtx, p.XUID)
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

// pausesAllAdds reports a delay for the whole account: a rate limit or a server fault. Other
// answers are about the request or its players.
func pausesAllAdds(err error) bool {
	var responseErr *xblsocial.ResponseError
	return errors.As(err, &responseErr) && (responseErr.StatusCode == http.StatusTooManyRequests || responseErr.StatusCode >= 500)
}

// aboutPlayers reports an answer about the request's players rather than the account or its
// token, so a delay or refusal holds just them.
func aboutPlayers(err error) bool {
	var responseErr *xblsocial.ResponseError
	return errors.As(err, &responseErr) && slices.Contains([]int{http.StatusBadRequest, http.StatusNotFound, http.StatusConflict}, responseErr.StatusCode)
}

func retryDelay(err error) time.Duration {
	var responseErr *xblsocial.ResponseError
	if errors.As(err, &responseErr) {
		if responseErr.RetryAfter > 0 {
			return responseErr.RetryAfter
		}
		if errors.Is(err, xblsocial.ErrRateLimited) {
			return minUpdateInterval
		}
	}
	return 0
}

// Run combines events, polling, and expiry into spaced sync passes.
func (s FriendSyncer) Run(ctx context.Context) {
	interval := max(s.Config.UpdateInterval, minUpdateInterval)
	expiryInterval := max(s.Config.ExpiryCheck, minUpdateInterval)
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
			nextScan = now.Add(max(minUpdateInterval, retryAfter))
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
	wasFull := state.full
	state.record(time.Now(), result)
	if state.full != wasFull && s.Log != nil {
		if state.full {
			s.Log.Error("xbox refuses new friend requests; the friend list may be full", "refused_players", len(state.fresh))
		} else {
			s.Log.Info("xbox accepts friend requests again")
		}
	}
	s.friendCount.setFull(state.full)
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
