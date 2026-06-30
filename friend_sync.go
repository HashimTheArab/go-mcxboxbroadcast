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

type FriendSyncer struct {
	Client  FriendAPI
	Config  FriendSyncConfig
	Inviter Inviter
	History HistoryStore
	Log     *slog.Logger
}

const friendListFullBackoff = time.Hour

type friendSyncOptions struct {
	expire       bool
	autoFollow   bool
	autoUnfollow bool
}

type friendSyncRunState struct {
	retryUntil      time.Time
	autoFollowUntil time.Time
}

func (s *friendSyncRunState) options(now time.Time, expire bool) friendSyncOptions {
	mutationsAllowed := !s.backingOff(now)
	return friendSyncOptions{
		expire:       expire,
		autoFollow:   mutationsAllowed && !now.Before(s.autoFollowUntil),
		autoUnfollow: mutationsAllowed,
	}
}

func (s *friendSyncRunState) backingOff(now time.Time) bool {
	return now.Before(s.retryUntil)
}

func (s *friendSyncRunState) recordError(now time.Time, err error) {
	if delay := retryDelay(err); delay > 0 {
		s.retryUntil = now.Add(delay)
	}
	if errors.Is(err, xblsocial.ErrFriendListFull) {
		s.autoFollowUntil = now.Add(friendListFullBackoff)
	}
}

func (s FriendSyncer) Sync(ctx context.Context) error {
	return s.sync(ctx, true)
}

func (s FriendSyncer) sync(ctx context.Context, expire bool) error {
	return s.syncWithOptions(ctx, friendSyncOptions{
		expire:       expire,
		autoFollow:   true,
		autoUnfollow: true,
	})
}

func (s FriendSyncer) syncWithOptions(ctx context.Context, opts friendSyncOptions) error {
	if s.Client == nil {
		return nil
	}
	if s.Config.InitialInvite && s.Inviter == nil {
		s.debug(ctx, "initial invite unavailable", "reason", "session inviter is not configured")
	}
	if s.Config.AutoFollow && opts.autoFollow {
		if accepter, ok := s.Client.(pendingFriendRequestAccepter); ok {
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
				if shouldStopPendingFriendAccept(err) {
					return err
				}
				s.logPendingFriendAcceptError(err)
			}
		}
	}
	people, err := s.Client.Friends(ctx)
	if err != nil {
		return err
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
		if s.Config.IgnoreGuestXUID && isGuestXUID(p.XUID) {
			continue
		}
		if s.Config.AutoFollow && opts.autoFollow && p.IsFollowingCaller && !p.IsFollowedByCaller {
			if err := s.Client.Follow(ctx, p.XUID); err != nil {
				s.logFriendSyncError("follow", p, err)
				s.debug(ctx, "failed to add friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
				return err
			}
			added++
			s.info(ctx, "added friend", "xuid", p.XUID, "gamertag", p.Gamertag)
			if s.Config.InitialInvite && s.Inviter != nil {
				s.sendInitialInvite(ctx, p, "auto_follow")
			}
		}
		if s.Config.AutoUnfollow && opts.autoUnfollow && !p.IsFollowingCaller && p.IsFollowedByCaller {
			if err := s.Client.Unfollow(ctx, p.XUID); err != nil {
				s.debug(ctx, "failed to remove friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
				return err
			}
			removed++
			s.info(ctx, "removed friend", "xuid", p.XUID, "gamertag", p.Gamertag)
			if s.History != nil {
				_ = s.History.Clear(ctx, p.XUID)
			}
			continue
		}
		if opts.expire && opts.autoUnfollow && s.Config.ExpiryEnabled && p.IsFollowedByCaller && s.History != nil {
			lastSeen, ok, err := s.History.LastSeen(ctx, p.XUID)
			if err != nil {
				return err
			}
			if !ok {
				if recorder, ok := s.History.(HistoryRecorder); ok {
					if err := recorder.Seen(ctx, p.XUID, time.Now()); err != nil {
						return err
					}
				}
				continue
			}
			expiryDays := s.Config.ExpiryDays
			if expiryDays <= 0 {
				expiryDays = 15
			}
			if ok && lastSeen.Before(time.Now().Add(-time.Duration(expiryDays)*24*time.Hour)) {
				s.info(ctx, "removing inactive friend", "xuid", p.XUID, "gamertag", p.Gamertag, "last_seen", lastSeen)
				if err := s.Client.Unfollow(ctx, p.XUID); err != nil {
					s.debug(ctx, "failed to remove inactive friend", "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
					return err
				}
				s.info(ctx, "removed friend", "xuid", p.XUID, "gamertag", p.Gamertag, "reason", "inactive")
				_ = s.History.Clear(ctx, p.XUID)
			}
		}
	}
	if stats.autoFollowCandidates > 0 {
		s.debug(ctx, "added friends", "count", added)
	}
	if stats.autoUnfollowCandidates > 0 {
		s.debug(ctx, "removed friends", "count", removed)
	}
	return nil
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
		if s.Config.IgnoreGuestXUID && isGuestXUID(p.XUID) {
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

func (s FriendSyncer) info(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.InfoContext(ctx, msg, args...)
	}
}

func (s FriendSyncer) debug(ctx context.Context, msg string, args ...any) {
	if s.Log != nil {
		s.Log.DebugContext(ctx, msg, args...)
	}
}

func shouldStopPendingFriendAccept(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || retryDelay(err) > 0
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

func (s FriendSyncer) runSync(ctx context.Context, state *friendSyncRunState, expire bool) {
	if state == nil {
		state = &friendSyncRunState{}
	}
	now := time.Now()
	if state.backingOff(now) {
		s.debug(ctx, "friend sync backing off", "retry_until", state.retryUntil)
		return
	}
	opts := state.options(now, expire)
	s.debug(ctx, "friend sync tick", "expire", expire, "auto_follow", opts.autoFollow, "auto_unfollow", opts.autoUnfollow)
	err := s.syncWithOptions(ctx, opts)
	if err == nil {
		return
	}
	state.recordError(time.Now(), err)
	if s.Log != nil {
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
