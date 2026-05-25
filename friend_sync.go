package broadcaster

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/df-mc/go-xsapi/mpsd"
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
	Invite(xuid string, titleID int32) error
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
	skip         bool
}

type friendSyncRunState struct {
	retryUntil      time.Time
	autoFollowUntil time.Time
}

func (s *friendSyncRunState) options(now time.Time) friendSyncOptions {
	mutationsAllowed := !now.Before(s.retryUntil)
	return friendSyncOptions{
		expire:       true,
		autoFollow:   mutationsAllowed && !now.Before(s.autoFollowUntil),
		autoUnfollow: mutationsAllowed,
		skip:         !mutationsAllowed,
	}
}

func (s *friendSyncRunState) recordError(now time.Time, err error) {
	if delay := retryDelay(err); delay > 0 {
		s.retryUntil = now.Add(delay)
	}
	if IsFriendListFull(err) {
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
	if s.Config.AutoFollow && opts.autoFollow {
		if accepter, ok := s.Client.(pendingFriendRequestAccepter); ok {
			accepted, err := accepter.AcceptPendingFriendRequests(ctx)
			if s.Config.InitialInvite && s.Inviter != nil {
				for _, p := range accepted {
					_ = s.Inviter.Invite(p.XUID, int32(TitleID))
				}
			}
			if err != nil {
				return err
			}
		}
	}
	people, err := s.Client.Friends(ctx)
	if err != nil {
		return err
	}
	for _, p := range people {
		if s.Config.IgnoreGuestXUID && isGuestXUID(p.XUID) {
			continue
		}
		if s.Config.AutoFollow && opts.autoFollow && p.IsFollowingCaller && !p.IsFollowedByCaller {
			if err := s.Client.Follow(ctx, p.XUID); err != nil {
				s.logFriendSyncError("follow", p, err)
				return err
			}
			if s.Config.InitialInvite && s.Inviter != nil {
				_ = s.Inviter.Invite(p.XUID, int32(TitleID))
			}
		}
		if s.Config.AutoUnfollow && opts.autoUnfollow && !p.IsFollowingCaller && p.IsFollowedByCaller {
			if err := s.Client.Unfollow(ctx, p.XUID); err != nil {
				return err
			}
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
				if err := s.Client.Unfollow(ctx, p.XUID); err != nil {
					return err
				}
				_ = s.History.Clear(ctx, p.XUID)
			}
		}
	}
	return nil
}

func (s FriendSyncer) logFriendSyncError(op string, p Person, err error) {
	if s.Log == nil {
		return
	}
	switch friendErrorKind(err) {
	case FriendErrorKindFullList:
		s.Log.Warn("friend list full while syncing friends", "op", op, "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	case FriendErrorKindRestricted:
		s.Log.Warn("friend restricted while syncing friends", "op", op, "xuid", p.XUID, "gamertag", p.Gamertag, "err", err)
	}
}

func friendErrorKind(err error) string {
	var classified interface {
		FriendErrorKind() string
	}
	if errors.As(err, &classified) {
		return classified.FriendErrorKind()
	}
	return ""
}

func retryDelay(err error) time.Duration {
	var retry interface {
		RetryDelay() time.Duration
	}
	if errors.As(err, &retry) {
		return retry.RetryDelay()
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
	opts := state.options(time.Now())
	opts.expire = expire
	if opts.skip {
		return
	}
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

func (i sessionInviter) Invite(xuid string, titleID int32) error {
	_, err := i.session.Invite(xuid, titleID)
	return err
}
