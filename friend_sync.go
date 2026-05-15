package broadcaster

import (
	"context"
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

func (s FriendSyncer) Sync(ctx context.Context) error {
	return s.sync(ctx, true)
}

func (s FriendSyncer) sync(ctx context.Context, expire bool) error {
	if s.Client == nil {
		return nil
	}
	people, err := s.Client.Friends(ctx)
	if err != nil {
		return err
	}
	for _, p := range people {
		if s.Config.IgnoreGuestXUID && isGuestXUID(p.XUID) {
			continue
		}
		if s.Config.AutoFollow && p.IsFollowingCaller && !p.IsFollowedByCaller {
			if err := s.Client.Follow(ctx, p.XUID); err != nil {
				return err
			}
			if s.Config.InitialInvite && s.Inviter != nil {
				_ = s.Inviter.Invite(p.XUID, int32(TitleID))
			}
		}
		if s.Config.AutoUnfollow && !p.IsFollowingCaller && p.IsFollowedByCaller {
			if err := s.Client.Unfollow(ctx, p.XUID); err != nil {
				return err
			}
			if s.History != nil {
				_ = s.History.Clear(ctx, p.XUID)
			}
			continue
		}
		if expire && s.Config.ExpiryEnabled && p.IsFollowedByCaller && s.History != nil {
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

func (s FriendSyncer) Run(ctx context.Context) {
	interval := s.Config.UpdateInterval
	if interval < 20*time.Second {
		interval = 20 * time.Second
	}
	if err := s.Sync(ctx); err != nil && s.Log != nil {
		s.Log.Error("sync friends", "err", err)
	}
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
			if err := s.sync(ctx, false); err != nil && s.Log != nil {
				s.Log.Error("sync friends", "err", err)
			}
		case <-expiryC:
			if err := s.sync(ctx, true); err != nil && s.Log != nil {
				s.Log.Error("sync friends", "err", err)
			}
		case <-ctx.Done():
			return
		}
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
