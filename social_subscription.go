package broadcaster

import (
	"context"
	"log/slog"
	"time"

	"github.com/df-mc/go-xsapi/v2"
	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// socialSubscriber is the part of go-xsapi's social client the broadcaster uses
// to receive and release RTA relationship events. It is satisfied by
// [*xblsocial.Client]. Subscribe returns a cleanup function for only that
// registration, leaving other subscribers on a shared client untouched.
type socialSubscriber interface {
	Subscribe(context.Context, xblsocial.SubscriptionHandler) (func(context.Context) error, error)
}

// reactiveFriendSyncApplicable reports whether an account with the given friend
// sync configuration benefits from a reactive social subscription. Only
// auto-follow and auto-unfollow act on social changes, so a subscription is
// pointless without at least one of them.
func reactiveFriendSyncApplicable(conf *FriendSyncConfig) bool {
	return conf != nil && (conf.AutoFollow || conf.AutoUnfollow)
}

// startSocialSubscription subscribes to the account's RTA social feed so friend
// requests and relationship changes can be handled before the next poll.
// It returns the syncer's trigger channel, or nil when no subscription is needed.
func (b *Broadcaster) startSocialSubscription(client *xsapi.Client, conf *FriendSyncConfig, log *slog.Logger) <-chan struct{} {
	if !hasSocialClient(client) || !reactiveFriendSyncApplicable(conf) {
		return nil
	}
	return b.subscribeSocial(client.Social(), log)
}

// Backoff between social RTA subscribe attempts after a failure or a loss.
const (
	socialResubscribeMinDelay = 5 * time.Second
	socialResubscribeMaxDelay = 5 * time.Minute
)

// subscribeSocial subscribes sub to the social RTA feed and returns a trigger
// channel that fires on each event. The subscription's lifetime is bound to the
// broadcaster: a dedicated goroutine unsubscribes once the broadcaster's context
// is canceled, and [Broadcaster.Close] waits for it via socialWg. This keeps a
// caller-provided client (which the broadcaster does not close) from
// accumulating stale handlers across broadcaster restarts.
//
// A failed or lost subscription is retried with backoff; meanwhile the
// periodic syncer is the backstop.
func (b *Broadcaster) subscribeSocial(sub socialSubscriber, log *slog.Logger) <-chan struct{} {
	// Buffered by one so bursts of events collapse into a single pending pass.
	trigger := make(chan struct{}, 1)
	lost := make(chan struct{}, 1)
	handler := friendRequestSubscriptionHandler{trigger: trigger, lost: lost, log: log}
	b.socialWg.Add(1)
	go func() {
		defer b.socialWg.Done()
		delay := socialResubscribeMinDelay
		resubscribing := false
		for {
			select {
			case <-lost: // a stale loss from the previous registration
			default:
			}
			// Subscribe dials RTA lazily; running it here keeps a slow or failing
			// dial off the start path. Bound setup because it holds the shared
			// RTA subscription lock while waiting for an acknowledgment.
			ctx, cancel := xboxOperationContext(b.ctx)
			unsubscribe, err := sub.Subscribe(ctx, handler)
			cancel()
			if err != nil {
				if b.ctx.Err() != nil {
					return
				}
				log.Warn("subscribe to social rta feed; friend requests will be accepted on the sync interval", "err", err, "retry_in", delay)
				if !sleepContext(b.ctx, delay) {
					return
				}
				delay = min(delay*2, socialResubscribeMaxDelay)
				continue
			}
			log.Debug("subscribed to social rta feed for reactive friend sync")
			if resubscribing {
				handler.signal() // catch up on anything missed while unsubscribed
			}
			resubscribing = true

			select {
			case <-b.ctx.Done():
			case <-lost:
			}
			// Release this registration even when lost, so resubscribing does not
			// leave a stale handler on a shared client. b.ctx may be done, so use a
			// fresh context.
			ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
			if err := unsubscribe(ctx); err != nil {
				log.Debug("unsubscribe social rta feed", "err", err)
			}
			cancel()
			if b.ctx.Err() != nil {
				return
			}
			delay = socialResubscribeMinDelay
			if !sleepContext(b.ctx, delay) {
				return
			}
		}
	}()
	return trigger
}

// sleepContext waits for d and reports false if ctx ended first.
func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// friendRequestSubscriptionHandler adapts go-xsapi's social RTA subscription to
// the friend syncer. Events request a pass through its normal rate limits.
type friendRequestSubscriptionHandler struct {
	trigger chan<- struct{}
	lost    chan<- struct{}
	log     *slog.Logger
}

// HandleIncomingFriendRequestCountChange requests a sync pass so newly received
// friend requests are accepted promptly, mirroring MCXboxBroadcast's reaction to
// the IncomingFriendRequestCountChanged notification.
func (h friendRequestSubscriptionHandler) HandleIncomingFriendRequestCountChange(count int) {
	h.log.Debug("incoming friend request count changed", "count", count)
	h.signal()
}

// HandleSocialNotification requests a sync pass when the caller's relationships
// change (a user added, removed, or updated the caller).
func (h friendRequestSubscriptionHandler) HandleSocialNotification(typ string, xuids []string) {
	h.log.Debug("social notification", "type", typ, "xuids", len(xuids))
	h.signal()
}

// HandleSubscriptionLost asks subscribeSocial to subscribe again. The periodic
// syncer keeps accepting requests meanwhile.
func (h friendRequestSubscriptionHandler) HandleSubscriptionLost() {
	h.log.Warn("social subscription lost; resubscribing, friend requests will be accepted on the sync interval until then")
	select {
	case h.lost <- struct{}{}:
	default:
	}
}

// signal requests a sync pass without blocking. A full buffer means a pass is
// already pending, so the event is coalesced into it.
func (h friendRequestSubscriptionHandler) signal() {
	select {
	case h.trigger <- struct{}{}:
	default:
	}
}
