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

// subscribeSocial subscribes sub to the social RTA feed and returns a trigger
// channel that fires on each event. The subscription's lifetime is bound to the
// broadcaster: a dedicated goroutine unsubscribes once the broadcaster's context
// is canceled, and [Broadcaster.Close] waits for it via socialWg. This keeps a
// caller-provided client (which the broadcaster does not close) from
// accumulating stale handlers across broadcaster restarts.
//
// The subscription is best-effort: on a subscribe failure or a lost subscription
// the periodic syncer remains the backstop, and the RTA connection re-subscribes
// automatically after a transient drop.
func (b *Broadcaster) subscribeSocial(sub socialSubscriber, log *slog.Logger) <-chan struct{} {
	// Buffered by one so bursts of events collapse into a single pending pass.
	trigger := make(chan struct{}, 1)
	handler := friendRequestSubscriptionHandler{trigger: trigger, log: log}
	b.socialWg.Add(1)
	go func() {
		defer b.socialWg.Done()
		// Subscribe dials RTA lazily; running it here keeps a slow or failing
		// dial off the start path. Bound setup because it holds the shared
		// RTA subscription lock while waiting for an acknowledgment.
		ctx, cancel := xboxOperationContext(b.ctx)
		unsubscribe, err := sub.Subscribe(ctx, handler)
		cancel()
		if err != nil {
			log.Warn("subscribe to social rta feed; friend requests will be accepted on the sync interval", "err", err)
			return
		}
		log.Debug("subscribed to social rta feed for reactive friend sync")

		<-b.ctx.Done()
		// b.ctx is done, so use a fresh context to release the subscription.
		// The cleanup removes only this registration, so a shared client's other
		// subscribers keep working.
		ctx, cancel = context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := unsubscribe(ctx); err != nil {
			log.Debug("unsubscribe social rta feed", "err", err)
		}
	}()
	return trigger
}

// friendRequestSubscriptionHandler adapts go-xsapi's social RTA subscription to
// the friend syncer. Events request a pass through its normal rate limits.
type friendRequestSubscriptionHandler struct {
	trigger chan<- struct{}
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

// HandleSubscriptionLost logs the loss. The periodic syncer continues to accept
// requests, and the RTA connection re-subscribes automatically after transient
// drops, so no action is taken here.
func (h friendRequestSubscriptionHandler) HandleSubscriptionLost() {
	h.log.Warn("social subscription lost; friend requests will be accepted on the sync interval until it is restored")
}

// signal requests a sync pass without blocking. A full buffer means a pass is
// already pending, so the event is coalesced into it.
func (h friendRequestSubscriptionHandler) signal() {
	select {
	case h.trigger <- struct{}{}:
	default:
	}
}
