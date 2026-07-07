package broadcaster

import (
	"log/slog"

	"github.com/df-mc/go-xsapi/v2"
)

// reactiveFriendSyncApplicable reports whether an account with the given friend
// sync configuration benefits from a reactive social subscription. Only
// auto-follow and auto-unfollow act on social changes, so a subscription is
// pointless without at least one of them.
func reactiveFriendSyncApplicable(conf *FriendSyncConfig) bool {
	return conf != nil && (conf.AutoFollow || conf.AutoUnfollow)
}

// startSocialSubscription subscribes to the account's RTA social feed so friend
// requests are accepted (and relationship changes reconciled) reactively rather
// than only on the sync interval. It returns a channel the account's friend
// syncer should select on to run an immediate pass, or nil when a reactive
// subscription is not applicable.
//
// The subscription is best-effort: on a subscribe failure or a lost
// subscription the periodic syncer remains the backstop, and the RTA connection
// re-subscribes automatically after a transient drop.
func (b *Broadcaster) startSocialSubscription(client *xsapi.Client, conf *FriendSyncConfig, log *slog.Logger) <-chan struct{} {
	if client == nil || !reactiveFriendSyncApplicable(conf) {
		return nil
	}
	// Buffered by one so bursts of events collapse into a single pending pass.
	trigger := make(chan struct{}, 1)
	handler := friendRequestSubscriptionHandler{trigger: trigger, log: log}
	go func() {
		// Subscribe dials RTA lazily; run it off the start path so a slow or
		// failing dial cannot delay broadcasting.
		if err := client.Social().Subscribe(b.ctx, handler); err != nil {
			log.Warn("subscribe to social rta feed; friend requests will be accepted on the sync interval", "err", err)
			return
		}
		log.Debug("subscribed to social rta feed for reactive friend sync")
	}()
	return trigger
}

// friendRequestSubscriptionHandler adapts go-xsapi's social RTA subscription to
// the friend syncer: any relationship change or incoming-friend-request event
// requests an immediate sync pass through trigger.
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
