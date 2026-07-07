package broadcaster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// TestFriendSyncerRunSyncsOnTrigger verifies that a signal on the Trigger channel
// causes an immediate sync pass, rather than waiting for the poll interval.
func TestFriendSyncerRunSyncsOnTrigger(t *testing.T) {
	accepted := make(chan struct{}, 16)
	client := &syncFriendClient{
		accept: func(context.Context) ([]Person, error) {
			accepted <- struct{}{}
			return nil, nil
		},
	}
	trigger := make(chan struct{}, 1)
	syncer := FriendSyncer{
		Client:  client,
		Config:  FriendSyncConfig{AutoFollow: true, UpdateInterval: time.Hour},
		Trigger: trigger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go syncer.Run(ctx)

	// Run performs an initial sync on startup.
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("no initial sync pass")
	}
	// An RTA event fires the trigger, causing an immediate extra sync well before
	// the one-hour ticker could.
	trigger <- struct{}{}
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("trigger did not cause a sync pass")
	}
}

// TestFriendRequestSubscriptionHandlerCoalescesEvents verifies that rapid social
// events collapse into a single pending sync (non-blocking), and that a lost
// subscription does not enqueue work.
func TestFriendRequestSubscriptionHandlerCoalescesEvents(t *testing.T) {
	trigger := make(chan struct{}, 1)
	h := friendRequestSubscriptionHandler{trigger: trigger, log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	h.HandleIncomingFriendRequestCountChange(2)
	h.HandleIncomingFriendRequestCountChange(3)
	h.HandleSocialNotification(xblsocial.NotificationTypeAdded, []string{"123"})
	if got := len(trigger); got != 1 {
		t.Fatalf("pending triggers = %d, want 1 (coalesced, non-blocking)", got)
	}
	<-trigger

	// A lost subscription must not enqueue a sync; the periodic syncer is the backstop.
	h.HandleSubscriptionLost()
	select {
	case <-trigger:
		t.Fatal("HandleSubscriptionLost enqueued a sync")
	default:
	}
}

// TestReactiveFriendSyncApplicable verifies which configurations warrant a
// reactive social subscription.
func TestReactiveFriendSyncApplicable(t *testing.T) {
	cases := []struct {
		name string
		conf *FriendSyncConfig
		want bool
	}{
		{"nil", nil, false},
		{"neither", &FriendSyncConfig{}, false},
		{"auto-follow", &FriendSyncConfig{AutoFollow: true}, true},
		{"auto-unfollow", &FriendSyncConfig{AutoUnfollow: true}, true},
	}
	for _, tc := range cases {
		if got := reactiveFriendSyncApplicable(tc.conf); got != tc.want {
			t.Errorf("%s: reactiveFriendSyncApplicable = %v, want %v", tc.name, got, tc.want)
		}
	}
}
