package broadcaster

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
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

// TestFriendSyncerRunHandlesClosedTrigger verifies that a closed Trigger channel
// does not make Run busy-loop over runSync (a closed channel receives forever).
func TestFriendSyncerRunHandlesClosedTrigger(t *testing.T) {
	accepted := make(chan struct{}, 16)
	client := &syncFriendClient{
		accept: func(context.Context) ([]Person, error) {
			accepted <- struct{}{}
			return nil, nil
		},
	}
	trigger := make(chan struct{})
	syncer := FriendSyncer{
		Client:  client,
		Config:  FriendSyncConfig{AutoFollow: true, UpdateInterval: time.Hour},
		Trigger: trigger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go syncer.Run(ctx)

	// Initial sync on startup.
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("no initial sync pass")
	}
	// Closing the trigger must not spin Run into repeated sync passes.
	close(trigger)
	select {
	case <-accepted:
		t.Fatal("closed trigger caused an extra sync pass (busy loop)")
	case <-time.After(200 * time.Millisecond):
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

// fakeSocialSubscriber records subscription ownership and can fail setup.
type fakeSocialSubscriber struct {
	subscribeErr error
	subscribed   chan xblsocial.SubscriptionHandler
	unsubscribed chan xblsocial.SubscriptionHandler
}

// Subscribe records the requested handler and returns the configured setup error.
func (f *fakeSocialSubscriber) Subscribe(_ context.Context, h xblsocial.SubscriptionHandler) error {
	f.subscribed <- h
	return f.subscribeErr
}

// Unsubscribe records which handler was released.
func (f *fakeSocialSubscriber) Unsubscribe(_ context.Context, h xblsocial.SubscriptionHandler) error {
	f.unsubscribed <- h
	return nil
}

// TestSubscribeSocialUnsubscribesOnShutdown verifies that a social subscription
// is undone when the broadcaster shuts down and that the shutdown waits for it,
// so a reused xsapi client does not accumulate stale handlers across restarts.
func TestSubscribeSocialUnsubscribesOnShutdown(t *testing.T) {
	b := &Broadcaster{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()
	fake := &fakeSocialSubscriber{
		subscribed:   make(chan xblsocial.SubscriptionHandler, 1),
		unsubscribed: make(chan xblsocial.SubscriptionHandler, 1),
	}

	trigger := b.subscribeSocial(fake, b.log)
	if trigger == nil {
		t.Fatal("subscribeSocial returned a nil trigger")
	}
	// The registered handler drives the returned trigger channel.
	var h xblsocial.SubscriptionHandler
	select {
	case h = <-fake.subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("social handler was not registered")
	}
	h.HandleIncomingFriendRequestCountChange(1)
	select {
	case <-trigger:
	case <-time.After(2 * time.Second):
		t.Fatal("social event did not reach the trigger")
	}

	// Shutdown unsubscribes exactly the handler it registered, and the
	// broadcaster's wait group tracks that cleanup.
	b.cancel()
	done := make(chan struct{})
	go func() { b.socialWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("socialWg.Wait did not return after shutdown")
	}
	select {
	case got := <-fake.unsubscribed:
		if got != h {
			t.Fatal("unsubscribed a different handler than was registered")
		}
	default:
		t.Fatal("subscription was not unsubscribed on shutdown")
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

func TestReactiveFriendSyncPreservesMutationBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		accepts := 0
		client := &syncFriendClient{
			people: []Person{{XUID: "123", IsFollowedByCaller: true}},
			accept: func(context.Context) ([]Person, error) {
				accepts++
				return nil, &xblsocial.ResponseError{StatusCode: 429, RetryAfter: time.Minute}
			},
		}
		trigger := make(chan struct{}, 1)
		syncer := FriendSyncer{
			Client:  client,
			Config:  FriendSyncConfig{AutoFollow: true, AutoUnfollow: true, UpdateInterval: time.Hour},
			Trigger: trigger,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go syncer.Run(ctx)
		synctest.Wait()
		if accepts != 1 || client.removeCalls != 1 {
			t.Fatalf("initial accepts=%d removals=%d, want 1 each", accepts, client.removeCalls)
		}
		trigger <- struct{}{}
		synctest.Wait()
		if accepts != 1 || client.removeCalls != 2 {
			t.Fatalf("during backoff accepts=%d removals=%d, want 1 and 2", accepts, client.removeCalls)
		}
		time.Sleep(time.Minute)
		trigger <- struct{}{}
		synctest.Wait()
		if accepts != 2 || client.removeCalls != 3 {
			t.Fatalf("after backoff accepts=%d removals=%d, want 2 and 3", accepts, client.removeCalls)
		}
	})
}

func TestSocialSubscriptionFailureKeepsPolling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &Broadcaster{log: testBroadcasterLogger()}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		defer b.cancel()
		sub := &fakeSocialSubscriber{
			subscribeErr: errors.New("RTA unavailable"),
			subscribed:   make(chan xblsocial.SubscriptionHandler, 1),
			unsubscribed: make(chan xblsocial.SubscriptionHandler, 1),
		}
		var accepts atomic.Int32
		syncer := FriendSyncer{
			Client: &syncFriendClient{accept: func(context.Context) ([]Person, error) {
				accepts.Add(1)
				return nil, nil
			}},
			Config:  FriendSyncConfig{AutoFollow: true, UpdateInterval: 20 * time.Second},
			Trigger: b.subscribeSocial(sub, b.log),
		}
		go syncer.Run(b.ctx)
		synctest.Wait()
		if accepts.Load() != 1 {
			t.Fatalf("initial polling accepts=%d, want 1", accepts.Load())
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if accepts.Load() != 2 {
			t.Fatalf("polling accepts=%d after failed subscription, want 2", accepts.Load())
		}
		b.cancel()
		b.socialWg.Wait()
		if len(sub.unsubscribed) != 0 {
			t.Fatal("failed subscription was unsubscribed")
		}
	})
}
