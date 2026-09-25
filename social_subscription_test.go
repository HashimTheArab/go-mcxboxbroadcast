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

// TestFriendSyncerRunSyncsOnTrigger checks that events can run ahead of polling.
func TestFriendSyncerRunSyncsOnTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		accepts := 0
		client := &syncFriendClient{
			accept: func(context.Context) ([]Person, error) {
				accepts++
				return nil, nil
			},
		}
		trigger := make(chan struct{}, 1)
		syncer := FriendSyncer{
			Client:  client,
			Config:  FriendSyncConfig{AutoFollow: true, UpdateInterval: time.Hour},
			Trigger: trigger,
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go syncer.Run(ctx)
		synctest.Wait()
		if accepts != 1 {
			t.Fatalf("initial passes = %d, want 1", accepts)
		}
		time.Sleep(30 * time.Second)
		trigger <- struct{}{}
		synctest.Wait()
		if accepts != 2 {
			t.Fatalf("passes after event = %d, want 2", accepts)
		}
	})
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
	attempts     atomic.Int32
	cleanupCalls atomic.Int32
}

// Subscribe records the handler and returns its cleanup or the configured error.
func (f *fakeSocialSubscriber) Subscribe(_ context.Context, h xblsocial.SubscriptionHandler) (func(context.Context) error, error) {
	f.attempts.Add(1)
	select {
	case f.subscribed <- h:
	default:
	}
	if f.subscribeErr != nil {
		return nil, f.subscribeErr
	}
	return func(context.Context) error {
		f.cleanupCalls.Add(1)
		return nil
	}, nil
}

// waitingSocialSubscriber leaves setup waiting until its context ends.
type waitingSocialSubscriber struct {
	result chan error
}

// Subscribe simulates an RTA subscription that never receives an acknowledgment.
func (s waitingSocialSubscriber) Subscribe(ctx context.Context, _ xblsocial.SubscriptionHandler) (func(context.Context) error, error) {
	<-ctx.Done()
	s.result <- ctx.Err()
	return nil, ctx.Err()
}

func TestSocialSubscriptionSetupTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &Broadcaster{log: testBroadcasterLogger()}
		b.ctx, b.cancel = context.WithCancel(t.Context())
		defer b.cancel()
		result := make(chan error, 1)
		b.subscribeSocial(waitingSocialSubscriber{result}, b.log)
		synctest.Wait()
		time.Sleep(defaultXboxOperationTimeout - time.Nanosecond)
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("setup ended early: %v", err)
		default:
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		select {
		case err := <-result:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("setup error = %v, want deadline exceeded", err)
			}
		default:
			t.Fatal("subscription setup did not time out")
		}
		if err := b.ctx.Err(); err != nil {
			t.Fatalf("setup timeout stopped the broadcaster: %v", err)
		}
		b.cancel()
		b.socialWg.Wait()
	})
}

// TestSubscribeSocialUnsubscribesOnShutdown verifies that a social subscription
// is undone when the broadcaster shuts down and that the shutdown waits for it,
// so a reused xsapi client does not accumulate stale handlers across restarts.
func TestSubscribeSocialUnsubscribesOnShutdown(t *testing.T) {
	b := &Broadcaster{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()
	fake := &fakeSocialSubscriber{
		subscribed: make(chan xblsocial.SubscriptionHandler, 1),
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

	// The broadcaster's wait group tracks the returned cleanup through shutdown.
	b.cancel()
	done := make(chan struct{})
	go func() { b.socialWg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("socialWg.Wait did not return after shutdown")
	}
	if got := fake.cleanupCalls.Load(); got != 1 {
		t.Fatalf("subscription cleanup calls = %d, want 1", got)
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
		if accepts != 1 || client.unfollowCalls != 1 {
			t.Fatalf("initial accepts=%d removals=%d, want 1 each", accepts, client.unfollowCalls)
		}
		trigger <- struct{}{}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if accepts != 1 || client.unfollowCalls != 2 {
			t.Fatalf("during backoff accepts=%d removals=%d, want 1 and 2", accepts, client.unfollowCalls)
		}
		time.Sleep(40 * time.Second)
		trigger <- struct{}{}
		synctest.Wait()
		if accepts != 2 || client.unfollowCalls != 3 {
			t.Fatalf("after backoff accepts=%d removals=%d, want 2 and 3", accepts, client.unfollowCalls)
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
		if got := sub.attempts.Load(); got < 2 {
			t.Fatalf("subscribe attempts = %d after 20s, want retries", got)
		}
		b.cancel()
		b.socialWg.Wait()
		if sub.cleanupCalls.Load() != 0 {
			t.Fatal("failed subscription was cleaned up")
		}
	})
}

// A lost subscription must be replaced, releasing the old registration first.
func TestSubscribeSocialResubscribesAfterLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := &Broadcaster{log: testBroadcasterLogger()}
		b.ctx, b.cancel = context.WithCancel(t.Context())
		defer b.cancel()
		sub := &fakeSocialSubscriber{subscribed: make(chan xblsocial.SubscriptionHandler, 1)}
		trigger := b.subscribeSocial(sub, b.log)
		synctest.Wait()
		h := <-sub.subscribed
		if len(trigger) != 0 {
			t.Fatal("first subscription should not request an extra sync")
		}

		h.HandleSubscriptionLost()
		synctest.Wait()
		if sub.cleanupCalls.Load() != 1 {
			t.Fatalf("cleanup calls = %d, want the lost registration released", sub.cleanupCalls.Load())
		}
		time.Sleep(socialResubscribeMinDelay)
		synctest.Wait()
		if got := sub.attempts.Load(); got != 2 {
			t.Fatalf("subscribe attempts = %d, want 2", got)
		}
		select {
		case <-trigger:
		default:
			t.Fatal("resubscribing should request a sync to catch up")
		}
		b.cancel()
		b.socialWg.Wait()
		if sub.cleanupCalls.Load() != 2 {
			t.Fatalf("cleanup calls = %d, want 2 after shutdown", sub.cleanupCalls.Load())
		}
	})
}
