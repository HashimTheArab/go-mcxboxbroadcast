package broadcaster

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestSessionLoopStopsAfterRepeatedPublicationFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publishErr := errors.New("write activity handle: 403 Forbidden")
		lost, lose := context.WithCancel(context.Background())
		lose()
		var attempts []time.Duration
		var notices []string
		start := time.Now()
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: &cancelableSignaling{ctx: lost, networkID: "123"},
			conf: Config{
				XUID:           "123",
				Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
				Status:         Status{HostName: "Host", WorldName: "World"},
				UpdateInterval: 30 * time.Second,
				SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
					attempts = append(attempts, time.Since(start))
					return &fakeSignaling{}, nil
				},
				Notifier: fakeNotifier{notify: func(_ context.Context, message string) {
					notices = append(notices, message)
				}},
			},
			announcerFactory: func(*Broadcaster) room.Announcer {
				return &fakeAnnouncer{announceErr: publishErr}
			},
		}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		defer b.cancel()
		done := make(chan struct{})
		go func() {
			b.sessionLoop()
			close(done)
		}()
		<-done
		if err := b.Wait(); !errors.Is(err, publishErr) {
			t.Fatalf("Wait() = %v, want publication failure", err)
		}
		want := []time.Duration{0, 5 * time.Second, 15 * time.Second, 35 * time.Second, 75 * time.Second, 155 * time.Second}
		if !reflect.DeepEqual(attempts, want) {
			t.Fatalf("rebuild times = %v, want %v; update ticks must not bypass recovery backoff", attempts, want)
		}
		if len(notices) != 1 || !strings.Contains(notices[0], "403 Forbidden") {
			t.Fatalf("notices = %v, want one recovery failure notice", notices)
		}
		// Failed recovery has no listener; Close must still return.
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRetryWithBackoffRecoversTransientFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts, notices := 0, 0
		err := retryWithBackoff(context.Background(), time.Second, time.Minute, sessionRecoveryAttempts, func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary failure")
			}
			return nil
		}, func(error, time.Duration) { notices++ })
		if err != nil || attempts != 3 || notices != 2 {
			t.Fatalf("err=%v attempts=%d notices=%d", err, attempts, notices)
		}
	})
}

func TestSessionLoopCancellationDoesNotBecomeRecoveryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost, lose := context.WithCancel(context.Background())
		lose()
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: &cancelableSignaling{ctx: lost},
			conf: Config{UpdateInterval: time.Minute, SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				return nil, errors.New("temporary failure")
			}},
		}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { b.sessionLoop(); close(done) }()
		synctest.Wait()
		b.cancel()
		<-done
		if err := b.Wait(); err != nil {
			t.Fatalf("normal cancellation returned %v", err)
		}
	})
}

func TestSessionLoopDoesNotRebuildStaticSignaling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost, lose := context.WithCancel(context.Background())
		lose()
		sig := &cancelableSignaling{ctx: lost}
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: sig,
			announcer: &fakeAnnouncer{},
			conf:      Config{Signaling: sig, UpdateInterval: time.Minute},
		}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { b.sessionLoop(); close(done) }()
		synctest.Wait()
		b.cancel()
		<-done
		if b.signaling != sig || b.failure != nil {
			t.Fatal("static signaling was rebuilt or treated as terminal failure")
		}
	})
}

func TestSlowSubAccountRecoveryDoesNotExpirePrimaryUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		primary := &contextCheckingAnnouncer{}
		b := &Broadcaster{
			log:           testBroadcasterLogger(),
			ctx:           context.Background(),
			started:       true,
			announcer:     primary,
			subAnnouncers: []publishedSubAccount{{id: "sub", announcer: &fakeAnnouncer{announceErr: errors.New("sub-account unavailable")}}},
			conf: Config{
				XUID:        "primary",
				SubAccounts: []SubAccountConfig{{ID: "sub", Enabled: true, XUID: "sub", XBLClient: &xsapi.Client{}}},
				HTTPClient: &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					<-req.Context().Done()
					return nil, req.Context().Err()
				})},
			},
		}
		err := b.refreshSession(sessionHealthIssue{reason: "sub-account session lost", subAccountID: "sub"})
		if err == nil || countsAsPrimaryUpdateFailure(err) {
			t.Fatalf("refreshSession() = %v, want a sub-account-only failure", err)
		}
		if primary.updates != 1 || primary.contextErr != nil {
			t.Fatalf("primary updates=%d context error=%v, want one update with a fresh context", primary.updates, primary.contextErr)
		}
	})
}

// contextCheckingAnnouncer checks the request context just as an HTTP write would.
type contextCheckingAnnouncer struct {
	updates    int
	contextErr error
}

// Announce records whether the primary had a usable request context.
func (a *contextCheckingAnnouncer) Announce(ctx context.Context, _ room.Status) error {
	a.updates++
	a.contextErr = ctx.Err()
	return a.contextErr
}

// Close has no resources to release for this test announcer.
func (*contextCheckingAnnouncer) Close() error { return nil }

// A listener announcement stuck on a hung MPSD write must not make Update
// overrun its own deadline or keep holding the broadcaster lock.
func TestUpdateReturnsWhileListenerAnnounceHangs(t *testing.T) {
	f := newFakeXbox(t)
	b, nonce := newFakeXboxBroadcaster(t, f)
	f.mu.Lock()
	f.hangCustom = make(chan struct{})
	f.mu.Unlock()
	b.conf.Status.Players = 3
	status, err := b.status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b.resolvedStatus.Store(&status)
	listener := b.roomListenConfig(b.announcer).Wrap(fakeNetworkListener{})
	t.Cleanup(func() { _ = listener.Close() })
	go listener.ServerStatus(minecraft.ServerStatus{})
	for len(nonce.busy) == 0 {
		time.Sleep(time.Millisecond)
	}

	b.conf.Status.Players = 4
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- b.Update(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Update = %v, want %v", err, context.DeadlineExceeded)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Update ignored its deadline behind a hung listener announcement")
	}
	if !b.mu.TryLock() {
		t.Fatal("Update left the broadcaster lock held")
	}
	b.mu.Unlock()
}

// Losing only the MPSD session, here deleted by Xbox, must republish a new
// session over the existing signaling, including injected static signaling.
func TestSessionLoopRepublishesDeletedSession(t *testing.T) {
	f := newFakeXbox(t)
	b, nonce := newFakeXboxBroadcaster(t, f)
	sig := &fakeSignaling{}
	b.conf.Signaling, b.signaling = sig, sig
	b.conf.UpdateInterval = 20 * time.Millisecond
	old := nonce.session()
	oldName := nonce.SessionReference.Name

	f.mu.Lock()
	f.deleted = true
	f.mu.Unlock()
	f.tap(nonce.SessionReference)
	select {
	case <-old.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session deleted by Xbox stayed open after a shoulder tap")
	}
	if issue := b.sessionHealthIssue(); issue.reason != "mpsd session lost" {
		t.Fatalf("health issue = %+v, want lost MPSD session", issue)
	}

	b.loopWg.Add(1)
	go func() {
		defer b.loopWg.Done()
		b.sessionLoop()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if session := nonce.session(); session != nil && session != old && session.Context().Err() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lost MPSD session was not republished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.cancel()
	b.loopWg.Wait()
	f.mu.Lock()
	created := slices.Clone(f.created)
	f.mu.Unlock()
	if len(created) != 2 || strings.EqualFold(created[1], oldName) {
		t.Fatalf("published sessions = %v, want a second session under a new name", created)
	}
	if b.signaling != sig || sig.closed {
		t.Fatal("republishing the MPSD session replaced or closed signaling")
	}
}

// A stale activity result must not discard a primary session that has
// already been replaced.
func TestRepublishIgnoresReplacedPrimarySession(t *testing.T) {
	f := newFakeXbox(t)
	b, nonce := newFakeXboxBroadcaster(t, f)
	current := nonce.session()
	stale := &publishedActivity{announcer: &room.XBLAnnouncer{}, session: current}
	if !b.republishPrimarySession(sessionHealthIssue{reason: "published activity handle missing", activity: stale}) {
		t.Fatal("republish reported an unsupported announcer")
	}
	if nonce.session() != current || current.Context().Err() != nil {
		t.Fatal("stale activity issue discarded the current session")
	}
}

// A primary session whose close fails must be retried, so it is not left
// registered and reactivated when RTA reconnects.
func TestFailedPrimarySessionCloseIsRetried(t *testing.T) {
	f := newFakeXbox(t)
	b, nonce := newFakeXboxBroadcaster(t, f)
	ghost := nonce.session()
	f.mu.Lock()
	f.failClose = true
	f.mu.Unlock()

	b.mu.Lock()
	failed, err := closeSessionStack(b.detachSessionStack())
	b.mu.Unlock()
	if err == nil {
		t.Fatal("close against an unavailable directory reported success")
	}
	b.retainStaleSessions(failed)
	if ghost.Context().Err() != nil {
		t.Fatal("test setup: failed close already released the session")
	}

	f.mu.Lock()
	f.failClose = false
	f.mu.Unlock()
	if err := b.retryStaleSessionCloses(); err != nil {
		t.Fatal(err)
	}
	if ghost.Context().Err() == nil || len(b.staleSessions) != 0 {
		t.Fatal("retry did not close the session")
	}
	f.mu.Lock()
	deleted := f.deleted
	f.mu.Unlock()
	if !deleted {
		t.Fatal("retried close did not remove the session from Xbox")
	}
}

// Close must cancel in-flight recovery instead of waiting for it.
func TestCloseCancelsInFlightRecovery(t *testing.T) {
	lost, lose := context.WithCancel(t.Context())
	dialing := make(chan struct{})
	var calls int
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			XUID:                 "123",
			Server:               ServerInfo{Host: "127.0.0.1", Port: 19132},
			Status:               Status{HostName: "Host", WorldName: "World"},
			UpdateInterval:       time.Hour,
			MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: uuid.New()},
			ListenConfig:         minecraft.ListenConfig{AuthenticationDisabled: true},
			SignalingFactory: func(ctx context.Context, _ Config) (nethernet.Signaling, error) {
				calls++
				if calls == 1 {
					return &cancelableSignaling{ctx: lost, networkID: "123"}, nil
				}
				close(dialing)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
		announcerFactory: func(*Broadcaster) room.Announcer { return &fakeAnnouncer{} },
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	lose()
	select {
	case <-dialing:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery did not start")
	}
	done := make(chan error, 1)
	go func() { done <- b.Close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for in-flight recovery")
	}
}

// Close must not wait for an Update whose caller context has no deadline.
func TestCloseCancelsUpdateWithUnboundedContext(t *testing.T) {
	f := newFakeXbox(t)
	b, nonce := newFakeXboxBroadcaster(t, f)
	f.mu.Lock()
	f.hangCustom = make(chan struct{})
	f.mu.Unlock()
	b.conf.Status.Players = 3
	updated := make(chan error, 1)
	go func() { updated <- b.Update(context.Background()) }()
	for len(nonce.busy) == 0 {
		time.Sleep(time.Millisecond)
	}

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close waited for an Update stuck on a hung MPSD write")
	}
	if err := <-updated; !errors.Is(err, context.Canceled) {
		t.Fatalf("Update = %v, want %v", err, context.Canceled)
	}
}
