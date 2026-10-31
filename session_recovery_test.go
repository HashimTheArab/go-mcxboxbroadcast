package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
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
			signaling: &fakeSignaling{ctx: lost, networkID: "123"},
			conf: Config{
				XBLClient:      testXBLClient(t, "123"),
				JoinURL:        "http://127.0.0.1:19132",
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
		// Failed recovery has no relay. Close must still release its announcer.
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// Three failed updates in a row start recovery and notify once, even with update logs suppressed.
func TestSessionLoopRecoversAfterRepeatedUpdateFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var b *Broadcaster
		var notices []string
		var rebuilds []time.Duration
		start := time.Now()
		b = &Broadcaster{
			log:         testBroadcasterLogger(),
			started:     true,
			healthCheck: make(chan struct{}, 1),
			signaling:   &fakeSignaling{},
			announcer:   &fakeAnnouncer{announceErr: errors.New("update rejected")},
			conf: Config{
				XBLClient:                    testXBLClient(t, "123"),
				Status:                       Status{HostName: "Host"},
				UpdateInterval:               30 * time.Second,
				SuppressSessionUpdateMessage: true,
				Notifier: fakeNotifier{notify: func(_ context.Context, message string) {
					notices = append(notices, message)
				}},
				SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
					rebuilds = append(rebuilds, time.Since(start))
					b.cancel()
					return nil, errors.New("stop after recovery was requested")
				},
			},
		}
		b.ctx, b.cancel = context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			b.sessionLoop()
			close(done)
		}()
		time.Sleep(3 * b.conf.UpdateInterval)
		synctest.Wait()
		b.cancel()
		<-done
		if len(rebuilds) != 1 || rebuilds[0] != 3*b.conf.UpdateInterval {
			t.Fatalf("recovery started at %v, want once after three failed updates", rebuilds)
		}
		if len(notices) != 1 || !strings.Contains(notices[0], "update rejected") {
			t.Fatalf("notices = %q, want one update failure notice", notices)
		}
	})
}

// A rotation that cannot publish its new session falls back to full recovery.
func TestSessionLoopRecoversAfterFailedRotation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var b *Broadcaster
		rebuilds := 0
		b = &Broadcaster{
			log:         testBroadcasterLogger(),
			started:     true,
			healthCheck: make(chan struct{}, 1),
			signaling:   &fakeSignaling{},
			conf: Config{
				XBLClient:      testXBLClient(t, "123"),
				UpdateInterval: time.Hour,
				SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
					rebuilds++
					b.cancel()
					return nil, errors.New("stop after recovery was requested")
				},
			},
			announcerFactory: func(*Broadcaster) room.Announcer {
				return &fakeAnnouncer{announceErr: errors.New("publish rejected")}
			},
		}
		b.ctx, b.cancel = context.WithCancel(t.Context())
		b.announcer = b.wrapAnnouncer(&hidingAnnouncer{members: sessionRotateMembers})
		b.membersChanged(sessionRotateMembers)
		done := make(chan struct{})
		go func() {
			b.sessionLoop()
			close(done)
		}()
		synctest.Wait()
		b.cancel()
		<-done
		if rebuilds != 1 {
			t.Fatalf("rebuilds = %d, want recovery after the failed rotation", rebuilds)
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

func TestRetryWithBackoffSkipsOnErrorAfterContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var onErrCalls int
	err := retryWithBackoff(ctx, time.Millisecond, time.Millisecond, sessionRecoveryAttempts, func() error {
		return errors.New("broadcaster is shut down")
	}, func(error, time.Duration) {
		onErrCalls++
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retryWithBackoff error = %v, want context.Canceled", err)
	}
	if onErrCalls != 0 {
		t.Fatalf("onError called %d times after context cancel, want 0", onErrCalls)
	}
}

func TestSessionLoopCancellationDoesNotBecomeRecoveryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost, lose := context.WithCancel(context.Background())
		lose()
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: &fakeSignaling{ctx: lost},
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
		sig := &fakeSignaling{ctx: lost}
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

func TestBroadcasterRecoversSuccessiveSignalingDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var signals []*fakeSignaling
		var disconnects []context.CancelFunc
		for i := range 3 {
			ctx, cancel := context.WithCancel(t.Context())
			signals = append(signals, &fakeSignaling{ctx: ctx, networkID: fmt.Sprint(123456789 + i)})
			disconnects = append(disconnects, cancel)
		}
		var calls atomic.Int32
		announcers := make(chan *fakeAnnouncer, 3)
		status := &recoveryStatusProvider{}
		status.value.Store(&room.Status{HostName: "Host", WorldName: "World"})
		b, err := New(Config{
			JoinURL:              "http://127.0.0.1:19132",
			XBLClient:            testXBLClient(t, "123"),
			MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: uuid.New()},
			StatusProvider:       status,
			UpdateInterval:       time.Hour,
			Log:                  testBroadcasterLogger(),
			SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				i := int(calls.Add(1)) - 1
				if i >= len(signals) {
					return nil, fmt.Errorf("unexpected signaling factory call %d", i+1)
				}
				return signals[i], nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		b.announcerFactory = func(*Broadcaster) room.Announcer {
			announcer := &fakeAnnouncer{}
			announcers <- announcer
			return announcer
		}
		if err := b.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		previousAnnouncer := <-announcers
		for i := range 2 {
			b.mu.Lock()
			previousRelay := b.relay
			b.mu.Unlock()
			disconnects[i]()
			synctest.Wait()
			b.mu.Lock()
			relay, signaling, recovering := b.relay, b.signaling, b.recovering
			b.mu.Unlock()
			if relay == nil || relay == previousRelay || relay.signaling != signaling {
				t.Fatalf("recovery %d did not move the relay to the new signaling", i+1)
			}
			if signaling != signals[i+1] || signaling.Context().Err() != nil || recovering {
				t.Fatalf("recovery %d did not finish with the replacement signaling", i+1)
			}
			if got := calls.Load(); got != int32(i+2) {
				t.Fatalf("signaling factory calls = %d, want %d", got, i+2)
			}
			if !previousAnnouncer.Closed() {
				t.Fatalf("recovery %d left the old session open", i+1)
			}
			currentAnnouncer := <-announcers
			worldName := fmt.Sprintf("Recovered world %d", i+1)
			status.value.Store(&room.Status{HostName: "Host", WorldName: worldName})
			if err := b.Update(t.Context()); err != nil {
				t.Fatalf("update after recovery %d: %v", i+1, err)
			}
			announced := currentAnnouncer.Status()
			if announced.WorldName != worldName {
				t.Fatalf("updated world = %q, want %q", announced.WorldName, worldName)
			}
			if len(announced.SupportedConnections) != 1 || string(announced.SupportedConnections[0].NetherNetID) != signals[i+1].networkID {
				t.Fatalf("recovery %d advertised stale signaling: %+v", i+1, announced.SupportedConnections)
			}
			previousAnnouncer = currentAnnouncer
		}
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if err := b.Wait(); err != nil {
			t.Fatalf("shutdown after recovery: %v", err)
		}
	})
}

type recoveryStatusProvider struct {
	value atomic.Pointer[room.Status]
}

// RoomStatus safely reads metadata shared by the test and live listener.
func (p *recoveryStatusProvider) RoomStatus() room.Status { return *p.value.Load() }

// Thresholds match MCXboxBroadcast: rotate at 28 of 30 members, retire after 30 s.
func TestRotationThresholdsMatchJava(t *testing.T) {
	if sessionRotateMembers != 28 || sessionRotateGrace != 30*time.Second {
		t.Fatalf("rotate at %d members after %s, want 28 and 30s", sessionRotateMembers, sessionRotateGrace)
	}
	b := &Broadcaster{healthCheck: make(chan struct{}, 1)}
	b.membersChanged(sessionRotateMembers - 1)
	select {
	case <-b.healthCheck:
		t.Fatal("health check requested below the rotation threshold")
	default:
	}
	b.membersChanged(sessionRotateMembers)
	b.membersChanged(sessionRotateMembers + 1) // must not block on a pending request
	select {
	case <-b.healthCheck:
	default:
		t.Fatal("no health check requested at the rotation threshold")
	}
}

// Rotation publishes a new session on the same signaling and keeps the old one,
// hiding it only after the grace period when configured.
func TestRotateSessionRetiresOldSession(t *testing.T) {
	for _, hide := range []bool{true, false} {
		t.Run(fmt.Sprintf("hide=%v", hide), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				old := &hidingAnnouncer{}
				next := &hidingAnnouncer{}
				b := &Broadcaster{
					log:     testBroadcasterLogger(),
					started: true,
					conf: Config{
						XBLClient:       testXBLClient(t, "123"),
						Status:          Status{HostName: "Host"},
						HideOldSessions: hide,
					},
					announcerFactory: func(*Broadcaster) room.Announcer { return next },
				}
				b.ctx, b.cancel = context.WithCancel(t.Context())
				defer b.cancel()
				b.sessionConnection = &p2p.Connection{Type: p2p.ConnectionTypeSignalingOverJSONRPC, NetherNetID: "123456789", PlayerMessagingID: fakePlayerMessagingID}
				b.announcer = b.wrapAnnouncer(old)

				if err := b.rotateSession(t.Context()); err != nil {
					t.Fatal(err)
				}
				if unwrapAnnouncer(b.announcer) != next || next.Status().HostName != "Host" {
					t.Fatal("rotation did not publish the new session")
				}
				if got := next.Status().SupportedConnections; len(got) != 1 || got[0].NetherNetID != "123456789" {
					t.Fatalf("new session advertises %+v, want the same signaling connection", got)
				}
				if old.Closed() || len(b.retiredSessions) != 1 {
					t.Fatal("old session was closed instead of kept for its members")
				}
				time.Sleep(sessionRotateGrace - time.Nanosecond)
				synctest.Wait()
				if old.hidden() {
					t.Fatal("old session hidden before the grace period ended")
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				if old.hidden() != hide {
					t.Fatalf("old session hidden = %v, want %v", old.hidden(), hide)
				}
				if err := b.cleanupPublishedSessions(); err != nil {
					t.Fatal(err)
				}
				if !old.Closed() || len(b.retiredSessions) != 0 {
					t.Fatal("shutdown left a retired session open")
				}
			})
		})
	}
}

// A retired session is closed only once its grace period has passed and only the bot remains.
func TestCloseDrainedSessionsKeepsPlayersAndGracePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		playing := &hidingAnnouncer{members: 5}
		drained := &hidingAnnouncer{members: 1}
		unpublished := &hidingAnnouncer{members: -1}
		b := &Broadcaster{log: testBroadcasterLogger()}
		for _, a := range []*hidingAnnouncer{playing, drained, unpublished} {
			b.retire(b.wrapAnnouncer(a))
		}
		time.Sleep(sessionRotateGrace - time.Nanosecond)
		b.closeDrainedSessions()
		if drained.Closed() || len(b.retiredSessions) != 3 {
			t.Fatal("drained session closed during its grace period")
		}
		time.Sleep(time.Nanosecond)
		b.closeDrainedSessions()
		if !drained.Closed() || playing.Closed() || unpublished.Closed() || len(b.retiredSessions) != 2 {
			t.Fatalf("closed playing=%v drained=%v unpublished=%v, want only the drained session", playing.Closed(), drained.Closed(), unpublished.Closed())
		}
		playing.setMembers(1)
		b.closeDrainedSessions()
		if !playing.Closed() || len(b.retiredSessions) != 1 {
			t.Fatal("session was not closed after its players left")
		}
	})
}
