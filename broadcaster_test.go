package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

func TestXBLAnnouncerUnwrapsDiagnosticsWrappers(t *testing.T) {
	inner := &room.XBLAnnouncer{}
	wrapped := signalingConnectionAnnouncer{
		Announcer: loggingAnnouncer{Announcer: inner},
		connection: p2p.Connection{
			Type: p2p.ConnectionTypeSignalingOverJSONRPC,
		},
	}
	got, ok := xblAnnouncer(wrapped)
	if !ok {
		t.Fatal("xbl announcer was not found")
	}
	if got != inner {
		t.Fatal("unexpected xbl announcer")
	}
}

func TestBroadcasterInviteRequiresActiveBroadcaster(t *testing.T) {
	var b Inviter = &Broadcaster{announcer: &room.XBLAnnouncer{Session: &mpsd.Session{}}}
	err := b.Invite(context.Background(), "456")
	if err == nil || !strings.Contains(err.Error(), "broadcaster not started") {
		t.Fatalf("Invite error = %v, want broadcaster-not-started error", err)
	}
}

func TestBroadcasterUpdateLogsUpdatedSession(t *testing.T) {
	var log bytes.Buffer
	b := &Broadcaster{
		log:       slog.New(slog.NewTextHandler(&log, nil)),
		announcer: &fakeAnnouncer{},
		started:   true,
		conf: Config{
			JoinURL:   "http://127.0.0.1:19132",
			XBLClient: testXBLClient(t, "123"),
			Status:    Status{HostName: "Host", WorldName: "World"},
		},
	}

	if err := b.Update(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := log.String(); !strings.Contains(got, "updated session") {
		t.Fatalf("updated session log missing: %q", got)
	}
}

func TestBroadcasterSignalingFactoryIsUsedOnceForSharedSignaling(t *testing.T) {
	var calls int
	sig := &fakeSignaling{networkID: "123456789"}
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			JoinURL:              "http://127.0.0.1:19132",
			XBLClient:            testXBLClient(t, "123"),
			MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: uuid.New()},
			Status:               Status{HostName: "Host", WorldName: "World"},
			UpdateInterval:       30 * time.Second,
			SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				calls++
				return sig, nil
			},
		},
		announcerFactory: func(*Broadcaster) room.Announcer {
			return &fakeAnnouncer{}
		},
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	if calls != 1 {
		t.Fatalf("signaling factory calls = %d, want 1", calls)
	}
	if b.signaling != sig {
		t.Fatal("broadcaster did not keep the shared signaling instance")
	}
}

func TestWaitConcurrentWithStart(t *testing.T) {
	startErr := errors.New("signaling startup failed")
	for range 100 {
		b := &Broadcaster{
			log: testBroadcasterLogger(),
			conf: Config{SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				return nil, startErr
			}},
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := b.Start(t.Context()); !errors.Is(err, startErr) {
				t.Errorf("Start() = %v, want startup failure", err)
			}
		})
		wg.Go(func() {
			if err := b.Wait(); err != nil {
				t.Errorf("Wait() = %v, want no runtime recovery failure", err)
			}
		})
		wg.Wait()
	}
}

// A version without a protocol from the config file is rejected before anything is advertised.
func TestNewRejectsHalfAdvertisedPair(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.Session.JoinURL = "https://bedrock.test:19132"
	cfg.Session.SessionInfo.Version = "1.26.50"
	if _, err := New(cfg.RuntimeConfig(RuntimeConfigInput{XBLClient: testXBLClient(t, "123")})); err == nil {
		t.Fatal("New accepted a version without a protocol")
	}
}

// A spent context, such as an expired upload deadline, must not drop the notification.
func TestNotifySurvivesSpentContext(t *testing.T) {
	spent, cancel := context.WithCancel(context.Background())
	cancel()
	for name, ctx := range map[string]context.Context{"live": context.Background(), "spent": spent} {
		var notified bool
		var gotErr error
		b := &Broadcaster{conf: Config{Notifier: fakeNotifier{notify: func(ctx context.Context, _ string) {
			notified = true
			gotErr = ctx.Err()
		}}}}
		b.notify(ctx, "message")
		if !notified || gotErr != nil {
			t.Fatalf("%s: notified = %v with context error %v, want a live notification", name, notified, gotErr)
		}
	}
}

func TestStartAdvertisesJSONRPCForInjectedSignaling(t *testing.T) {
	sig := &fakeSignaling{networkID: "123456789"}
	announcer := &fakeAnnouncer{}
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			JoinURL:        "http://127.0.0.1:19132",
			XBLClient:      testXBLClient(t, "123"),
			Signaling:      sig,
			Status:         Status{HostName: "Host", WorldName: "World"},
			UpdateInterval: 30 * time.Second,
		},
		announcerFactory: func(*Broadcaster) room.Announcer {
			return announcer
		},
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	status := announcer.Status()
	if len(status.SupportedConnections) != 1 {
		t.Fatalf("unexpected connections: %#v", status.SupportedConnections)
	}
	got := status.SupportedConnections[0]
	if got.Type != p2p.ConnectionTypeSignalingOverJSONRPC {
		t.Fatalf("connection type = %d, want %d", got.Type, p2p.ConnectionTypeSignalingOverJSONRPC)
	}
	if got.NetherNetID != p2p.NetherNetID("123456789") {
		t.Fatalf("nethernet id = %q", got.NetherNetID)
	}
	if got.PlayerMessagingID != fakePlayerMessagingID {
		t.Fatalf("pmsg id = %s, want %s", got.PlayerMessagingID, fakePlayerMessagingID)
	}
	if status.OwnerID != "123" {
		t.Fatalf("owner id = %q", status.OwnerID)
	}
	if status.TransportLayer != p2p.TransportLayerNetherNet {
		t.Fatalf("transport layer = %d", status.TransportLayer)
	}
	if status.TitleID != 0 {
		t.Fatalf("title id = %d", status.TitleID)
	}
}

func TestStartAdvertisesOpaqueNetherNetID(t *testing.T) {
	pmsgID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	networkID := uuid.MustParse("11111111-2222-3333-4444-555555555555").String()
	sig := &fakeSignaling{networkID: networkID}
	announcer := &fakeAnnouncer{}
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			JoinURL:              "http://127.0.0.1:19132",
			XBLClient:            testXBLClient(t, "123"),
			Signaling:            sig,
			MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: pmsgID},
			Status:               Status{HostName: "Host", WorldName: "World"},
			UpdateInterval:       30 * time.Second,
		},
		announcerFactory: func(*Broadcaster) room.Announcer {
			return announcer
		},
	}

	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	status := announcer.Status()
	if len(status.SupportedConnections) != 1 {
		t.Fatalf("unexpected connections: %#v", status.SupportedConnections)
	}
	if got := status.SupportedConnections[0].NetherNetID; got != p2p.NetherNetID(networkID) {
		t.Fatalf("nethernet id = %q, want %q", got, networkID)
	}
}

// Signaling startup, including creating the Minecraft token source, gives up at the dial timeout.
func TestStartTimesOutDefaultSignalingDial(t *testing.T) {
	for name, tokens := range map[string]service.TokenSource{
		"dial":         minecraftTokenSourceWithPMID{pmid: uuid.New()},
		"token source": nil,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var requests atomic.Int32
				b, err := New(Config{
					JoinURL:              "http://127.0.0.1:19132",
					XBLClient:            testXBLClient(t, "123"),
					MinecraftTokenSource: tokens,
					HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
						requests.Add(1)
						<-req.Context().Done()
						return nil, req.Context().Err()
					})},
					Log: testBroadcasterLogger(),
				})
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				if err := b.Start(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Start() error = %v, want context deadline exceeded", err)
				}
				if elapsed := time.Since(start); elapsed != defaultSignalingDialTimeout || requests.Load() == 0 {
					t.Fatalf("gave up after %v and %d requests, want %v after a blocked request", elapsed, requests.Load(), defaultSignalingDialTimeout)
				}
				// Service discovery keeps retrying in a detached goroutine; let it give up.
				time.Sleep(time.Hour)
				synctest.Wait()
			})
		})
	}
}

func TestSignalingStartupHTTPClientBoundsTimeout(t *testing.T) {
	base := &http.Client{}
	got := signalingStartupHTTPClient(base, 2*time.Second)
	if got == base {
		t.Fatal("expected a cloned client when adding a timeout")
	}
	if got.Timeout != 2*time.Second {
		t.Fatalf("timeout = %s, want 2s", got.Timeout)
	}
	if base.Timeout != 0 {
		t.Fatalf("base client was mutated: timeout = %s", base.Timeout)
	}

	existing := &http.Client{Timeout: time.Second}
	if got := signalingStartupHTTPClient(existing, 2*time.Second); got != existing {
		t.Fatal("expected existing tighter timeout to be reused")
	}
}

func TestStartupFailureCleanupClosesSignaling(t *testing.T) {
	sig := &fakeSignaling{}
	b := &Broadcaster{signaling: sig}
	if err := b.releaseSessions(); err != nil {
		t.Fatal(err)
	}
	if !sig.closed.Load() {
		t.Fatal("signaling was not closed")
	}
}

func TestStartCleansUpWhenAnnounceFails(t *testing.T) {
	announceErr := errors.New("announce failed")
	sig := &fakeSignaling{}
	announcer := &fakeAnnouncer{announceErr: announceErr}
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			JoinURL:        "http://127.0.0.1:19132",
			XBLClient:      testXBLClient(t, "123"),
			Signaling:      sig,
			StatusProvider: room.NewStatusProvider(room.Status{HostName: "Host", WorldName: "World"}),
		},
		announcerFactory: func(*Broadcaster) room.Announcer { return announcer },
	}
	if err := b.Start(context.Background()); !errors.Is(err, announceErr) {
		t.Fatalf("Start() error = %v, want announce error", err)
	}
	if !announcer.Closed() || !sig.closed.Load() {
		t.Fatalf("announcer closed = %v, signaling closed = %v, want both", announcer.Closed(), sig.closed.Load())
	}
}

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
