package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

func TestSubAccountStatusOwnsIndependentActivity(t *testing.T) {
	primaryConnection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "primary-nethernet",
		PmsgID:         uuid.New(),
	}
	subConnection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "sub-nethernet",
		PmsgID:         uuid.New(),
	}
	primary := room.Status{
		OwnerID:              "primary",
		LevelID:              accountLevelID("primary"),
		HostName:             "Lunar",
		WorldName:            "Lunar",
		SupportedConnections: []room.Connection{primaryConnection},
	}

	got := subAccountStatusWithConnection(primary, "sub", subConnection)

	if got.OwnerID != "sub" {
		t.Fatalf("OwnerID = %q, want sub", got.OwnerID)
	}
	if got.LevelID != accountLevelID("sub") {
		t.Fatalf("LevelID = %q, want account-specific level id", got.LevelID)
	}
	if len(got.SupportedConnections) != 1 || got.SupportedConnections[0] != subConnection {
		t.Fatalf("SupportedConnections = %#v, want independent sub-account connection %#v", got.SupportedConnections, subConnection)
	}
	if primary.OwnerID != "primary" || primary.LevelID != accountLevelID("primary") {
		t.Fatalf("subAccountStatus mutated primary status: %#v", primary)
	}
}

func TestBroadcasterStartSubAccountPublishesIndependentSessionAndSignaling(t *testing.T) {
	primaryConnection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "primary-nethernet",
		PmsgID:         uuid.New(),
	}
	subConnection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "sub-nethernet",
		PmsgID:         uuid.New(),
	}
	sub := &fakeAnnouncer{}
	subSignaling := &fakeSignaling{networkID: "sub-nethernet"}
	subListener := newTrackedListener()
	var ref mpsd.SessionReference
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "peoplehub.xboxlive.com" {
			t.Fatalf("unexpected social request: %s %s", req.Method, req.URL)
		}
		return broadcasterResponse(http.StatusOK, `{"people":[{"xuid":"primary","isFollowingCaller":true,"isFollowedByCaller":true}]}`), nil
	})}
	b := &Broadcaster{
		log:               testBroadcasterLogger(),
		ctx:               context.Background(),
		signaling:         &fakeSignaling{networkID: "primary-nethernet"},
		sessionRef:        mpsd.SessionReference{ServiceConfigID: serviceConfigUUID, TemplateName: TemplateName, Name: "PRIMARY"},
		sessionConnection: &primaryConnection,
		conf: Config{
			XBLClient:  &xsapi.Client{},
			XUID:       "primary",
			HTTPClient: client,
		},
		subAccountAnnouncerFactory: func(_ context.Context, _ SubAccountConfig, got mpsd.SessionReference) (room.Announcer, error) {
			ref = got
			return sub, nil
		},
		subAccountSignalingFactory: func(context.Context, *SubAccountConfig) (nethernet.Signaling, error) {
			return subSignaling, nil
		},
		subAccountConnectionFactory: func(context.Context, *SubAccountConfig, nethernet.Signaling) (*room.Connection, error) {
			return &subConnection, nil
		},
		subAccountListenerFactory: func(nethernet.Signaling, room.Status) (net.Listener, error) {
			return subListener, nil
		},
	}
	account := &SubAccountConfig{ID: "sub1", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "sub"}
	status := room.Status{OwnerID: "primary", LevelID: accountLevelID("primary")}

	if err := b.startSubAccount(context.Background(), account, status); err != nil {
		t.Fatal(err)
	}

	if ref.Name == "" || ref.Name == b.sessionRef.Name {
		t.Fatalf("sub-account session name = %q, want non-empty name distinct from primary %q", ref.Name, b.sessionRef.Name)
	}
	got := sub.Status()
	if got.OwnerID != "sub" || got.LevelID != accountLevelID("sub") {
		t.Fatalf("sub-account published primary-owned status: %#v", got)
	}
	if len(got.SupportedConnections) != 1 || got.SupportedConnections[0] != subConnection {
		t.Fatalf("sub-account connection = %#v, want independent connection %#v", got.SupportedConnections, subConnection)
	}
	if b.subAnnouncersByID["sub1"] == nil {
		t.Fatal("sub-account announcer was not retained for updates and invites")
	}
	if len(b.subAnnouncers) != 1 || b.subAnnouncers[0].signaling != subSignaling || b.subAnnouncers[0].listener != subListener {
		t.Fatalf("sub-account endpoint not retained: %#v", b.subAnnouncers)
	}
	if err := b.cleanupPublishedSessions(false); err != nil {
		t.Fatal(err)
	}
	b.acceptWg.Wait()
	if !sub.Closed() || !subSignaling.closed || !subListener.Closed() {
		t.Fatalf("cleanup incomplete: announcer=%v signaling=%v listener=%v", sub.Closed(), subSignaling.closed, subListener.Closed())
	}
}

func TestBroadcasterUpdateRefreshesSubAccountSession(t *testing.T) {
	primary := &fakeAnnouncer{}
	sub := &fakeAnnouncer{}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		announcer: primary,
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: sub,
		}},
		started: true,
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:   "primary",
			Status: Status{HostName: "Host", WorldName: "World"},
			SubAccounts: []SubAccountConfig{{
				ID:      "sub1",
				Enabled: true,
				XUID:    "sub",
			}},
		},
	}

	if err := b.Update(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := primary.Status(); got.OwnerID != "primary" {
		t.Fatalf("primary OwnerID = %q, want primary", got.OwnerID)
	}
	if got := sub.Status(); got.OwnerID != "sub" || got.LevelID != accountLevelID("sub") {
		t.Fatalf("sub-account update used primary ownership: %#v", got)
	}
}

func TestBroadcasterSubAccountUpdateFailureDoesNotCountAsPrimaryFailure(t *testing.T) {
	primary := &fakeAnnouncer{}
	sub := &fakeAnnouncer{announceErr: fmt.Errorf("sub update failed")}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		announcer: primary,
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: sub,
		}},
		started: true,
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:   "primary",
			Status: Status{HostName: "Host", WorldName: "World"},
		},
	}

	err := b.Update(context.Background())
	var subErr *subAccountUpdateError
	if !errors.As(err, &subErr) {
		t.Fatalf("Update() error = %v, want subAccountUpdateError", err)
	}
	if countsAsPrimaryUpdateFailure(err) {
		t.Fatalf("sub-account-only update error counted as primary failure: %v", err)
	}
	if !countsAsPrimaryUpdateFailure(fmt.Errorf("primary update failed")) {
		t.Fatal("primary update error did not count as primary failure")
	}
}

func TestBroadcasterCleanupClosesIndependentSubAccountSessions(t *testing.T) {
	sub := &fakeAnnouncer{}
	signaling := &fakeSignaling{}
	listener := newTrackedListener()
	b := &Broadcaster{subAnnouncers: []publishedSubAccount{{
		id:        "sub1",
		xuid:      "sub",
		announcer: sub,
		signaling: signaling,
		listener:  listener,
	}}}

	if err := b.cleanupPublishedSessions(false); err != nil {
		t.Fatal(err)
	}
	if !sub.Closed() {
		t.Fatal("sub-account announcer was not closed")
	}
	if !signaling.closed || !listener.Closed() {
		t.Fatalf("sub-account transport not closed: signaling=%v listener=%v", signaling.closed, listener.Closed())
	}
	if len(b.subAnnouncersByID) != 0 {
		t.Fatalf("sub-account announcers retained after cleanup: %#v", b.subAnnouncersByID)
	}
}

func TestBroadcasterSubAccountListenerFailureClosesPublishedResources(t *testing.T) {
	announcer := &fakeAnnouncer{}
	signaling := &fakeSignaling{networkID: "sub-nethernet"}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		ctx:       context.Background(),
		signaling: &fakeSignaling{networkID: "primary-nethernet"},
		conf: Config{
			XBLClient: &xsapi.Client{},
			XUID:      "primary",
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			return announcer, nil
		},
		subAccountSignalingFactory: func(context.Context, *SubAccountConfig) (nethernet.Signaling, error) {
			return signaling, nil
		},
		subAccountConnectionFactory: func(context.Context, *SubAccountConfig, nethernet.Signaling) (*room.Connection, error) {
			return &room.Connection{ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC, NetherNetID: "sub-nethernet", PmsgID: uuid.New()}, nil
		},
		subAccountListenerFactory: func(nethernet.Signaling, room.Status) (net.Listener, error) {
			return nil, errors.New("listen failed")
		},
	}
	account := &SubAccountConfig{ID: "sub1", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "primary"}

	err := b.startSubAccount(context.Background(), account, room.Status{})
	if err == nil || !strings.Contains(err.Error(), "listen failed") {
		t.Fatalf("startSubAccount() error = %v, want listener failure", err)
	}
	if !announcer.Closed() || !signaling.closed {
		t.Fatalf("failed startup leaked resources: announcer=%v signaling=%v", announcer.Closed(), signaling.closed)
	}
	if len(b.subAnnouncers) != 0 {
		t.Fatalf("failed sub-account retained: %#v", b.subAnnouncers)
	}
}

func TestBroadcasterRejectsPrimarySignalingForSubAccount(t *testing.T) {
	primary := &fakeSignaling{networkID: "primary-nethernet"}
	b := &Broadcaster{
		signaling: primary,
		subAccountSignalingFactory: func(context.Context, *SubAccountConfig) (nethernet.Signaling, error) {
			return primary, nil
		},
	}

	_, err := b.subAccountSignalingFor(context.Background(), &SubAccountConfig{}, primary)
	if err == nil || !strings.Contains(err.Error(), "primary signaling instance") {
		t.Fatalf("subAccountSignalingFor() error = %v, want shared-instance rejection", err)
	}
	if primary.closed {
		t.Fatal("rejecting a shared signaling instance closed the primary endpoint")
	}
}

func TestBroadcasterSubAccountTokenSetupHonoursStartupDeadline(t *testing.T) {
	b := &Broadcaster{
		ctx:  context.Background(),
		conf: Config{SignalingMode: SignalingModeJSONRPC},
		subAccountMinecraftTokenSourceFactory: func(ctx context.Context, _ *SubAccountConfig) (service.TokenSource, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := b.subAccountSignalingFor(ctx, &SubAccountConfig{}, nil)
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("subAccountSignalingFor() error = %v, want context deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("sub-account token setup ignored the startup deadline")
	}
}

func TestBroadcasterSubAccountConnectionUsesSubAccountPMID(t *testing.T) {
	pmid := uuid.New()
	b := &Broadcaster{conf: Config{SignalingMode: SignalingModeJSONRPC}}
	account := &SubAccountConfig{
		MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: pmid},
	}

	connection, err := b.subAccountConnection(context.Background(), account, &fakeSignaling{networkID: "sub-nethernet"})
	if err != nil {
		t.Fatal(err)
	}
	if connection.NetherNetID != "sub-nethernet" || connection.PmsgID != pmid {
		t.Fatalf("connection = %#v, want sub-account network id and PMID", connection)
	}
}

func TestBroadcasterDetectsLostSubAccountSignaling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &Broadcaster{
		announcer: &room.XBLAnnouncer{Session: &mpsd.Session{}},
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			signaling: &cancelableSignaling{ctx: ctx, networkID: "sub-nethernet"},
		}},
	}

	id, signaling := b.lostSubAccountSignaling()
	if id != "sub1" || signaling != b.subAnnouncers[0].signaling {
		t.Fatalf("lostSubAccountSignaling() = (%q, %p), want sub1 signaling", id, signaling)
	}
}

func TestBroadcasterReconnectsLostSubAccountWithoutRestartingPrimary(t *testing.T) {
	lostCtx, lose := context.WithCancel(context.Background())
	lose()
	primary := &fakeSignaling{networkID: "primary-nethernet"}
	lost := &cancelableSignaling{ctx: lostCtx, networkID: "lost-sub-nethernet"}
	replacement := &fakeSignaling{networkID: "replacement-sub-nethernet"}
	oldAnnouncer := &fakeAnnouncer{}
	newAnnouncer := &fakeAnnouncer{}
	oldListener := newTrackedListener()
	newListener := newTrackedListener()
	primaryAnnouncer := &fakeAnnouncer{}
	connection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "replacement-sub-nethernet",
		PmsgID:         uuid.New(),
	}
	b := &Broadcaster{
		log:               testBroadcasterLogger(),
		signaling:         primary,
		sessionConnection: &room.Connection{ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC, NetherNetID: "primary-nethernet", PmsgID: uuid.New()},
		announcer:         primaryAnnouncer,
		started:           true,
		conf: Config{
			XBLClient: &xsapi.Client{},
			XUID:      "same",
			Status:    Status{HostName: "Host", WorldName: "World"},
			SubAccounts: []SubAccountConfig{{
				ID:        "sub1",
				Enabled:   true,
				XBLClient: &xsapi.Client{},
				XUID:      "same",
			}},
		},
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "same",
			announcer: oldAnnouncer,
			signaling: lost,
			listener:  oldListener,
		}},
		subAnnouncersByID: map[string]room.Announcer{"sub1": oldAnnouncer},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			return newAnnouncer, nil
		},
		subAccountSignalingFactory: func(context.Context, *SubAccountConfig) (nethernet.Signaling, error) {
			return replacement, nil
		},
		subAccountConnectionFactory: func(context.Context, *SubAccountConfig, nethernet.Signaling) (*room.Connection, error) {
			return &connection, nil
		},
		subAccountListenerFactory: func(nethernet.Signaling, room.Status) (net.Listener, error) {
			return newListener, nil
		},
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()

	if !b.checkSessionHealth() {
		t.Fatal("checkSessionHealth() = false, want lost sub-account signaling recovery")
	}

	deadline := time.Now().Add(time.Second)
	for {
		b.mu.Lock()
		replaced := len(b.subAnnouncers) == 1 && b.subAnnouncers[0].signaling == replacement && b.subAnnouncers[0].listener == newListener
		b.mu.Unlock()
		if replaced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for targeted sub-account recovery")
		}
		time.Sleep(time.Millisecond)
	}

	if !oldAnnouncer.Closed() || !oldListener.Closed() {
		t.Fatalf("lost sub-account resources remain open: announcer=%v listener=%v", oldAnnouncer.Closed(), oldListener.Closed())
	}
	if primaryAnnouncer.Closed() || primary.closed {
		t.Fatal("targeted sub-account recovery restarted the primary endpoint")
	}
	if len(b.subAnnouncers) != 1 || b.subAnnouncers[0].signaling != replacement || b.subAnnouncers[0].listener != newListener {
		t.Fatalf("sub-account endpoint was not replaced: %#v", b.subAnnouncers)
	}
	if b.subAnnouncersByID["sub1"] == nil || b.subAnnouncersByID["sub1"] == oldAnnouncer {
		t.Fatal("sub-account announcer lookup was not replaced")
	}
	b.reconnectWg.Wait()

	if err := b.cleanupPublishedSessions(false); err != nil {
		t.Fatal(err)
	}
	b.acceptWg.Wait()
}

func TestBroadcasterRevalidatesPreparedSubAccountAgainstCurrentPrimary(t *testing.T) {
	lostCtx, lose := context.WithCancel(context.Background())
	lose()
	oldPrimary := &fakeSignaling{networkID: "old-primary"}
	newPrimary := &fakeSignaling{networkID: "new-primary"}
	lost := &cancelableSignaling{ctx: lostCtx, networkID: "lost-sub"}
	factoryStarted := make(chan struct{})
	releaseFactory := make(chan struct{})
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		signaling: oldPrimary,
		started:   true,
		conf: Config{
			XBLClient: &xsapi.Client{},
			XUID:      "same",
			Status:    Status{HostName: "Host", WorldName: "World"},
			SubAccounts: []SubAccountConfig{{
				ID:        "sub1",
				Enabled:   true,
				XBLClient: &xsapi.Client{},
				XUID:      "same",
			}},
		},
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "same",
			announcer: &fakeAnnouncer{},
			signaling: lost,
			listener:  newTrackedListener(),
		}},
		subAnnouncersByID: map[string]room.Announcer{"sub1": &fakeAnnouncer{}},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			return &fakeAnnouncer{}, nil
		},
		subAccountSignalingFactory: func(context.Context, *SubAccountConfig) (nethernet.Signaling, error) {
			close(factoryStarted)
			<-releaseFactory
			return newPrimary, nil
		},
		subAccountConnectionFactory: func(context.Context, *SubAccountConfig, nethernet.Signaling) (*room.Connection, error) {
			return &room.Connection{ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC, NetherNetID: "new-primary", PmsgID: uuid.New()}, nil
		},
		subAccountListenerFactory: func(nethernet.Signaling, room.Status) (net.Listener, error) {
			return newTrackedListener(), nil
		},
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()

	result := make(chan error, 1)
	go func() {
		result <- b.recreateSubAccount("sub1", lost)
	}()
	select {
	case <-factoryStarted:
	case <-time.After(time.Second):
		t.Fatal("sub-account signaling preparation did not start")
	}
	b.mu.Lock()
	b.signaling = newPrimary
	b.mu.Unlock()
	close(releaseFactory)

	err := <-result
	if err == nil || !strings.Contains(err.Error(), "matches the current primary") {
		t.Fatalf("recreateSubAccount() error = %v, want current-primary rejection", err)
	}
	if newPrimary.closed {
		t.Fatal("rejecting the now-primary signaling closed the primary endpoint")
	}
}

func TestBroadcasterSubAccountReconnectDoesNotBlockHealthLoopOrMutex(t *testing.T) {
	lostCtx, lose := context.WithCancel(context.Background())
	lose()
	reconnectStarted := make(chan struct{})
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		signaling: &fakeSignaling{networkID: "primary-nethernet"},
		started:   true,
		conf: Config{
			XUID: "same",
			SubAccounts: []SubAccountConfig{{
				ID:        "sub1",
				Enabled:   true,
				XBLClient: &xsapi.Client{},
				XUID:      "same",
			}},
		},
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			signaling: &cancelableSignaling{ctx: lostCtx, networkID: "lost-sub-nethernet"},
			announcer: &fakeAnnouncer{},
			listener:  newTrackedListener(),
		}},
		subAccountSignalingFactory: func(ctx context.Context, _ *SubAccountConfig) (nethernet.Signaling, error) {
			close(reconnectStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())

	returned := make(chan bool, 1)
	go func() {
		returned <- b.checkSessionHealth()
	}()
	select {
	case unhealthy := <-returned:
		if !unhealthy {
			t.Fatal("checkSessionHealth() = false, want recovery scheduled")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("checkSessionHealth blocked on sub-account reconnection")
	}
	select {
	case <-reconnectStarted:
	case <-time.After(time.Second):
		t.Fatal("sub-account reconnection did not start")
	}

	lockAcquired := make(chan struct{})
	go func() {
		b.mu.Lock()
		b.mu.Unlock()
		close(lockAcquired)
	}()
	select {
	case <-lockAcquired:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("sub-account reconnection held the broadcaster mutex during network work")
	}

	b.cancel()
	b.reconnectWg.Wait()
}

func TestBroadcasterSkipsDuplicateIDsBeforePublishing(t *testing.T) {
	first := &fakeAnnouncer{}
	second := &fakeAnnouncer{}
	announcers := []room.Announcer{first, second}
	factoryCalls := 0
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			XBLClient: &xsapi.Client{},
			XUID:      "same",
			SubAccounts: []SubAccountConfig{
				{ID: "duplicate", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "same"},
				{ID: "duplicate", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "same"},
			},
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			factoryCalls++
			next := announcers[0]
			announcers = announcers[1:]
			return next, nil
		},
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	if err := b.cleanupPublishedSessions(false); err != nil {
		t.Fatal(err)
	}
	if factoryCalls != 1 {
		t.Fatalf("announcer factory calls = %d, want 1", factoryCalls)
	}
	if !first.Closed() {
		t.Fatal("published session was not closed")
	}
	if second.Closed() {
		t.Fatal("duplicate-ID session was published and retained")
	}
}

type trackedListener struct {
	mu     sync.Mutex
	closed bool
	done   chan struct{}
}

func newTrackedListener() *trackedListener {
	return &trackedListener{done: make(chan struct{})}
}

func (l *trackedListener) Accept() (net.Conn, error) {
	<-l.done
	return nil, net.ErrClosed
}

func (l *trackedListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		close(l.done)
	}
	l.closed = true
	return nil
}

func (l *trackedListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func (l *trackedListener) Closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}
