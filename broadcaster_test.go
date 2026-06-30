package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestBroadcasterStartSubAccountsMutuallyFollowsBeforePublish(t *testing.T) {
	var calls []string
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, fmt.Sprintf("%s %s auth=%s", req.Method, req.URL.String(), req.Header.Get("Authorization")))
		return broadcasterResponse(http.StatusNoContent, ""), nil
	})}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient:  &xsapi.Client{},
		XUID:       "100",
		HTTPClient: client,
		SubAccounts: []SubAccountConfig{{
			ID:        "sub",
			Enabled:   true,
			XBLClient: &xsapi.Client{},
			XUID:      "200",
		}},
	}}
	b.subAccountPublisher = func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error) {
		calls = append(calls, "publish")
		return &mpsd.Session{}, nil
	}

	if err := b.startSubAccounts(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"PUT https://social.xboxlive.com/users/me/people/xuid(200) auth=",
		"PUT https://social.xboxlive.com/users/me/people/xuid(100) auth=",
		"publish",
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("unexpected call order\n got: %v\nwant: %v", calls, want)
	}
}

func TestBroadcasterStartSubAccountsSkipsMutualFollowWithoutXUIDs(t *testing.T) {
	var httpCalls, publishCalls int
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient: &xsapi.Client{},
		HTTPClient: &http.Client{Transport: broadcasterRoundTripFunc(func(*http.Request) (*http.Response, error) {
			httpCalls++
			return broadcasterResponse(http.StatusNoContent, ""), nil
		})},
		SubAccounts: []SubAccountConfig{{
			ID:        "sub",
			Enabled:   true,
			XBLClient: &xsapi.Client{},
			XUID:      "200",
		}},
	}}
	b.subAccountPublisher = func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error) {
		publishCalls++
		return &mpsd.Session{}, nil
	}

	if err := b.startSubAccounts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if httpCalls != 0 {
		t.Fatalf("expected no follow requests without both XUIDs, got %d", httpCalls)
	}
	if publishCalls != 1 {
		t.Fatalf("expected sub-account publish to continue, got %d calls", publishCalls)
	}
}

func TestBroadcasterStartSubAccountsSkipsEnabledAccountWithoutCredentials(t *testing.T) {
	var httpCalls, publishCalls int
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient: &xsapi.Client{},
		XUID:      "100",
		HTTPClient: &http.Client{Transport: broadcasterRoundTripFunc(func(*http.Request) (*http.Response, error) {
			httpCalls++
			return broadcasterResponse(http.StatusNoContent, ""), nil
		})},
		SubAccounts: []SubAccountConfig{{
			ID:      "missing",
			Enabled: true,
		}},
	}}
	b.subAccountPublisher = func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error) {
		publishCalls++
		return &mpsd.Session{}, nil
	}

	if err := b.startSubAccounts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if httpCalls != 0 {
		t.Fatalf("expected no follow requests for uncredentialed sub-account, got %d", httpCalls)
	}
	if publishCalls != 0 {
		t.Fatalf("expected no publish for uncredentialed sub-account, got %d calls", publishCalls)
	}
}

func TestBroadcasterClearCreatedXBLClientReferences(t *testing.T) {
	primary := &xsapi.Client{}
	createdSub := &xsapi.Client{}
	externalSub := &xsapi.Client{}
	tokens := staticMinecraftTokenSource{}
	b := &Broadcaster{
		xblClient:         primary,
		minecraftTokens:   tokens,
		createdXBLClients: []*xsapi.Client{primary, createdSub},
		conf: Config{
			XBLClient:            primary,
			MinecraftTokenSource: tokens,
			SubAccounts: []SubAccountConfig{
				{ID: "created", XBLClient: createdSub},
				{ID: "external", XBLClient: externalSub},
			},
		},
	}

	b.clearCreatedXBLClientReferences(createdXBLClientSet(b.createdXBLClients))

	if b.xblClient != nil {
		t.Fatal("created primary client cache was not cleared")
	}
	if b.conf.XBLClient != nil {
		t.Fatal("created primary config client was not cleared")
	}
	if b.minecraftTokens != nil || b.conf.MinecraftTokenSource != nil {
		t.Fatal("minecraft token source derived from created primary client was not cleared")
	}
	if b.conf.SubAccounts[0].XBLClient != nil {
		t.Fatal("created sub-account client was not cleared")
	}
	if b.conf.SubAccounts[1].XBLClient != externalSub {
		t.Fatal("external sub-account client should not be cleared")
	}
}

func TestXBLAnnouncerUnwrapsDiagnosticsWrappers(t *testing.T) {
	inner := &room.XBLAnnouncer{}
	wrapped := signalingConnectionAnnouncer{
		Announcer: loggingAnnouncer{Announcer: inner},
		connection: room.Connection{
			ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
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

func TestBroadcasterWarnsForWebSocketSignaling(t *testing.T) {
	var log bytes.Buffer
	b := &Broadcaster{log: slog.New(slog.NewTextHandler(&log, nil))}
	b.warnWebSocketSignalingMode(SignalingModeWebSocket)
	got := log.String()
	if !strings.Contains(got, "websocket signaling may not appear in Minecraft friends list") {
		t.Fatalf("warning missing from log: %q", got)
	}
	if !strings.Contains(got, "recommended_signaling_mode=jsonrpc") {
		t.Fatalf("recommended mode missing from log: %q", got)
	}
	log.Reset()
	b.warnWebSocketSignalingMode(SignalingModeJSONRPC)
	if log.Len() != 0 {
		t.Fatalf("unexpected warning for jsonrpc mode: %q", log.String())
	}
}

func TestBroadcasterUpdateLogsUpdatedSession(t *testing.T) {
	var log bytes.Buffer
	b := &Broadcaster{
		log:       slog.New(slog.NewTextHandler(&log, nil)),
		announcer: &fakeAnnouncer{},
		started:   true,
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:   "123",
			Status: Status{HostName: "Host", WorldName: "World"},
		},
	}

	if err := b.Update(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := log.String(); !strings.Contains(got, "updated session") {
		t.Fatalf("updated session log missing: %q", got)
	}
}

func TestBroadcasterTransferLogsBedrockClientAndTarget(t *testing.T) {
	var log bytes.Buffer
	conn := &recordingTransferConn{}
	b := &Broadcaster{
		log: slog.New(slog.NewTextHandler(&log, nil)),
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19133},
		},
		transferCloseTimeout: -1,
	}

	b.transfer(conn)

	got := log.String()
	if !strings.Contains(got, "transferred bedrock client") {
		t.Fatalf("transfer log missing: %q", got)
	}
	if !strings.Contains(got, "xuid=visitor") || !strings.Contains(got, "name=Visitor") || !strings.Contains(got, "target=play.example.net:19133") {
		t.Fatalf("transfer log missing client or target fields: %q", got)
	}
}

func TestBroadcasterSignalingFactoryIsUsedOnceForSharedSignaling(t *testing.T) {
	var calls int
	sig := &fakeSignaling{networkID: "123456789"}
	b := &Broadcaster{
		conf: Config{
			Server:               ServerInfo{Host: "127.0.0.1", Port: 19132},
			XUID:                 "123",
			SignalingMode:        SignalingModeJSONRPC,
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

func TestBroadcasterWatchSignalingDetectsDeadSignaling(t *testing.T) {
	sigCtx, sigCancel := context.WithCancel(context.Background())
	defer sigCancel()
	var log bytes.Buffer
	b := &Broadcaster{
		log:       slog.New(slog.NewTextHandler(&log, nil)),
		signaling: &cancelableSignaling{ctx: sigCtx, networkID: "12345"},
		started:   true,
		conf: Config{
			Server: ServerInfo{Host: "127.0.0.1", Port: 19132},
			XUID:   "123",
			Status: Status{HostName: "Host", WorldName: "World"},
			SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				return nil, errors.New("test: signaling factory error")
			},
		},
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()

	sigCancel()
	done := make(chan struct{})
	go func() {
		b.watchSignaling()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchSignaling did not return after signaling context was canceled")
	}
	got := log.String()
	if !strings.Contains(got, "connection to signaling lost") {
		t.Fatalf("expected signaling lost warning, got: %q", got)
	}
	if !strings.Contains(got, "re-create session failed") {
		t.Fatalf("expected re-create error log, got: %q", got)
	}
}

func TestBroadcasterWatchSignalingSkipsStaticSignaling(t *testing.T) {
	sigCtx, sigCancel := context.WithCancel(context.Background())
	defer sigCancel()
	var log bytes.Buffer
	staticSig := &cancelableSignaling{ctx: sigCtx, networkID: "12345"}
	b := &Broadcaster{
		log:       slog.New(slog.NewTextHandler(&log, nil)),
		signaling: staticSig,
		started:   true,
		conf: Config{
			Signaling: staticSig,
		},
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()

	done := make(chan struct{})
	go func() {
		b.watchSignaling()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchSignaling should have returned immediately for static signaling")
	}
}

type cancelableSignaling struct {
	ctx       context.Context
	networkID string
}

func (s *cancelableSignaling) Signal(context.Context, *nethernet.Signal) error { return nil }
func (s *cancelableSignaling) Notify(nethernet.Notifier) func() {
	return func() {}
}
func (s *cancelableSignaling) Context() context.Context { return s.ctx }
func (s *cancelableSignaling) Credentials(context.Context) (*nethernet.Credentials, error) {
	return nil, nil
}
func (s *cancelableSignaling) NetworkID() string { return s.networkID }
func (s *cancelableSignaling) PongData([]byte)   {}

func TestBroadcasterUsesLongerDefaultNetherNetTransportTimeout(t *testing.T) {
	b := &Broadcaster{}
	conf := b.netherNetListenConfig()
	if conf.ConnContext == nil {
		t.Fatal("default nethernet ConnContext missing")
	}
	ctx, cancel := conf.ConnContext(context.Background(), nil)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("default nethernet ConnContext has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining < defaultNetherNetConnTimeout-time.Second || remaining > defaultNetherNetConnTimeout {
		t.Fatalf("default nethernet ConnContext timeout = %s, want about %s", remaining, defaultNetherNetConnTimeout)
	}
	if !b.usesDefaultNetherNetConnContext() {
		t.Fatal("default nethernet ConnContext should be reported as default")
	}
	if got := b.netherNetTransportTimeoutLogValue(); got != defaultNetherNetConnTimeout.String() {
		t.Fatalf("transport timeout log value = %q, want %q", got, defaultNetherNetConnTimeout.String())
	}
}

func TestBroadcasterPreservesCustomNetherNetTransportContext(t *testing.T) {
	type contextKey struct{}
	want := context.WithValue(context.Background(), contextKey{}, "custom")
	called := false
	b := &Broadcaster{conf: Config{NetherNetListenConfig: nethernet.ListenConfig{
		ConnContext: func(ctx context.Context, _ *nethernet.Conn) (context.Context, context.CancelFunc) {
			called = true
			return want, func() {}
		},
	}}}

	conf := b.netherNetListenConfig()
	got, cancel := conf.ConnContext(context.Background(), nil)
	defer cancel()
	if !called {
		t.Fatal("custom nethernet ConnContext was not called")
	}
	if got != want {
		t.Fatal("custom nethernet ConnContext was not preserved")
	}
	if b.usesDefaultNetherNetConnContext() {
		t.Fatal("custom nethernet ConnContext should not be reported as default")
	}
	if got := b.netherNetTransportTimeoutLogValue(); got != "custom" {
		t.Fatalf("transport timeout log value = %q, want custom", got)
	}
}

func TestBroadcasterTransferSendsStartGameBeforeTransfer(t *testing.T) {
	conn := &recordingTransferConn{}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
		Status: Status{WorldName: "Redirect Lobby"},
	}, transferCloseTimeout: -1}

	b.transfer(conn)

	startGameIndex, transferIndex := -1, -1
	for i, pk := range conn.packets {
		switch pk.(type) {
		case *packet.StartGame:
			if startGameIndex == -1 {
				startGameIndex = i
			}
		case *packet.Transfer:
			transferIndex = i
		}
	}
	if startGameIndex == -1 {
		t.Fatal("StartGame was not sent")
	}
	if transferIndex == -1 {
		t.Fatal("Transfer was not sent")
	}
	if startGameIndex > transferIndex {
		t.Fatalf("StartGame sent after Transfer: startGame=%d transfer=%d", startGameIndex, transferIndex)
	}
	transfer := conn.packets[transferIndex].(*packet.Transfer)
	if transfer.Address != "play.example.net" || transfer.Port != 19133 {
		t.Fatalf("unexpected transfer target %#v", transfer)
	}
	if conn.flushes != 1 {
		t.Fatalf("expected one flush, got %d", conn.flushes)
	}
	if !conn.closed {
		t.Fatal("connection was not closed")
	}
}

func TestBroadcasterTransferWaitsForClientDisconnectAfterFlush(t *testing.T) {
	conn := &recordingTransferConn{
		readErrCh:     make(chan error, 1),
		readStartedCh: make(chan struct{}),
		closedCh:      make(chan struct{}),
	}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
	}}

	go b.transfer(conn)

	select {
	case <-conn.readStartedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("transfer did not wait for client disconnect")
	}
	select {
	case <-conn.closedCh:
		t.Fatal("connection closed before client disconnect")
	default:
	}

	conn.readErrCh <- net.ErrClosed
	select {
	case <-conn.closedCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("connection was not closed after client disconnect")
	}
}

func TestBroadcasterTransferClosesAfterDisconnectTimeout(t *testing.T) {
	timeout := 20 * time.Millisecond
	conn := &recordingTransferConn{
		readErrCh:        make(chan error, 1),
		deadlineTriggers: true,
		closedCh:         make(chan struct{}),
	}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
	}, transferCloseTimeout: timeout}

	go b.transfer(conn)

	select {
	case <-conn.closedCh:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("connection was not closed after transfer disconnect timeout")
	}
}

func TestBroadcasterTransferDoesNotWaitWhenFlushFails(t *testing.T) {
	conn := &recordingTransferConn{
		flushErr: fmt.Errorf("flush failed"),
		closedCh: make(chan struct{}),
	}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
	}, transferCloseTimeout: time.Second}

	b.transfer(conn)

	if !conn.closed {
		t.Fatal("connection was not closed after flush failed")
	}
	if conn.readStarted() {
		t.Fatal("transfer waited for client disconnect after flush failed")
	}
}

type broadcasterRoundTripFunc func(*http.Request) (*http.Response, error)

func (f broadcasterRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func broadcasterResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func testBroadcasterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recordingTransferConn struct {
	packets          []packet.Packet
	flushErr         error
	flushes          int
	closed           bool
	closedCh         chan struct{}
	readErrCh        chan error
	readStartedCh    chan struct{}
	readStartedOnce  sync.Once
	readStartedValue bool
	deadlineTriggers bool
}

func (c *recordingTransferConn) WritePacket(pk packet.Packet) error {
	c.packets = append(c.packets, pk)
	return nil
}

func (c *recordingTransferConn) ReadPacket() (packet.Packet, error) {
	c.readStartedOnce.Do(func() {
		c.readStartedValue = true
		if c.readStartedCh != nil {
			close(c.readStartedCh)
		}
	})
	if c.readErrCh == nil {
		return nil, net.ErrClosed
	}
	return nil, <-c.readErrCh
}

func (c *recordingTransferConn) Flush() error {
	c.flushes++
	return c.flushErr
}

func (c *recordingTransferConn) Close() error {
	if !c.closed && c.closedCh != nil {
		close(c.closedCh)
	}
	c.closed = true
	return nil
}

func (c *recordingTransferConn) SetReadDeadline(t time.Time) error {
	if c.deadlineTriggers && c.readErrCh != nil && !t.IsZero() {
		delay := time.Until(t)
		if delay < 0 {
			delay = 0
		}
		time.AfterFunc(delay, func() {
			select {
			case c.readErrCh <- context.DeadlineExceeded:
			default:
			}
		})
	}
	return nil
}

func (c *recordingTransferConn) IdentityData() login.IdentityData {
	return login.IdentityData{XUID: "visitor", DisplayName: "Visitor"}
}

func (c *recordingTransferConn) readStarted() bool {
	return c.readStartedValue
}
