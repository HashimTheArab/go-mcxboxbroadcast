package broadcaster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/df-mc/go-xsapi/v2/xal/xasd"
	"github.com/df-mc/go-xsapi/v2/xal/xasu"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

// broadcasterTokenSource supplies deterministic credentials to test clients.
type broadcasterTokenSource struct {
	xuid string
	key  *ecdsa.PrivateKey
}

// XSTSToken returns the account identity used by the test transport.
func (s broadcasterTokenSource) XSTSToken(context.Context, string) (*xsts.Token, error) {
	return &xsts.Token{
		Token: "token",
		DisplayClaims: xsts.DisplayClaims{UserInfo: []xsts.UserInfo{{
			UserInfo: xasu.UserInfo{UserHash: "uhs"},
			XUID:     s.xuid,
		}}},
	}, nil
}

// DeviceToken rejects unexpected device authentication during a test.
func (broadcasterTokenSource) DeviceToken(context.Context) (*xasd.Token, error) {
	return nil, errors.New("unexpected device token request")
}

// ProofKey returns the key used to sign test requests.
func (s broadcasterTokenSource) ProofKey() *ecdsa.PrivateKey {
	return s.key
}

// newTestXSAPIClient constructs real API subclients with a stubbed title configuration.
func newTestXSAPIClient(t *testing.T, client *http.Client, xuid string) *xsapi.Client {
	t.Helper()
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	configured := *client
	configured.Transport = broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "title.mgt.xboxlive.com" {
			return broadcasterResponse(http.StatusOK, `{"EndPoints":[{"Protocol":"https","Host":"peoplehub.xboxlive.com","HostType":"fqdn","RelyingParty":"http://xboxlive.com","TokenType":"JWT"},{"Protocol":"https","Host":"social.xboxlive.com","HostType":"fqdn","RelyingParty":"http://xboxlive.com","TokenType":"JWT"},{"Protocol":"https","Host":"userpresence.xboxlive.com","HostType":"fqdn","RelyingParty":"http://xboxlive.com","TokenType":"JWT"}]}`), nil
		}
		return base.RoundTrip(req)
	})
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate proof key: %v", err)
	}
	xbl, err := (xsapi.ClientConfig{HTTPClient: &configured, RTAMode: xsapi.RTADisabled}).New(t.Context(), broadcasterTokenSource{xuid: xuid, key: key})
	if err != nil {
		t.Fatalf("create xsapi client: %v", err)
	}
	return xbl
}

func TestSubAccountFriendshipSkipsMissingSocialClients(t *testing.T) {
	xbl := newTestXSAPIClient(t, &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected friendship request: %s %s", req.Method, req.URL)
		return nil, errors.New("unexpected request")
	})}, "100")
	for _, tc := range []struct {
		name    string
		primary *xsapi.Client
		sub     *xsapi.Client
	}{
		{name: "nil primary", sub: xbl},
		{name: "nil sub", primary: xbl},
		{name: "missing primary social", primary: &xsapi.Client{}, sub: xbl},
		{name: "missing sub social", primary: xbl, sub: &xsapi.Client{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &Broadcaster{conf: Config{XBLClient: tc.primary, XUID: "100"}}
			if err := b.ensureSubAccountFriendship(t.Context(), SubAccountConfig{XBLClient: tc.sub, XUID: "200"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBroadcasterStartSubAccountsBefriendsPrimaryBeforePublish(t *testing.T) {
	var calls []string
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, fmt.Sprintf("%s %s auth=%s", req.Method, req.URL.String(), req.Header.Get("Authorization")))
		if req.Method == http.MethodGet {
			return broadcasterResponse(http.StatusNoContent, ""), nil
		}
		return broadcasterResponse(http.StatusOK, ""), nil
	})}
	primary := newTestXSAPIClient(t, client, "100")
	sub := newTestXSAPIClient(t, client, "200")
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient:  primary,
		XUID:       "100",
		HTTPClient: client,
		SubAccounts: []SubAccountConfig{{
			ID:        "sub",
			Enabled:   true,
			XBLClient: sub,
			XUID:      "200",
		}},
	}}
	b.subAccountAnnouncerFactory = func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
		calls = append(calls, "publish")
		return &fakeAnnouncer{}, nil
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	want := []string{
		// The friend list is checked first so restarts do not re-PUT an
		// existing friendship; the 204 here fails decoding, so both adds
		// proceed: the sub-account requests, then the primary accepts.
		"GET https://peoplehub.xboxlive.com/users/me/people/friends auth=XBL3.0 x=uhs;token",
		"PUT https://social.xboxlive.com/users/me/people/friends/v2/xuid(100) auth=XBL3.0 x=uhs;token",
		"PUT https://social.xboxlive.com/users/me/people/friends/v2/xuid(200) auth=XBL3.0 x=uhs;token",
		"publish",
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("unexpected call order\n got: %v\nwant: %v", calls, want)
	}
}

func TestBroadcasterStartSubAccountsSkipsExistingFriendship(t *testing.T) {
	var calls []string
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.Path)
		switch req.URL.Host {
		case "peoplehub.xboxlive.com":
			return broadcasterResponse(http.StatusOK, `{"people":[{"xuid":"100","isFriend":true}]}`), nil
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			return nil, nil
		}
	})}
	primary := newTestXSAPIClient(t, client, "100")
	sub := newTestXSAPIClient(t, client, "200")
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient:  primary,
		XUID:       "100",
		HTTPClient: client,
		SubAccounts: []SubAccountConfig{{
			ID:        "sub",
			Enabled:   true,
			XBLClient: sub,
			XUID:      "200",
		}},
	}}
	b.subAccountAnnouncerFactory = func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
		calls = append(calls, "publish")
		return &fakeAnnouncer{}, nil
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		if strings.HasPrefix(call, "PUT ") {
			t.Fatalf("existing friendship should not be re-PUT, got calls %v", calls)
		}
	}
	if calls[len(calls)-1] != "publish" {
		t.Fatalf("expected publish after friendship check, got %v", calls)
	}
}

func TestBroadcasterStartSubAccountsStopsQuietlyOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var notified []string
	var started []string
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient: &xsapi.Client{},
		XUID:      "100",
		Notifier: fakeNotifier{notify: func(_ context.Context, message string) {
			notified = append(notified, message)
		}},
		SubAccounts: []SubAccountConfig{
			{ID: "first", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "100"},
			{ID: "second", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "100"},
		},
	}}
	b.subAccountAnnouncerFactory = func(_ context.Context, account SubAccountConfig, _ mpsd.SessionReference) (room.Announcer, error) {
		started = append(started, account.ID)
		cancel()
		return nil, ctx.Err()
	}

	err := b.startSubAccounts(ctx, room.Status{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startSubAccounts() error = %v, want context.Canceled", err)
	}
	if fmt.Sprint(started) != "[first]" {
		t.Fatalf("started = %v, want [first] (stop after cancellation)", started)
	}
	if len(notified) != 0 {
		t.Fatalf("notifications = %v, want none during shutdown", notified)
	}
}

func TestBroadcasterPrimarySyncerDisablesPruningWithSubAccountSyncers(t *testing.T) {
	conf := Config{
		XBLClient:  &xsapi.Client{},
		XUID:       "100",
		FriendSync: &FriendSyncConfig{AutoFriend: true, ExpiryEnabled: true},
	}

	solo := &Broadcaster{log: testBroadcasterLogger(), conf: conf}
	if !solo.friendSyncer().PruneHistory {
		t.Fatal("primary syncer should prune when it owns the history store alone")
	}

	conf.SubAccounts = []SubAccountConfig{{ID: "sub", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "200"}}
	shared := &Broadcaster{log: testBroadcasterLogger(), conf: conf}
	if shared.friendSyncer().PruneHistory {
		t.Fatal("primary syncer must not prune a history store shared with sub-account syncers")
	}
}

func TestBroadcasterStartSubAccountsContinuesPastFailingAccount(t *testing.T) {
	var published []string
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient: &xsapi.Client{},
		XUID:      "100",
		SubAccounts: []SubAccountConfig{
			{ID: "bad", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "100"},
			{ID: "good", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "100"},
		},
	}}
	b.subAccountAnnouncerFactory = func(_ context.Context, account SubAccountConfig, _ mpsd.SessionReference) (room.Announcer, error) {
		if account.ID == "bad" {
			return nil, errors.New("boom")
		}
		published = append(published, account.ID)
		return &fakeAnnouncer{}, nil
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatalf("startSubAccounts() error = %v, want nil (bad account skipped)", err)
	}
	if fmt.Sprint(published) != "[good]" {
		t.Fatalf("published = %v, want [good]", published)
	}
}

func TestBroadcasterStartSubAccountsSkipsFriendshipWithoutXUIDs(t *testing.T) {
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
	b.subAccountAnnouncerFactory = func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
		publishCalls++
		return &fakeAnnouncer{}, nil
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	if httpCalls != 0 {
		t.Fatalf("expected no friend requests without both XUIDs, got %d", httpCalls)
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
	b.subAccountAnnouncerFactory = func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
		publishCalls++
		return &fakeAnnouncer{}, nil
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	if httpCalls != 0 {
		t.Fatalf("expected no friend requests for uncredentialed sub-account, got %d", httpCalls)
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
		connection: p2p.Connection{
			Type: p2p.ConnectionTypeSignalingOverWebSocket,
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
	// Keep the public API Minecraft-specific: callers provide only the XUID,
	// while Broadcaster supplies the package's title ID.
	var invite func(*Broadcaster, context.Context, string) error = (*Broadcaster).Invite

	b := &Broadcaster{
		announcer: &room.XBLAnnouncer{Session: &mpsd.Session{}},
	}
	err := invite(b, context.Background(), "456")
	if err == nil || !strings.Contains(err.Error(), "broadcaster not started") {
		t.Fatalf("Invite error = %v, want broadcaster-not-started error", err)
	}
}

func TestBroadcasterBuildsWebSocketSignalingConnection(t *testing.T) {
	b := &Broadcaster{}
	connection, err := b.signalingConnection(&fakeSignaling{networkID: "123456789"})
	if err != nil {
		t.Fatal(err)
	}
	if connection == nil {
		t.Fatal("websocket signaling connection is nil")
	}
	if connection.Type != p2p.ConnectionTypeSignalingOverWebSocket {
		t.Fatalf("connection type = %d, want websocket", connection.Type)
	}
	if connection.NetherNetID != "123456789" {
		t.Fatalf("nethernet id = %q, want shared signaling id", connection.NetherNetID)
	}
	if connection.PlayerMessagingID != uuid.Nil {
		t.Fatalf("pmsg id = %s, want nil for websocket signaling", connection.PlayerMessagingID)
	}
}

func TestNewRejectsJSONRPCWithEnabledSubAccounts(t *testing.T) {
	_, err := New(Config{
		JoinURL:       "http://127.0.0.1:19132",
		SignalingMode: SignalingModeJSONRPC,
		SubAccounts: []SubAccountConfig{{
			ID:      "sub",
			Enabled: true,
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "jsonrpc signaling does not support sub-accounts") {
		t.Fatalf("New() error = %v, want JSON-RPC sub-account rejection", err)
	}
}

func TestNewAllowsWebSocketWithEnabledSubAccounts(t *testing.T) {
	if _, err := New(Config{
		JoinURL:        "http://127.0.0.1:19132",
		XBLTokenSource: staticTokenSource{},
		SignalingMode:  SignalingModeWebSocket,
		SubAccounts: []SubAccountConfig{{
			ID:      "sub",
			Enabled: true,
		}},
	}); err != nil {
		t.Fatalf("New() error = %v, want WebSocket sub-accounts accepted", err)
	}
}

func TestBroadcasterBuildsJSONRPCSignalingConnection(t *testing.T) {
	pmid := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	b := &Broadcaster{conf: Config{SignalingMode: SignalingModeJSONRPC}}
	connection, err := b.signalingConnection(&jsonRPCFakeSignaling{
		fakeSignaling: fakeSignaling{networkID: "123456789"},
		pmid:          pmid,
	})
	if err != nil {
		t.Fatal(err)
	}
	if connection.Type != p2p.ConnectionTypeSignalingOverJSONRPC || connection.NetherNetID != "123456789" || connection.PlayerMessagingID != pmid {
		t.Fatalf("JSON-RPC connection = %#v", connection)
	}
}

func TestBroadcasterRejectsInvalidWebSocketNetworkID(t *testing.T) {
	t.Parallel()

	for _, networkID := range []string{"0", "01"} {
		networkID := networkID
		t.Run(networkID, func(t *testing.T) {
			t.Parallel()
			b := &Broadcaster{}
			if _, err := b.signalingConnection(&fakeSignaling{networkID: networkID}); err == nil {
				t.Fatalf("signalingConnection() accepted network ID %q", networkID)
			}
		})
	}
}

func TestBroadcasterUpdateLogsUpdatedSession(t *testing.T) {
	var log bytes.Buffer
	b := &Broadcaster{
		log:       slog.New(slog.NewTextHandler(&log, nil)),
		announcer: &fakeAnnouncer{},
		started:   true,
		conf: Config{
			JoinURL: "http://127.0.0.1:19132",
			XUID:    "123",
			Status:  Status{HostName: "Host", WorldName: "World"},
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
		conf: Config{
			JoinURL:              "http://127.0.0.1:19132",
			XUID:                 "123",
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

func TestSubAccountInviterRequiresPublishedSession(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		JoinURL:        "http://127.0.0.1:19132",
	})
	if err != nil {
		t.Fatal(err)
	}
	inviter := &subAccountInviter{b: b, id: "sub1"}
	if err := inviter.Invite(context.Background(), "456", "1739947436"); err == nil {
		t.Fatal("expected error inviting before the sub-account published a session")
	}
}

func TestStartSubAccountsTimeoutDoesNotBlockStartup(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		JoinURL:        "http://127.0.0.1:19132",
		SubAccounts: []SubAccountConfig{{
			ID:             "sub1",
			Enabled:        true,
			XBLTokenSource: staticTokenSource{},
			XUID:           "456",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	defer b.cancel()
	b.subAccountStartTimeout = 50 * time.Millisecond
	b.subAccountAnnouncerFactory = func(ctx context.Context, _ SubAccountConfig, _ mpsd.SessionReference) (room.Announcer, error) {
		<-ctx.Done() // a hung publish must be bounded by the per-account timeout
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() {
		// Start holds b.mu for the whole startup sequence; mirror that so
		// re-locking inside the sub-account path deadlocks the test too.
		b.mu.Lock()
		defer b.mu.Unlock()
		done <- b.startSubAccounts(b.ctx, room.Status{})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("hung sub-account should be skipped, not fail startup: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startSubAccounts blocked on a hung sub-account publish")
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
						XUID:            "123",
						Status:          Status{HostName: "Host"},
						HideOldSessions: hide,
					},
					announcerFactory: func(*Broadcaster) room.Announcer { return next },
				}
				b.ctx, b.cancel = context.WithCancel(t.Context())
				defer b.cancel()
				b.sessionConnection = &p2p.Connection{Type: p2p.ConnectionTypeSignalingOverWebSocket, NetherNetID: "123456789"}
				b.announcer = b.wrapAnnouncer(old, b.log)

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
				if err := b.cleanupPublishedSessions(true); err != nil {
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
			b.retire(b.wrapAnnouncer(a, b.log), b.log)
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

// hidingAnnouncer records when a retired session is made invite-only.
type hidingAnnouncer struct {
	fakeAnnouncer
	hid     bool
	members int // negative means unpublished
}

func (a *hidingAnnouncer) memberCount() (int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.members, a.members >= 0
}

func (a *hidingAnnouncer) setMembers(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.members = n
}

func (a *hidingAnnouncer) hide(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hid = true
	return nil
}

func (a *hidingAnnouncer) hidden() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hid
}
