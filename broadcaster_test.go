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
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/df-mc/go-xsapi/v2/xal/xasd"
	"github.com/df-mc/go-xsapi/v2/xal/xasu"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
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

func TestMutualFollowSkipsMissingSocialClients(t *testing.T) {
	xbl := newTestXSAPIClient(t, &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected mutual-follow request: %s %s", req.Method, req.URL)
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
			if err := b.ensureSubAccountMutualFollow(t.Context(), SubAccountConfig{XBLClient: tc.sub, XUID: "200"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBroadcasterStartSubAccountsMutuallyFollowsBeforePublish(t *testing.T) {
	var calls []string
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, fmt.Sprintf("%s %s auth=%s", req.Method, req.URL.String(), req.Header.Get("Authorization")))
		return broadcasterResponse(http.StatusNoContent, ""), nil
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

	if _, err := b.startSubAccounts(context.Background(), room.Status{}, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{
		// The follow state is checked first so restarts do not re-PUT
		// existing friendships; the 204 here fails decoding, so both follows
		// proceed.
		"GET https://peoplehub.xboxlive.com/users/me/people/followers auth=XBL3.0 x=uhs;token",
		"PUT https://social.xboxlive.com/users/me/people/xuid(200) auth=XBL3.0 x=uhs;token",
		"PUT https://social.xboxlive.com/users/me/people/xuid(100) auth=XBL3.0 x=uhs;token",
		"publish",
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("unexpected call order\n got: %v\nwant: %v", calls, want)
	}
}

func TestBroadcasterStartSubAccountsSkipsExistingMutualFollow(t *testing.T) {
	var calls []string
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.Method+" "+req.URL.Path)
		switch req.URL.Host {
		case "peoplehub.xboxlive.com":
			return broadcasterResponse(http.StatusOK, `{"people":[{"xuid":"100","isFollowingCaller":true,"isFollowedByCaller":true}]}`), nil
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

	if _, err := b.startSubAccounts(context.Background(), room.Status{}, nil); err != nil {
		t.Fatal(err)
	}
	for _, call := range calls {
		if strings.HasPrefix(call, "PUT ") {
			t.Fatalf("existing mutual follow should not be re-PUT, got calls %v", calls)
		}
	}
	if calls[len(calls)-1] != "publish" {
		t.Fatalf("expected publish after follow check, got %v", calls)
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

	_, err := b.startSubAccounts(ctx, room.Status{}, nil)
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
		FriendSync: &FriendSyncConfig{AutoFollow: true, ExpiryEnabled: true},
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

	if _, err := b.startSubAccounts(context.Background(), room.Status{}, nil); err != nil {
		t.Fatalf("startSubAccounts() error = %v, want nil (bad account skipped)", err)
	}
	if fmt.Sprint(published) != "[good]" {
		t.Fatalf("published = %v, want [good]", published)
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
	b.subAccountAnnouncerFactory = func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
		publishCalls++
		return &fakeAnnouncer{}, nil
	}

	if _, err := b.startSubAccounts(context.Background(), room.Status{}, nil); err != nil {
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
	b.subAccountAnnouncerFactory = func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
		publishCalls++
		return &fakeAnnouncer{}, nil
	}

	if _, err := b.startSubAccounts(context.Background(), room.Status{}, nil); err != nil {
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
	b := &Broadcaster{
		xblClient:         primary,
		createdXBLClients: []*xsapi.Client{primary, createdSub},
		conf: Config{
			XBLClient: primary,
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
		Server:        ServerInfo{Host: "127.0.0.1", Port: 19132},
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
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
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

func TestMinecraftListenConfigEnablesPacketDiagnosticsInDebugMode(t *testing.T) {
	var log bytes.Buffer
	b := &Broadcaster{
		log: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
		conf: Config{
			Status: Status{HostName: "Host", WorldName: "World"},
		},
	}

	conf := b.minecraftListenConfig(room.Status{HostName: "Host", WorldName: "World"})
	if conf.PacketFunc == nil {
		t.Fatal("expected packet diagnostics in debug mode")
	}
	conf.PacketFunc(packet.Header{PacketID: packet.IDRequestNetworkSettings}, []byte{1, 2, 3}, nil, nil)

	got := log.String()
	if !strings.Contains(got, "minecraft packet") || !strings.Contains(got, "packet_id=193") {
		t.Fatalf("packet diagnostic log missing: %q", got)
	}
}

func TestMinecraftListenConfigKeepsFullLoginFlow(t *testing.T) {
	b := &Broadcaster{
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		conf: Config{
			Status: Status{HostName: "Host", WorldName: "World"},
		},
	}

	conf := b.minecraftListenConfig(room.Status{HostName: "Host", WorldName: "World"})
	if conf.DisablePacketHandling {
		t.Fatal("expected listener to wait for resource-pack login flow")
	}
	if conf.AuthenticationDisabled {
		t.Fatal("client authentication should be enabled by default so recorded XUIDs are verified")
	}
	if conf.CompressionThreshold != -1 {
		t.Fatalf("CompressionThreshold = %d, want -1 for Java-compatible threshold 0", conf.CompressionThreshold)
	}
	if !conf.ForceDisableVibrantVisuals {
		t.Fatal("expected listener to force-disable vibrant visuals")
	}
	if conf.ResourcePackWorldTemplateUUID != uuid.Nil || conf.ResourcePackWorldTemplateVersion != "" {
		t.Fatalf("unexpected resource-pack template metadata: uuid=%s version=%q, want zero UUID and empty version", conf.ResourcePackWorldTemplateUUID, conf.ResourcePackWorldTemplateVersion)
	}
}

func TestMinecraftListenConfigUsesCurrentProtocolOnly(t *testing.T) {
	b := &Broadcaster{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	conf := b.minecraftListenConfig(room.Status{})
	if len(conf.AcceptedProtocols) != 0 || conf.AcceptNewerProtocols {
		t.Fatal("default listener must only accept the current Minecraft protocol")
	}
}

func TestMinecraftListenConfigRejectsLegacyProtocols(t *testing.T) {
	for _, version := range []struct {
		name string
		id   int32
	}{
		{"1.26.40", 2168}, {"1.26.44", 2168}, {"1.26.45", 2169},
	} {
		t.Run(version.name, func(t *testing.T) {
			b := &Broadcaster{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			listener, err := b.minecraftListenConfig(room.Status{}).Listen("raknet", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			conn, err := (minecraft.Dialer{Protocol: minecraft.BasicProtocol{Protocol: version.id, Version: version.name}}).DialTimeout("raknet", listener.Addr().String(), 5*time.Second)
			if conn != nil {
				_ = conn.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "client outdated") {
				t.Fatalf("legacy client error = %v, want client outdated", err)
			}
		})
	}
}

func TestMinecraftListenConfigKeepsCustomPacketFunc(t *testing.T) {
	customCalled := false
	custom := func(packet.Header, []byte, net.Addr, net.Addr) {
		customCalled = true
	}
	b := &Broadcaster{
		log: slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})),
		conf: Config{
			ListenConfig: minecraft.ListenConfig{PacketFunc: custom},
			Status:       Status{HostName: "Host", WorldName: "World"},
		},
	}

	conf := b.minecraftListenConfig(room.Status{HostName: "Host", WorldName: "World"})
	conf.PacketFunc(packet.Header{}, nil, nil, nil)
	if !customCalled {
		t.Fatal("custom packet func was not preserved")
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
			MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: uuid.New()},
			ListenConfig: minecraft.ListenConfig{
				AuthenticationDisabled: true,
			},
			Status:         Status{HostName: "Host", WorldName: "World"},
			UpdateInterval: 30 * time.Second,
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

func TestBroadcasterAllowsAnonymousNetherNetByDefault(t *testing.T) {
	b := &Broadcaster{}
	conf := b.netherNetListenConfig()
	if !conf.AllowAnonymous {
		t.Fatal("default nethernet listener should allow anonymous offers for Lunar friend-world compatibility")
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

func TestBroadcasterTransferSendsStartupSequenceBeforeTransfer(t *testing.T) {
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
		Status: Status{WorldName: "Redirect Lobby"},
	}, transferCloseTimeout: time.Second}
	var packetsMu sync.Mutex
	var startupPackets []uint32
	var startGamePayload []byte
	cfg := b.minecraftListenConfig(room.Status{})
	cfg.AuthenticationDisabled = true
	cfg.PacketFunc = func(header packet.Header, payload []byte, _, _ net.Addr) {
		switch header.PacketID {
		case packet.IDJigsawStructureData, packet.IDVoxelShapes, packet.IDStartGame, packet.IDItemRegistry, packet.IDTransfer:
			packetsMu.Lock()
			startupPackets = append(startupPackets, header.PacketID)
			if header.PacketID == packet.IDStartGame {
				startGamePayload = bytes.Clone(payload)
			}
			packetsMu.Unlock()
		}
	}
	listener, err := cfg.Listen("raknet", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	served := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			served <- err
			return
		}
		b.transfer(conn.(*minecraft.Conn))
		served <- nil
	}()

	// Transfer can arrive during login or just after the client finishes spawning.
	conn, err := (minecraft.Dialer{}).DialTimeout("raknet", listener.Addr().String(), 5*time.Second)
	var transfer *packet.Transfer
	if err != nil {
		var transferErr *minecraft.TransferError
		if !errors.As(err, &transferErr) {
			t.Fatalf("client was not transferred: %v", err)
		}
		transfer = &packet.Transfer{Address: transferErr.Address, Port: transferErr.Port}
	} else {
		defer conn.Close()
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		for transfer == nil {
			pk, err := conn.ReadPacket()
			if err != nil {
				t.Fatalf("read transfer: %v", err)
			}
			transfer, _ = pk.(*packet.Transfer)
		}
		_ = conn.Close()
	}
	if transfer.Address != "play.example.net" || transfer.Port != 19133 {
		t.Fatalf("unexpected transfer target %#v", transfer)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("transfer handler did not finish")
	}

	// Vanilla requires structure and shape data before StartGame, even for redirects.
	packetsMu.Lock()
	defer packetsMu.Unlock()
	wantPackets := []uint32{packet.IDJigsawStructureData, packet.IDVoxelShapes, packet.IDStartGame, packet.IDItemRegistry, packet.IDTransfer}
	if !slices.Equal(startupPackets, wantPackets) {
		t.Fatalf("startup packet IDs = %v, want %v", startupPackets, wantPackets)
	}
	var startGame packet.StartGame
	startGame.Marshal(protocol.NewReader(bytes.NewReader(startGamePayload), 0, true))
	if startGame.WorldName != "Redirect Lobby" || startGame.Dimension != 2 || startGame.PlayerGameMode != 1 || startGame.WorldGameMode != 1 {
		t.Fatalf("unexpected StartGame redirect shape %#v", startGame)
	}
	if startGame.BaseGameVersion != "*" || startGame.GameVersion != protocol.CurrentVersion || startGame.ServerAuthoritativeInventory {
		t.Fatalf("unexpected StartGame version/inventory fields %#v", startGame)
	}
	if startGame.PlayerMovementSettings.ServerAuthoritativeBlockBreaking {
		t.Fatalf("unexpected StartGame movement settings %#v", startGame.PlayerMovementSettings)
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

func TestBroadcasterTransferStopsWhenStartupFails(t *testing.T) {
	conn := &recordingTransferConn{startGameErr: errors.New("startup failed")}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
	}}

	b.transfer(conn)

	if len(conn.packets) != 0 || conn.flushes != 0 || conn.readStarted() {
		t.Fatal("transfer continued after startup failed")
	}
	if !conn.closed {
		t.Fatal("connection was not closed after startup failed")
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
	startGameErr     error
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

// SendStartGame returns the scripted startup result for transfer lifecycle tests.
func (c *recordingTransferConn) SendStartGame(minecraft.GameData) error {
	return c.startGameErr
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

func TestSubAccountInviterRequiresPublishedSession(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
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
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
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
		_, err := b.startSubAccounts(b.ctx, room.Status{}, nil)
		done <- err
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
