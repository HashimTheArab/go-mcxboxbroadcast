package broadcaster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

func TestSlackNotifierPostsConfiguredMessage(t *testing.T) {
	var body string
	n := SlackNotifier{
		WebhookURL: "https://example.test/slack",
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() != "https://example.test/slack" {
				t.Fatalf("unexpected URL %s", req.URL)
			}
			data, _ := io.ReadAll(req.Body)
			body = string(data)
			return response(http.StatusOK, ""), nil
		})},
	}
	if err := n.Notify(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(body, "hello") {
		t.Fatalf("message missing from body %q", body)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["text"] != "hello" || payload["content"] != "hello" {
		t.Fatalf("unexpected webhook payload %#v", payload)
	}
}

func TestSlackNotifierErrorDoesNotLeakWebhookURL(t *testing.T) {
	secretURL := "https://hooks.slack.com/services/TOKEN/SECRET"
	n := SlackNotifier{
		WebhookURL: secretURL,
		Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusForbidden, ""), nil
		})},
	}
	err := n.Notify(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected webhook error")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("webhook URL leaked in error: %v", err)
	}

	n.Client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	})}
	err = n.Notify(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected transport error")
	}
	if strings.Contains(err.Error(), secretURL) || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("webhook URL leaked in transport error: %v", err)
	}

	n.WebhookURL = "https://hooks.slack.com/services/TOKEN/%zzSECRET"
	err = n.Notify(context.Background(), "hello")
	if err == nil {
		t.Fatal("expected malformed URL error")
	}
	if strings.Contains(err.Error(), "TOKEN") || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("webhook URL leaked in malformed URL error: %v", err)
	}
}

func TestSessionUpdateFailureNotificationCanBeSuppressed(t *testing.T) {
	var notified bool
	b := &Broadcaster{
		conf: Config{
			SuppressSessionUpdateMessage: true,
			Notifier: fakeNotifier{notify: func(context.Context, string) {
				notified = true
			}},
		},
	}
	b.notifySessionUpdateFailure(context.Background(), errors.New("boom"))
	if notified {
		t.Fatal("session update notification was not suppressed")
	}
}

func TestSessionUpdateFailureNotificationUsesLiveContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var gotErr error
	b := &Broadcaster{
		ctx: context.Background(),
		conf: Config{
			Notifier: fakeNotifier{notify: func(ctx context.Context, _ string) {
				gotErr = ctx.Err()
			}},
		},
	}
	b.notifySessionUpdateFailure(ctx, errors.New("boom"))
	if gotErr != nil {
		t.Fatalf("notification used expired context: %v", gotErr)
	}
}

func TestNotifyUsesLiveContextWhenInputContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var gotErr error
	b := &Broadcaster{
		ctx: context.Background(),
		conf: Config{
			Notifier: fakeNotifier{notify: func(ctx context.Context, _ string) {
				gotErr = ctx.Err()
			}},
		},
	}
	b.notify(ctx, "message")
	if gotErr != nil {
		t.Fatalf("notification used canceled context: %v", gotErr)
	}
}

func TestGalleryClientUploadsImageWhenConfigured(t *testing.T) {
	var uploaded, listed bool
	g := GalleryClient{
		TokenSource: staticMinecraftTokenSource{},
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				listed = true
				return response(http.StatusOK, `{"result":{"showcasedImages":[]}}`), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
				uploaded = true
				return response(http.StatusAccepted, `{"result":{"id":"img","isFeatured":true}}`), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
				return nil, nil
			}
		})},
	}
	if err := g.SetShowcase(context.Background(), "1", testGalleryImageFile(t), true); err != nil {
		t.Fatal(err)
	}
	if !listed || !uploaded {
		t.Fatalf("listed=%v uploaded=%v", listed, uploaded)
	}
}

func TestGalleryClientDoesNotSendAuthToImageURL(t *testing.T) {
	imagePath := testGalleryImageFile(t)
	imageBytes := testGalleryImageBytes(t)
	g := GalleryClient{
		TokenSource: staticMinecraftTokenSource{},
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				return response(http.StatusOK, `{"result":{"showcasedImages":[{"id":"img","url":"https://cdn.example.test/image.jpg"}]}}`), nil
			case req.Method == http.MethodGet && req.URL.Host == "cdn.example.test":
				if req.Header.Get("Authorization") != "" {
					t.Fatal("authorization header sent to gallery image URL")
				}
				return responseBytes(http.StatusOK, imageBytes), nil
			case req.Method == http.MethodPost && req.URL.Path == "/api/v1.0/gallery":
				t.Fatal("image should have been reused instead of uploaded")
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}
	if err := g.SetShowcase(context.Background(), "1", imagePath, true); err != nil {
		t.Fatal(err)
	}
}

func TestGalleryClientReportsDeleteFailures(t *testing.T) {
	imagePath := testGalleryImageFile(t)
	imageBytes := testGalleryImageBytes(t)
	g := GalleryClient{
		TokenSource: staticMinecraftTokenSource{},
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/xuid/1"):
				return response(http.StatusOK, `{"result":{"showcasedImages":[{"id":"keep","url":"https://cdn.example.test/image.jpg"},{"id":"old"}]}}`), nil
			case req.Method == http.MethodGet && req.URL.Host == "cdn.example.test":
				return responseBytes(http.StatusOK, imageBytes), nil
			case req.Method == http.MethodDelete && strings.HasSuffix(req.URL.Path, "/old"):
				return response(http.StatusForbidden, ""), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}
	if err := g.SetShowcase(context.Background(), "1", imagePath, true); err == nil {
		t.Fatal("expected delete failure")
	}
}

func TestFriendSyncerFollowsAndUnfollows(t *testing.T) {
	var followed, unfollowed bool
	syncer := FriendSyncer{
		Client: fakeFriendClient{
			people: []Person{
				{XUID: "1", Gamertag: "Follower", IsFollowingCaller: true},
				{XUID: "2", Gamertag: "Old", IsFollowedByCaller: true},
			},
			follow:   func(xuid string) { followed = xuid == "1" },
			unfollow: func(xuid string) { unfollowed = xuid == "2" },
		},
		Config: FriendSyncConfig{AutoFollow: true, AutoUnfollow: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !followed || !unfollowed {
		t.Fatalf("followed=%v unfollowed=%v", followed, unfollowed)
	}
}

func TestFriendSyncerSendsInitialInvite(t *testing.T) {
	var invited string
	syncer := FriendSyncer{
		Client: fakeFriendClient{
			people: []Person{{XUID: "1", Gamertag: "Follower", IsFollowingCaller: true}},
		},
		Inviter: fakeInviter{invite: func(xuid string) { invited = xuid }},
		Config:  FriendSyncConfig{AutoFollow: true, InitialInvite: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if invited != "1" {
		t.Fatalf("expected invite for xuid 1, got %q", invited)
	}
}

func TestFriendSyncerExpiresInactiveFriends(t *testing.T) {
	var unfollowed bool
	syncer := FriendSyncer{
		Client: fakeFriendClient{
			people:   []Person{{XUID: "1", Gamertag: "Old", IsFollowedByCaller: true}},
			unfollow: func(xuid string) { unfollowed = xuid == "1" },
		},
		History: fakeHistoryStore{seen: map[string]time.Time{"1": time.Now().Add(-48 * time.Hour)}},
		Config:  FriendSyncConfig{ExpiryEnabled: true, ExpiryDays: 1},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !unfollowed {
		t.Fatal("expected inactive friend to be unfollowed")
	}
}

func TestFriendSyncerDefaultsExpiryDays(t *testing.T) {
	var unfollowed bool
	syncer := FriendSyncer{
		Client: fakeFriendClient{
			people:   []Person{{XUID: "1", Gamertag: "Recent", IsFollowedByCaller: true}},
			unfollow: func(string) { unfollowed = true },
		},
		History: fakeHistoryStore{seen: map[string]time.Time{"1": time.Now().Add(-24 * time.Hour)}},
		Config:  FriendSyncConfig{ExpiryEnabled: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if unfollowed {
		t.Fatal("zero expiry days should default instead of pruning recent friends")
	}
}

func TestFileHistoryStoreRecordsAndClearsLastSeen(t *testing.T) {
	store := NewFileHistoryStore(filepath.Join(t.TempDir(), "player_history.json"))
	when := time.Now().Add(-time.Hour).Truncate(time.Second)

	if err := store.Seen(context.Background(), "1", when); err != nil {
		t.Fatal(err)
	}
	lastSeen, ok, err := store.LastSeen(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !lastSeen.Equal(when) {
		t.Fatalf("last seen = %s, %v", lastSeen, ok)
	}
	if err := store.Clear(context.Background(), "1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.LastSeen(context.Background(), "1"); err != nil || ok {
		t.Fatalf("expected cleared history, ok=%v err=%v", ok, err)
	}
}

func TestStatusUsesConfiguredProvider(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource:  staticTokenSource{},
		LiveTokenSource: staticOAuthSource{},
		XUID:            "123",
		Server:          ServerInfo{Host: "127.0.0.1", Port: 19132},
		StatusProvider:  staticStatusProvider{host: "Provider Host", world: "Provider World", titleID: TitleID},
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.HostName != "Provider Host" || status.WorldName != "Provider World" {
		t.Fatalf("provider not used: %#v", status)
	}
	if status.OwnerID != "123" {
		t.Fatalf("provider owner id = %q", status.OwnerID)
	}
	if status.TitleID != 0 {
		t.Fatalf("provider title id = %d", status.TitleID)
	}
}

func TestRoomStatusProviderNormalizesConfiguredProvider(t *testing.T) {
	b := &Broadcaster{conf: Config{
		XUID:           "123",
		StatusProvider: staticStatusProvider{host: "Provider Host", world: "Provider World", titleID: TitleID},
	}}
	status := b.roomStatusProvider(room.Status{}).RoomStatus()
	if status.Protocol == 0 || status.Version == "" {
		t.Fatalf("provider status was not normalized: %#v", status)
	}
	if status.OwnerID != "123" {
		t.Fatalf("provider owner id = %q", status.OwnerID)
	}
	if status.TransportLayer != room.TransportLayerNetherNet {
		t.Fatalf("provider transport layer = %d", status.TransportLayer)
	}
	if status.TitleID != 0 {
		t.Fatalf("provider title id = %d", status.TitleID)
	}
	if status.BroadcastSetting == 0 || status.Joinability == "" {
		t.Fatalf("provider session controls were not normalized: %#v", status)
	}
	if !status.OnlineCrossPlatformGame {
		t.Fatalf("provider status missing cross-platform default: %#v", status)
	}
}

func TestMinecraftStatusProviderMirrorsConfiguredProvider(t *testing.T) {
	b := &Broadcaster{conf: Config{
		StatusProvider: staticStatusProvider{host: "Provider Host", world: "Provider World"},
	}}
	status := b.minecraftStatusProvider(room.Status{}).ServerStatus(0, 0)
	if status.ServerName != "Provider World" || status.ServerSubName != "Provider Host" {
		t.Fatalf("minecraft status provider did not mirror room provider: %#v", status)
	}
	if status.PlayerCount != 1 || status.MaxPlayers != 2 {
		t.Fatalf("minecraft status provider did not mirror counts: %#v", status)
	}
	if !b.roomListenConfig(room.Status{}).DisableServerStatusOverride {
		t.Fatal("room server status override must stay disabled so listener pong status does not rewrite MPSD")
	}
}

func TestRoomListenerDoesNotOverridePublishedStatusWithMinecraftPong(t *testing.T) {
	pmsgID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	inner := &fakeAnnouncer{}
	b := &Broadcaster{
		announcer: signalingConnectionAnnouncer{
			Announcer: inner,
			connection: room.Connection{
				ConnectionType: room.ConnectionTypeJSONRPCSignaling,
				NetherNetID:    room.NetherNetID("123456789"),
				PmsgID:         pmsgID,
			},
		},
	}
	listener := b.roomListenConfig(room.Status{
		HostName:       "CoveredJLA",
		WorldName:      "Minecraft World",
		WorldType:      WorldTypeSurvival,
		MemberCount:    1,
		MaxMemberCount: 20,
		TransportLayer: room.TransportLayerNetherNet,
	}).Wrap(fakeNetworkListener{
		addr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132},
	})

	listener.ServerStatus(minecraft.ServerStatus{
		ServerName:    "Lunar Proxy",
		ServerSubName: "\u00a7bLunar Proxy",
		PlayerCount:   0,
		MaxPlayers:    1,
	})

	status := inner.Status()
	if status.HostName != "CoveredJLA" || status.WorldName != "Minecraft World" {
		t.Fatalf("listener pong rewrote published names: %#v", status)
	}
	if status.MemberCount != 1 || status.MaxMemberCount != 20 {
		t.Fatalf("listener pong rewrote published counts: %#v", status)
	}
	if len(status.SupportedConnections) != 1 {
		t.Fatalf("unexpected supported connections: %#v", status.SupportedConnections)
	}
	connection := status.SupportedConnections[0]
	if connection.ConnectionType != room.ConnectionTypeJSONRPCSignaling ||
		connection.NetherNetID != room.NetherNetID("123456789") ||
		connection.PmsgID != pmsgID {
		t.Fatalf("json-rpc connection was not preserved: %#v", connection)
	}
}

func TestSignalingConnectionAnnouncerPublishesJSONRPCConnection(t *testing.T) {
	pmsgID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	inner := &fakeAnnouncer{}
	announcer := signalingConnectionAnnouncer{
		Announcer: inner,
		connection: room.Connection{
			ConnectionType: room.ConnectionTypeJSONRPCSignaling,
			NetherNetID:    room.NetherNetID("123456789"),
			PmsgID:         pmsgID,
		},
	}

	err := announcer.Announce(context.Background(), room.Status{
		SupportedConnections: []room.Connection{{
			ConnectionType: room.ConnectionTypeWebSocketsWebRTCSignaling,
			NetherNetID:    room.NetherNetID("old"),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := inner.Status()
	if len(status.SupportedConnections) != 1 {
		t.Fatalf("unexpected connections: %#v", status.SupportedConnections)
	}
	got := status.SupportedConnections[0]
	if got.ConnectionType != room.ConnectionTypeJSONRPCSignaling {
		t.Fatalf("connection type = %d, want %d", got.ConnectionType, room.ConnectionTypeJSONRPCSignaling)
	}
	if got.NetherNetID != room.NetherNetID("123456789") {
		t.Fatalf("nethernet id = %q", got.NetherNetID)
	}
	if got.PmsgID != pmsgID {
		t.Fatalf("pmsg id = %s", got.PmsgID)
	}
}

func TestStartAdvertisesJSONRPCConnectionWhenConfigured(t *testing.T) {
	pmsgID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	sig := &fakeSignaling{networkID: "123456789"}
	announcer := &fakeAnnouncer{}
	b := &Broadcaster{
		conf: Config{
			Server:               ServerInfo{Host: "127.0.0.1", Port: 19132},
			XBLTokenSource:       staticTokenSource{xuid: "123"},
			Signaling:            sig,
			SignalingMode:        SignalingModeJSONRPC,
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
	got := status.SupportedConnections[0]
	if got.ConnectionType != room.ConnectionTypeJSONRPCSignaling {
		t.Fatalf("connection type = %d, want %d", got.ConnectionType, room.ConnectionTypeJSONRPCSignaling)
	}
	if got.NetherNetID != room.NetherNetID("123456789") {
		t.Fatalf("nethernet id = %q", got.NetherNetID)
	}
	if got.PmsgID != pmsgID {
		t.Fatalf("pmsg id = %s", got.PmsgID)
	}
	if status.OwnerID != "123" {
		t.Fatalf("owner id = %q", status.OwnerID)
	}
	if status.TransportLayer != room.TransportLayerNetherNet {
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
		conf: Config{
			Server:               ServerInfo{Host: "127.0.0.1", Port: 19132},
			XBLTokenSource:       staticTokenSource{xuid: "123"},
			Signaling:            sig,
			SignalingMode:        SignalingModeJSONRPC,
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
	if got := status.SupportedConnections[0].NetherNetID; got != room.NetherNetID(networkID) {
		t.Fatalf("nethernet id = %q, want %q", got, networkID)
	}
}

func TestStartupFailureCleanupClosesSignaling(t *testing.T) {
	sig := &fakeSignaling{}
	b := &Broadcaster{signaling: sig}
	if err := b.cleanupStartupFailure(false); err != nil {
		t.Fatal(err)
	}
	if !sig.closed {
		t.Fatal("signaling was not closed")
	}
}

func TestStartCleansUpWhenPrimaryAnnounceFails(t *testing.T) {
	announceErr := errors.New("announce failed")
	sig := &fakeSignaling{}
	announcer := &fakeAnnouncer{announceErr: announceErr}
	b := &Broadcaster{
		conf: Config{
			Server:    ServerInfo{Host: "127.0.0.1", Port: 19132},
			Signaling: sig,
			Status:    Status{HostName: "Host", WorldName: "World"},
		},
		announcerFactory: func(*Broadcaster) room.Announcer {
			return announcer
		},
	}
	err := b.Start(context.Background())
	if !errors.Is(err, announceErr) {
		t.Fatalf("Start() error = %v, want announce error", err)
	}
	if !announcer.Closed() {
		t.Fatal("announcer was not closed")
	}
	if !sig.closed {
		t.Fatal("signaling was not closed")
	}
}

func TestQueryStatusFallsBackToWeb(t *testing.T) {
	status, err := queryStatusWithFallback(context.Background(), QueryOptions{
		Address:            "127.0.0.1:1",
		Timeout:            time.Nanosecond,
		WebFallbackEnabled: true,
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return response(http.StatusOK, `{"success":true,"ping":{"pong":{"motd":"World","subMotd":"Host","playerCount":3,"maximumPlayerCount":10}}}`), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.ServerName != "World" || status.ServerSubName != "Host" || status.PlayerCount != 3 {
		t.Fatalf("unexpected status %#v", status)
	}
}

type staticStatusProvider struct {
	host, world string
	titleID     int64
}

func (s staticStatusProvider) RoomStatus() room.Status {
	return room.Status{
		HostName:       s.host,
		WorldName:      s.world,
		MemberCount:    1,
		MaxMemberCount: 2,
		TitleID:        s.titleID,
	}
}

type fakeFriendClient struct {
	people   []Person
	follow   func(string)
	unfollow func(string)
}

func (f fakeFriendClient) Friends(context.Context) ([]Person, error) { return f.people, nil }
func (f fakeFriendClient) Follow(_ context.Context, xuid string) error {
	if f.follow != nil {
		f.follow(xuid)
	}
	return nil
}
func (f fakeFriendClient) Unfollow(_ context.Context, xuid string) error {
	if f.unfollow != nil {
		f.unfollow(xuid)
	}
	return nil
}

type fakeInviter struct {
	invite func(string)
}

func (f fakeInviter) Invite(_ context.Context, xuid, _ string) error {
	f.invite(xuid)
	return nil
}

type fakeHistoryStore struct {
	seen map[string]time.Time
}

func (f fakeHistoryStore) LastSeen(_ context.Context, xuid string) (time.Time, bool, error) {
	t, ok := f.seen[xuid]
	return t, ok, nil
}

func (f fakeHistoryStore) Clear(_ context.Context, xuid string) error {
	delete(f.seen, xuid)
	return nil
}

type fakeNotifier struct {
	notify func(context.Context, string)
}

func (f fakeNotifier) Notify(ctx context.Context, message string) error {
	if f.notify != nil {
		f.notify(ctx, message)
	}
	return nil
}

type fakeAnnouncer struct {
	mu          sync.Mutex
	announceErr error
	closed      bool
	status      room.Status
}

func (f *fakeAnnouncer) Announce(_ context.Context, status room.Status) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
	return f.announceErr
}

func (f *fakeAnnouncer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeAnnouncer) Status() room.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeAnnouncer) Closed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

type fakeNetworkListener struct {
	addr net.Addr
}

func (f fakeNetworkListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }
func (f fakeNetworkListener) Close() error              { return nil }
func (f fakeNetworkListener) Addr() net.Addr            { return f.addr }
func (f fakeNetworkListener) ID() int64                 { return 1 }
func (f fakeNetworkListener) PongData([]byte)           {}

type fakeSignaling struct {
	closed    bool
	networkID string
}

func (f *fakeSignaling) Signal(context.Context, *nethernet.Signal) error { return nil }
func (f *fakeSignaling) Notify() (<-chan *nethernet.Signal, func()) {
	ch := make(chan *nethernet.Signal)
	var once sync.Once
	return ch, func() {
		once.Do(func() { close(ch) })
	}
}
func (f *fakeSignaling) Context() context.Context { return context.Background() }
func (f *fakeSignaling) Credentials(context.Context) (*nethernet.Credentials, error) {
	return nil, nil
}
func (f *fakeSignaling) NetworkID() string {
	if f.networkID != "" {
		return f.networkID
	}
	return "network"
}
func (f *fakeSignaling) PongData([]byte) {}
func (f *fakeSignaling) Close() error {
	f.closed = true
	return nil
}

type minecraftTokenSourceWithPMID struct {
	pmid uuid.UUID
}

func (s minecraftTokenSourceWithPMID) ServiceToken(context.Context) (*service.Token, error) {
	payload, err := json.Marshal(map[string]string{"pmid": s.pmid.String()})
	if err != nil {
		return nil, err
	}
	return &service.Token{
		AuthorizationHeader: "Bearer header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature",
		ValidUntil:          time.Now().Add(time.Hour),
		Claims:              service.Claims{PlayerMessagingID: s.pmid},
	}, nil
}
