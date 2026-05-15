package broadcaster

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/sandertv/gophertunnel/minecraft/room"
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
	if err := g.SetShowcase(context.Background(), "1", testImageFile(t), true); err != nil {
		t.Fatal(err)
	}
	if !listed || !uploaded {
		t.Fatalf("listed=%v uploaded=%v", listed, uploaded)
	}
}

func TestGalleryClientDoesNotSendAuthToImageURL(t *testing.T) {
	imagePath := filepath.Join(t.TempDir(), "image.jpg")
	if err := os.WriteFile(imagePath, []byte("same-image"), 0o600); err != nil {
		t.Fatal(err)
	}
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
				return response(http.StatusOK, "same-image"), nil
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
		TokenSource:     staticTokenSource{},
		LiveTokenSource: staticOAuthSource{},
		Server:          ServerInfo{Host: "127.0.0.1", Port: 19132},
		StatusProvider:  staticStatusProvider{host: "Provider Host", world: "Provider World"},
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
}

func TestRoomStatusProviderNormalizesConfiguredProvider(t *testing.T) {
	b := &Broadcaster{conf: Config{
		StatusProvider: staticStatusProvider{host: "Provider Host", world: "Provider World"},
	}}
	status := b.roomStatusProvider(room.Status{}).RoomStatus()
	if status.Protocol == 0 || status.Version == "" || status.TitleID == 0 {
		t.Fatalf("provider status was not normalized: %#v", status)
	}
	if status.BroadcastSetting == 0 || status.Joinability == "" {
		t.Fatalf("provider session controls were not normalized: %#v", status)
	}
	if !status.OnlineCrossPlatformGame {
		t.Fatalf("provider status missing cross-platform default: %#v", status)
	}
}

func TestRoomListenConfigDisablesServerStatusOverrideForConfiguredProvider(t *testing.T) {
	b := &Broadcaster{conf: Config{
		StatusProvider: staticStatusProvider{host: "Provider Host", world: "Provider World"},
	}}
	conf := b.roomListenConfig(room.Status{})
	if !conf.DisableServerStatusOverride {
		t.Fatal("custom status provider should not be overwritten by listener server status")
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
}

func (s staticStatusProvider) RoomStatus() room.Status {
	return room.Status{
		HostName:       s.host,
		WorldName:      s.world,
		MemberCount:    1,
		MaxMemberCount: 2,
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

func (f fakeInviter) Invite(xuid string, _ int32) error {
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

type fakeSignaling struct {
	closed bool
}

func (f *fakeSignaling) Signal(context.Context, *nethernet.Signal) error { return nil }
func (f *fakeSignaling) Notify(chan<- *nethernet.Signal) func()          { return func() {} }
func (f *fakeSignaling) Context() context.Context                        { return context.Background() }
func (f *fakeSignaling) Credentials(context.Context) (*nethernet.Credentials, error) {
	return nil, nil
}
func (f *fakeSignaling) NetworkID() string { return "network" }
func (f *fakeSignaling) PongData([]byte)   {}
func (f *fakeSignaling) Close() error {
	f.closed = true
	return nil
}
