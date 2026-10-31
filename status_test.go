package broadcaster

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestStatusDefaults(t *testing.T) {
	b, err := New(Config{
		XBLClient: testXBLClient(t, "123"),
		JoinURL:   "http://127.0.0.1:19132",
		Status: Status{
			HostName:   "§aHost",
			WorldName:  "",
			Players:    0,
			MaxPlayers: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := b.status(context.Background())
	if status.HostName != "Host" {
		t.Fatalf("unexpected host name %q", status.HostName)
	}
	if status.WorldName != "Host" {
		t.Fatalf("unexpected world name %q", status.WorldName)
	}
	if status.MemberCount != 1 {
		t.Fatalf("unexpected member count %d (the host counts itself; clients list zero-member worlds as online-only)", status.MemberCount)
	}
	if status.MaxMemberCount != 2 {
		t.Fatalf("unexpected max member count %d", status.MaxMemberCount)
	}
	if status.OwnerID != "123" {
		t.Fatalf("unexpected owner id %q", status.OwnerID)
	}
	if status.TransportLayer != p2p.TransportLayerNetherNet {
		t.Fatalf("unexpected transport layer %d", status.TransportLayer)
	}
	if status.TitleID != 0 {
		t.Fatalf("unexpected title id %d", status.TitleID)
	}
}

func TestStatusUsesConfiguredProvider(t *testing.T) {
	b, err := New(Config{
		XBLClient:      testXBLClient(t, "123"),
		JoinURL:        "http://127.0.0.1:19132",
		StatusProvider: staticStatusProvider{host: "Provider Host", world: "Provider World", titleID: TitleID},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := b.status(context.Background())
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

func TestStatusLevelIDUniquePerAccount(t *testing.T) {
	levelID := func(xuid string) string {
		b, err := New(Config{
			XBLClient: testXBLClient(t, xuid),
			JoinURL:   "http://127.0.0.1:19132",
			Status:    Status{HostName: "Host"},
		})
		if err != nil {
			t.Fatal(err)
		}
		status := b.status(context.Background())
		if status.LevelID == "" {
			t.Fatal("level ID is empty")
		}
		// Vanilla level ids are base64 of 8 bytes (a random int64).
		if raw, err := base64.StdEncoding.DecodeString(status.LevelID); err != nil || len(raw) != 8 {
			t.Fatalf("level ID %q is not base64 of 8 bytes (err=%v)", status.LevelID, err)
		}
		again := b.status(context.Background())
		if again.LevelID != status.LevelID {
			t.Fatalf("level ID not stable: %q != %q", again.LevelID, status.LevelID)
		}
		return status.LevelID
	}
	if levelID("123") == levelID("456") {
		t.Fatal("accounts share a level ID; duplicate world identities collapse into one friend card")
	}
}

func TestNormalizeStatusKeepsDefaultLevelIDStable(t *testing.T) {
	first := normalizeStatus(room.Status{HostName: "Host", WorldName: "World"})
	second := normalizeStatus(room.Status{HostName: "Host", WorldName: "World"})
	if first.LevelID != "level" {
		t.Fatalf("default level ID = %q, want Java's literal \"level\"", first.LevelID)
	}
	if first.LevelID != second.LevelID {
		t.Fatalf("default level ID changed: %q != %q", first.LevelID, second.LevelID)
	}
}

func TestStatusKeepsLastQueryResultWhenQueryFails(t *testing.T) {
	b, err := New(Config{
		XBLClient:  testXBLClient(t, "123"),
		JoinURL:    "http://127.0.0.1:1",
		HTTPClient: offlineHTTPClient(),
		Status: Status{
			HostName:     "Config Host",
			WorldName:    "Config World",
			QueryTarget:  true,
			QueryTimeout: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	b.lastQuery = &minecraft.ServerStatus{
		ServerName:    "Queried World",
		ServerSubName: "Queried Host",
		PlayerCount:   7,
		MaxPlayers:    30,
	}
	status := b.status(context.Background())
	if status.WorldName != "Queried World" || status.HostName != "Queried Host" {
		t.Fatalf("expected last query result to be kept, got %q/%q", status.WorldName, status.HostName)
	}
	if status.MemberCount != 7 || status.MaxMemberCount != 30 {
		t.Fatalf("expected last query counts, got %d/%d", status.MemberCount, status.MaxMemberCount)
	}
}

func TestStatusResetsToConfigWhenQueryFailsWithConfigFallback(t *testing.T) {
	b, err := New(Config{
		XBLClient:  testXBLClient(t, "123"),
		JoinURL:    "http://127.0.0.1:1",
		HTTPClient: offlineHTTPClient(),
		Status: Status{
			HostName:      "Config Host",
			WorldName:     "Config World",
			QueryTarget:   true,
			QueryFallback: true,
			QueryTimeout:  50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	b.lastQuery = &minecraft.ServerStatus{ServerName: "Queried World", ServerSubName: "Queried Host"}
	status := b.status(context.Background())
	if status.WorldName != "Config World" || status.HostName != "Config Host" {
		t.Fatalf("expected config fallback values, got %q/%q", status.WorldName, status.HostName)
	}
}

// A session advertises exactly one pair: the configured override, else gophertunnel's, never a provider's.
func TestStatusAdvertisesOnePair(t *testing.T) {
	provider := room.NewStatusProvider(room.Status{HostName: "Host", Protocol: 2168, Version: "1.26.44"})
	for name, tc := range map[string]struct {
		conf     Config
		version  string
		protocol int32
	}{
		"default":           {Config{Status: Status{HostName: "Host"}}, protocol.CurrentVersion, protocol.CurrentProtocol},
		"provider":          {Config{StatusProvider: provider}, protocol.CurrentVersion, protocol.CurrentProtocol},
		"override":          {Config{Status: Status{HostName: "Host", Version: "1.26.50", Protocol: 2193}}, "1.26.50", 2193},
		"provider override": {Config{StatusProvider: provider, Status: Status{Version: "1.26.50", Protocol: 2193}}, "1.26.50", 2193},
	} {
		t.Run(name, func(t *testing.T) {
			tc.conf.XBLClient = testXBLClient(t, "123")
			tc.conf.JoinURL = "http://127.0.0.1:19132"
			b, err := New(tc.conf)
			if err != nil {
				t.Fatal(err)
			}
			status := b.status(context.Background())
			if status.Version != tc.version || status.Protocol != tc.protocol {
				t.Fatalf("advertised %s/%d, want %s/%d", status.Version, status.Protocol, tc.version, tc.protocol)
			}
		})
	}
}

func TestQueryStatusFallsBackToWeb(t *testing.T) {
	status, err := queryStatusWithFallback(context.Background(), queryOptions{
		JoinURL:            "http://127.0.0.1:1",
		Timeout:            time.Nanosecond,
		WebFallbackEnabled: true,
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host != "checker.geysermc.org" || req.URL.Query().Get("hostname") != "127.0.0.1" || req.URL.Query().Get("port") != "1" {
				return nil, errors.New("endpoint unreachable")
			}
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

// Status queries read the target's /v1/join status document and send the advertised client info.
func TestQueryStatusReadsHTTPSignalingStatus(t *testing.T) {
	var query url.Values
	status, err := queryStatus(context.Background(), queryOptions{
		JoinURL:    "https://bedrock.test:19132",
		ClientInfo: endpoint.ClientInfo{Version: "1.26.60", Protocol: 2225, Platform: relayClientPlatform},
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet || req.URL.Path != "/v1/join" {
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			query = req.URL.Query()
			return response(http.StatusOK, `{"name":"World","level":"Host","players":3,"maxPlayers":10,"protocol":2225,"version":"1.26.60"}`), nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.ServerName != "World" || status.ServerSubName != "Host" || status.PlayerCount != 3 || status.MaxPlayers != 10 {
		t.Fatalf("unexpected status %#v", status)
	}
	if query.Get("version") != "1.26.60" || query.Get("protocol") != "2225" || query.Get("platform") != "-1" || query.Get("id") == "" {
		t.Fatalf("probe query = %v", query)
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
