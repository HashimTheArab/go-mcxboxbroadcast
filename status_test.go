package broadcaster

import (
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft"

	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestStatusDefaults(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
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
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
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

// Every advertised protocol/version pair must be one the broadcaster's own listener accepts.
func TestStatusAdvertisesPairListenerAccepts(t *testing.T) {
	legacy := room.Status{HostName: "Host", Protocol: 2168, Version: "1.26.44"}
	for name, conf := range map[string]Config{
		"configured":      {Status: Status{HostName: "Host"}},
		"status provider": {StatusProvider: room.NewStatusProvider(legacy)},
	} {
		t.Run(name, func(t *testing.T) {
			conf.XBLTokenSource = staticTokenSource{}
			conf.XUID = "123"
			conf.Server = ServerInfo{Host: "127.0.0.1", Port: 19132}
			conf.ListenConfig = minecraft.ListenConfig{AuthenticationDisabled: true}
			b, err := New(conf)
			if err != nil {
				t.Fatal(err)
			}
			status, err := b.status(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if status.Protocol != protocol.CurrentProtocol || status.Version != protocol.CurrentVersion {
				t.Fatalf("advertised %s/%d, want %s/%d", status.Version, status.Protocol, protocol.CurrentVersion, protocol.CurrentProtocol)
			}
			l, err := b.minecraftListenConfig(status).Listen("raknet", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			dialer := minecraft.Dialer{Protocol: minecraft.BasicProtocol{Protocol: status.Protocol, Version: status.Version}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				_ = conn.(*minecraft.Conn).StartGame(minecraft.GameData{})
			}()
			conn, err := dialer.DialContext(ctx, "raknet", l.Addr().String())
			if err != nil {
				t.Fatalf("listener rejects the advertised pair: %v", err)
			}
			_ = conn.Close()
		})
	}
}

func TestStatusDefaultsToMinecraft12650(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
		Status:         Status{HostName: "Host"},
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.Protocol != 2193 || status.Version != "1.26.50" {
		t.Fatalf("advertised protocol/version = %d/%q, want 2193/%q", status.Protocol, status.Version, "1.26.50")
	}
}

func TestStatusLevelIDUniquePerAccount(t *testing.T) {
	levelID := func(xuid string) string {
		b, err := New(Config{
			XBLTokenSource: staticTokenSource{},
			XUID:           xuid,
			Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
			Status:         Status{HostName: "Host"},
		})
		if err != nil {
			t.Fatal(err)
		}
		status, err := b.status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if status.LevelID == "" {
			t.Fatal("level ID is empty")
		}
		// Vanilla level ids are base64 of 8 bytes (a random int64).
		if raw, err := base64.StdEncoding.DecodeString(status.LevelID); err != nil || len(raw) != 8 {
			t.Fatalf("level ID %q is not base64 of 8 bytes (err=%v)", status.LevelID, err)
		}
		again, err := b.status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
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
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		// Port 1 on localhost is closed, so the query fails quickly.
		Server: ServerInfo{Host: "127.0.0.1", Port: 1},
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
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.WorldName != "Queried World" || status.HostName != "Queried Host" {
		t.Fatalf("expected last query result to be kept, got %q/%q", status.WorldName, status.HostName)
	}
	if status.MemberCount != 7 || status.MaxMemberCount != 30 {
		t.Fatalf("expected last query counts, got %d/%d", status.MemberCount, status.MaxMemberCount)
	}
}

func TestStatusResetsToConfigWhenQueryFailsWithConfigFallback(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 1},
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
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.WorldName != "Config World" || status.HostName != "Config Host" {
		t.Fatalf("expected config fallback values, got %q/%q", status.WorldName, status.HostName)
	}
}

// A timed-out status query must not leave its RakNet ping running.
func TestQueryStatusStopsPingOnTimeout(t *testing.T) {
	silent, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	if _, err := queryStatus(context.Background(), silent.LocalAddr().String(), 20*time.Millisecond); err == nil {
		t.Fatal("expected a timeout from a server that never answers")
	}
	deadline := time.Now().Add(time.Second)
	for {
		var dump bytes.Buffer
		_ = pprof.Lookup("goroutine").WriteTo(&dump, 2)
		if !strings.Contains(dump.String(), "go-raknet.Dialer.Ping") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ping still running after queryStatus returned:\n%s", dump.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
