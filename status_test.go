package broadcaster

import (
	"context"
	"encoding/base64"
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

func TestStatusVersionOverride(t *testing.T) {
	status := func(version string) room.Status {
		b, err := New(Config{
			XBLTokenSource: staticTokenSource{},
			XUID:           "123",
			Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
			Status:         Status{HostName: "Host", Version: version},
		})
		if err != nil {
			t.Fatal(err)
		}
		st, err := b.status(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	if got := status("1.26.32").Version; got != "1.26.32" {
		t.Fatalf("version override not applied: %q", got)
	}
	if got := status("").Version; got != protocol.CurrentVersion {
		t.Fatalf("empty version should fall back to protocol.CurrentVersion, got %q", got)
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
