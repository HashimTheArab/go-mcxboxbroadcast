package broadcaster

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadConfigFileCreatesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigVersion != CurrentConfigVersion {
		t.Fatalf("unexpected config version %d", cfg.ConfigVersion)
	}
	if cfg.Session.UpdateInterval != 30 {
		t.Fatalf("unexpected update interval %d", cfg.Session.UpdateInterval)
	}
	if cfg.Gallery.ImagePath != "screenshot.jpg" {
		t.Fatalf("unexpected image path %q", cfg.Gallery.ImagePath)
	}
	if cfg.FriendSync.Expiry.HistoryPath != "cache/player_history.json" {
		t.Fatalf("unexpected history path %q", cfg.FriendSync.Expiry.HistoryPath)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("default config was not written: %v", err)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	cfg, err := LoadConfigFile("config.example.yml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigVersion != CurrentConfigVersion {
		t.Fatalf("unexpected config version %d", cfg.ConfigVersion)
	}
	if cfg.Gallery.ImagePath != "screenshot.jpg" {
		t.Fatalf("unexpected gallery image path %q", cfg.Gallery.ImagePath)
	}
}

func TestConfigFileToConfigMapsOperatorSettings(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.Session.UpdateInterval = 45
	cfg.Session.SessionInfo.IP = "bedrock.example.net"
	cfg.Session.SessionInfo.Port = 19133
	cfg.Session.SessionInfo.HostName = "Host"
	cfg.Session.SessionInfo.WorldName = "World"
	cfg.Session.BroadcastSetting = int32(BroadcastSettingFriendsOnly)
	cfg.Session.Joinability = JoinabilityInviteOnly
	cfg.Session.WorldType = WorldTypeSurvival
	cfg.Session.QueryServer = false
	cfg.Gallery.Enabled = true
	cfg.Gallery.ImagePath = "images/showcase.jpg"

	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{
		TokenSource:     staticTokenSource{},
		LiveTokenSource: staticOAuthSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Server.Host != "bedrock.example.net" || runtime.Server.Port != 19133 {
		t.Fatalf("unexpected server %#v", runtime.Server)
	}
	if runtime.UpdateInterval != 45*time.Second {
		t.Fatalf("unexpected runtime interval %s", runtime.UpdateInterval)
	}
	if runtime.SuppressSessionUpdateMessage {
		t.Fatal("unexpected suppressed session update message")
	}
	if runtime.Status.Broadcast != int32(BroadcastSettingFriendsOnly) {
		t.Fatalf("unexpected broadcast setting %d", runtime.Status.Broadcast)
	}
	if runtime.Gallery == nil || runtime.Gallery.ImagePath != "images/showcase.jpg" {
		t.Fatalf("gallery config not mapped: %#v", runtime.Gallery)
	}
	if runtime.FriendHistory == nil {
		t.Fatal("friend history store not mapped")
	}
}

func TestConfigFileMapsSuppressSessionUpdateMessage(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.SuppressSessionUpdateMessage = true

	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{
		TokenSource:     staticTokenSource{},
		LiveTokenSource: staticOAuthSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.SuppressSessionUpdateMessage {
		t.Fatal("suppress session update message was not mapped")
	}
}

func TestConfigFileDisablesFriendSyncWhenNoActionsConfigured(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.FriendSync.AutoFollow = false
	cfg.FriendSync.AutoUnfollow = false
	cfg.FriendSync.Expiry.Enabled = false

	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{
		TokenSource:     staticTokenSource{},
		LiveTokenSource: staticOAuthSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.FriendSync != nil {
		t.Fatalf("expected friend sync disabled, got %#v", runtime.FriendSync)
	}
}

func TestHTTPConfigClientConfiguresProxyTransport(t *testing.T) {
	cfg := HTTPFileConfig{Proxy: "http://127.0.0.1:8080"}

	client, err := cfg.Client(nil)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := client.Transport.(*http.Transport).Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL.String() != "http://127.0.0.1:8080" {
		t.Fatalf("unexpected proxy URL %q", proxyURL.String())
	}
}

func TestHTTPConfigClientRejectsInvalidProxy(t *testing.T) {
	cfg := HTTPFileConfig{Proxy: "://bad"}

	if _, err := cfg.Client(nil); err == nil {
		t.Fatal("expected invalid proxy error")
	}
}

func TestHTTPConfigClientRejectsCustomTransportWithProxy(t *testing.T) {
	cfg := HTTPFileConfig{Proxy: "http://127.0.0.1:8080"}
	base := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, nil
	})}

	if _, err := cfg.Client(base); err == nil {
		t.Fatal("expected custom transport error")
	}
}

func TestLoadConfigFileMigratesVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(`
configVersion = 1

[session]
updateInterval = 10
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConfigVersion != CurrentConfigVersion {
		t.Fatalf("expected migrated version, got %d", cfg.ConfigVersion)
	}
	if cfg.Session.UpdateInterval != 20 {
		t.Fatalf("expected interval clamp during migration, got %d", cfg.Session.UpdateInterval)
	}
	if cfg.FriendSync.Expiry.HistoryPath != "cache/player_history.json" {
		t.Fatalf("expected default history path, got %q", cfg.FriendSync.Expiry.HistoryPath)
	}
}
