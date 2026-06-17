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

func TestLoadConfigFileAcceptsUpstreamKebabCaseKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(`
config-version: 2
debug-mode: true
suppress-session-update-message: true

session:
  remote-address: bedrock.example.net
  remote-port: "19133"
  update-interval: 45
  query-server: false
  web-query-fallback: true
  config-fallback: false
  broadcast-setting: 2
  joinability: InviteOnly
  world-type: Creative
  session-info:
    host-name: Example Host
    world-name: Example World
    players: 4
    max-players: 32
    ip: ignored.example.net
    port: 19134

friend-sync:
  update-interval: 75
  auto-follow: false
  auto-unfollow: true
  initial-invite: false
  expiry:
    enabled: false
    days: 21
    check: 2400
    history-path: cache/upstream_history.json

notifications:
  enabled: true
  webhook-url: https://example.net/webhook

gallery:
  enabled: true
  image-path: images/upstream.jpg
  delete-other-images: false

accounts:
  primary-cache-path: cache/upstream_live_token.json
  sub-accounts:
    - id: alt
      enabled: true
      cache-path: cache/alt_live_token.json
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if cfg.ConfigVersion != CurrentConfigVersion {
		t.Fatalf("expected migrated config version, got %d", cfg.ConfigVersion)
	}
	if !cfg.DebugMode || !cfg.SuppressSessionUpdateMessage {
		t.Fatalf("top-level kebab-case aliases were not loaded: %#v", cfg)
	}
	if cfg.Session.RemoteAddress != "bedrock.example.net" || cfg.Session.RemotePort != "19133" {
		t.Fatalf("session remote aliases were not loaded: %#v", cfg.Session)
	}
	if cfg.Session.UpdateInterval != 45 || cfg.Session.QueryServer || !cfg.Session.WebQueryFallback || cfg.Session.ConfigFallback {
		t.Fatalf("session query aliases were not loaded: %#v", cfg.Session)
	}
	if cfg.Session.BroadcastSetting != 2 || cfg.Session.Joinability != "InviteOnly" || cfg.Session.WorldType != "Creative" {
		t.Fatalf("session status aliases were not loaded: %#v", cfg.Session)
	}
	if cfg.Session.SessionInfo.HostName != "Example Host" || cfg.Session.SessionInfo.MaxPlayers != 32 {
		t.Fatalf("session-info aliases were not loaded: %#v", cfg.Session.SessionInfo)
	}
	if cfg.FriendSync.UpdateInterval != 75 || cfg.FriendSync.AutoFollow || !cfg.FriendSync.AutoUnfollow || cfg.FriendSync.InitialInvite {
		t.Fatalf("friend-sync aliases were not loaded: %#v", cfg.FriendSync)
	}
	if cfg.FriendSync.Expiry.HistoryPath != "cache/upstream_history.json" || cfg.FriendSync.Expiry.Enabled {
		t.Fatalf("friend-sync expiry aliases were not loaded: %#v", cfg.FriendSync.Expiry)
	}
	if cfg.Notifications.WebhookURL != "https://example.net/webhook" {
		t.Fatalf("notification alias was not loaded: %#v", cfg.Notifications)
	}
	if cfg.Gallery.ImagePath != "images/upstream.jpg" || cfg.Gallery.DeleteOtherImages {
		t.Fatalf("gallery aliases were not loaded: %#v", cfg.Gallery)
	}
	if cfg.Accounts.PrimaryCachePath != "cache/upstream_live_token.json" || len(cfg.Accounts.SubAccounts) != 1 || cfg.Accounts.SubAccounts[0].CachePath != "cache/alt_live_token.json" {
		t.Fatalf("account aliases were not loaded: %#v", cfg.Accounts)
	}
}

func TestLoadConfigFileMigratesUpstreamLegacyKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(`
config-version: 1
debug-log: true
suppress-session-update-info: true
remote-address: legacy.example.net
remote-port: "19135"
update-interval: 55

friend-sync:
  should-expire: false
  expire-days: 30
  expire-check: 3600
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}

	if !cfg.DebugMode || !cfg.SuppressSessionUpdateMessage {
		t.Fatalf("legacy top-level aliases were not migrated: %#v", cfg)
	}
	if cfg.Session.RemoteAddress != "legacy.example.net" || cfg.Session.RemotePort != "19135" || cfg.Session.UpdateInterval != 55 {
		t.Fatalf("legacy session keys were not moved: %#v", cfg.Session)
	}
	if cfg.FriendSync.Expiry.Enabled || cfg.FriendSync.Expiry.Days != 30 || cfg.FriendSync.Expiry.Check != 3600 {
		t.Fatalf("legacy friend expiry aliases were not migrated: %#v", cfg.FriendSync.Expiry)
	}
}

func TestLoadConfigFileMigratesLegacySlackWebhook(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(`
slack-webhook: https://example.net/hook
`), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Notifications.Enabled || cfg.Notifications.WebhookURL != "https://example.net/hook" {
		t.Fatalf("legacy slack webhook was not migrated: %#v", cfg.Notifications)
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
		XBLTokenSource:  staticTokenSource{},
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
	if runtime.SignalingMode != SignalingModeJSONRPC {
		t.Fatalf("unexpected signaling mode %q", runtime.SignalingMode)
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
		XBLTokenSource:  staticTokenSource{},
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
		XBLTokenSource:  staticTokenSource{},
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
