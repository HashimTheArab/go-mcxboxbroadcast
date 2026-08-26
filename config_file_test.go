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
	if cfg.Session.SignalingMode != string(SignalingModeJSONRPC) {
		t.Fatalf("unexpected signaling mode %q", cfg.Session.SignalingMode)
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
	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatalf("example config does not produce a valid runtime config: %v", err)
	}
	if runtime.SignalingMode != SignalingModeJSONRPC {
		t.Fatalf("example signaling mode = %q, want JSON-RPC", runtime.SignalingMode)
	}
}

func TestConfigFileAcceptsWebSocketSignalingMode(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.Session.SignalingMode = "websocket"
	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.SignalingMode != SignalingModeWebSocket {
		t.Fatalf("signaling mode = %q, want WebSocket", runtime.SignalingMode)
	}
}

func TestLoadConfigFileAcceptsCanonicalYAMLKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(`
configVersion: 2
debugMode: true
suppressSessionUpdateMessage: true

session:
  remoteAddress: bedrock.example.net
  remotePort: "19133"
  updateInterval: 45
  queryServer: false
  webQueryFallback: true
  configFallback: false
  broadcastSetting: 2
  worldType: Creative
  sessionInfo:
    hostName: Example Host
    worldName: Example World
    players: 4
    maxPlayers: 32
    ip: ignored.example.net
    port: 19134

friendSync:
  updateInterval: 75
  autoFollow: false
  autoUnfollow: true
  initialInvite: false
  expiry:
    enabled: false
    days: 21
    check: 2400
    historyPath: cache/upstream_history.json

notifications:
  enabled: true
  webhookUrl: https://example.net/webhook

gallery:
  enabled: true
  imagePath: images/upstream.jpg
  deleteOtherImages: false

accounts:
  primaryCachePath: cache/upstream_live_token.json
  subAccounts:
    - id: alt
      enabled: true
      cachePath: cache/alt_live_token.json
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
		t.Fatalf("canonical top-level keys were not loaded: %#v", cfg)
	}
	if cfg.Session.UpdateInterval != 45 || cfg.Session.QueryServer || !cfg.Session.WebQueryFallback || cfg.Session.ConfigFallback {
		t.Fatalf("canonical session query keys were not loaded: %#v", cfg.Session)
	}
	if cfg.Session.BroadcastSetting != 2 || cfg.Session.WorldType != "Creative" {
		t.Fatalf("canonical session status keys were not loaded: %#v", cfg.Session)
	}
	if cfg.Session.SessionInfo.HostName != "Example Host" || cfg.Session.SessionInfo.MaxPlayers != 32 {
		t.Fatalf("canonical sessionInfo keys were not loaded: %#v", cfg.Session.SessionInfo)
	}
	if cfg.FriendSync.UpdateInterval != 75 || cfg.FriendSync.AutoFollow || !cfg.FriendSync.AutoUnfollow || cfg.FriendSync.InitialInvite {
		t.Fatalf("canonical friendSync keys were not loaded: %#v", cfg.FriendSync)
	}
	if cfg.FriendSync.Expiry.HistoryPath != "cache/upstream_history.json" || cfg.FriendSync.Expiry.Enabled {
		t.Fatalf("canonical friendSync expiry keys were not loaded: %#v", cfg.FriendSync.Expiry)
	}
	if cfg.Notifications.WebhookURL != "https://example.net/webhook" {
		t.Fatalf("canonical notification key was not loaded: %#v", cfg.Notifications)
	}
	if cfg.Gallery.ImagePath != "images/upstream.jpg" || cfg.Gallery.DeleteOtherImages {
		t.Fatalf("canonical gallery keys were not loaded: %#v", cfg.Gallery)
	}
	if cfg.Accounts.PrimaryCachePath != "cache/upstream_live_token.json" || len(cfg.Accounts.SubAccounts) != 1 || cfg.Accounts.SubAccounts[0].CachePath != "cache/alt_live_token.json" {
		t.Fatalf("canonical account keys were not loaded: %#v", cfg.Accounts)
	}
}

func TestLoadConfigFileDoesNotTranslateLegacyKeys(t *testing.T) {
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

	if cfg.DebugMode || cfg.SuppressSessionUpdateMessage {
		t.Fatalf("legacy top-level keys were translated: %#v", cfg)
	}
	if cfg.Session.UpdateInterval != 30 {
		t.Fatalf("legacy session keys were translated: %#v", cfg.Session)
	}
	if !cfg.FriendSync.Expiry.Enabled || cfg.FriendSync.Expiry.Days != 15 || cfg.FriendSync.Expiry.Check != 1800 {
		t.Fatalf("legacy friend expiry keys were translated: %#v", cfg.FriendSync.Expiry)
	}
}

func TestLoadConfigFileDoesNotTranslateLegacySlackWebhook(t *testing.T) {
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
	if cfg.Notifications.Enabled || cfg.Notifications.WebhookURL != "" {
		t.Fatalf("legacy slack webhook was translated: %#v", cfg.Notifications)
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
	cfg.Session.WorldType = WorldTypeSurvival
	cfg.Session.QueryServer = false
	cfg.Gallery.Enabled = true
	cfg.Gallery.ImagePath = "images/showcase.jpg"

	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{
		XBLTokenSource: staticTokenSource{},
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
		XBLTokenSource: staticTokenSource{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !runtime.SuppressSessionUpdateMessage {
		t.Fatal("suppress session update message was not mapped")
	}
}

func TestRuntimeConfigMapsICEUDPPortRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte(`
session:
  icePortRange:
    min: 40000
    max: 40010
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.NetherNetListenConfig.API == nil {
		t.Fatal("configured ICE UDP port range did not create a WebRTC API")
	}
}

func TestRuntimeConfigRejectsInvalidICEUDPPortRange(t *testing.T) {
	tests := map[string]string{
		"missing maximum": "min: 40000\n    max: 0",
		"missing minimum": "min: 0\n    max: 40010",
		"reversed":        "min: 40010\n    max: 40000",
		"negative":        "min: -1\n    max: 40000",
		"too large":       "min: 40000\n    max: 65536",
	}
	for name, ports := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			data := "session:\n  icePortRange:\n    " + ports + "\n"
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfigFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}}); err == nil {
				t.Fatal("expected invalid ICE UDP port range error")
			}
		})
	}
}

func TestConfigFileDisablesFriendSyncWhenNoActionsConfigured(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.FriendSync.AutoFollow = false
	cfg.FriendSync.AutoUnfollow = false
	cfg.FriendSync.Expiry.Enabled = false

	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{
		XBLTokenSource: staticTokenSource{},
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
