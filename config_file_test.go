package broadcaster

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
	if cfg.Session.SignalingMode != string(SignalingModeWebSocket) {
		t.Fatalf("default signaling mode = %q, want websocket", cfg.Session.SignalingMode)
	}
	if cfg.Gallery.ImagePath != "screenshot.jpg" {
		t.Fatalf("unexpected image path %q", cfg.Gallery.ImagePath)
	}
	if want := (FriendCleanupFile{InactiveDays: 15, MaxFriends: 950, Interval: 1800, HistoryPath: "cache/player_history.json"}); cfg.FriendSync.Cleanup != want {
		t.Fatalf("friend cleanup = %#v, want %#v", cfg.FriendSync.Cleanup, want)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("default config was not written: %v", err)
	}
}

func TestExampleConfigLoads(t *testing.T) {
	example, err := os.ReadFile("config.example.yml")
	if err != nil {
		t.Fatal(err)
	}
	// Load a copy: a migration would rewrite the file it loads.
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, example, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Notes) != 0 {
		t.Fatalf("example config needed adjustments: %q", cfg.Notes)
	}
	if cfg.ConfigVersion != CurrentConfigVersion {
		t.Fatalf("unexpected config version %d", cfg.ConfigVersion)
	}
	if cfg.Gallery.ImagePath != "screenshot.jpg" {
		t.Fatalf("unexpected gallery image path %q", cfg.Gallery.ImagePath)
	}
	if _, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}}); err != nil {
		t.Fatalf("example config does not produce a valid runtime config: %v", err)
	}
}

func TestConfigFileMapsJSONRPCSignalingMode(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.Session.SignalingMode = "jsonrpc"
	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.SignalingMode != SignalingModeJSONRPC {
		t.Fatalf("signaling mode = %q, want jsonrpc", runtime.SignalingMode)
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
	if want := (FriendCleanupFile{Interval: 2400, HistoryPath: "cache/upstream_history.json"}); cfg.FriendSync.Cleanup != want {
		t.Fatalf("friendSync expiry was not migrated: %#v, want %#v", cfg.FriendSync.Cleanup, want)
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
	if cfg.FriendSync.Cleanup != DefaultConfigFile().FriendSync.Cleanup {
		t.Fatalf("legacy friend expiry keys were translated: %#v", cfg.FriendSync.Cleanup)
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
	cfg.Session.SessionInfo.Version = "1.26.44"
	cfg.Session.SessionInfo.Protocol = 2168
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
	if runtime.Status.Version != "1.26.44" || runtime.Status.Protocol != 2168 {
		t.Fatalf("advertised pair = %s/%d, want 1.26.44/2168", runtime.Status.Version, runtime.Status.Protocol)
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
	cfg.FriendSync.Cleanup = FriendCleanupFile{}

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
	if cfg.FriendSync.Cleanup.HistoryPath != "cache/player_history.json" {
		t.Fatalf("expected default history path, got %q", cfg.FriendSync.Cleanup.HistoryPath)
	}
}

// Old expiry settings must keep working after an upgrade, with the new
// capacity limit left off until the operator opts in.
func TestLoadConfigFileMigratesFriendExpiry(t *testing.T) {
	tests := []struct {
		name string
		file string
		data string
		want FriendCleanupFile
	}{
		{
			name: "enabled",
			file: "config.yml",
			data: "configVersion: 3\nfriendSync:\n  expiry:\n    enabled: true\n    days: 7\n    check: 600\n    historyPath: cache/h.json\n",
			want: FriendCleanupFile{InactiveDays: 7, Interval: 600, HistoryPath: "cache/h.json"},
		},
		{
			name: "disabled",
			file: "config.yml",
			data: "configVersion: 3\nfriendSync:\n  expiry:\n    enabled: false\n    days: 7\n",
			want: FriendCleanupFile{Interval: 1800, HistoryPath: "cache/player_history.json"},
		},
		{
			name: "partial block keeps old defaults",
			file: "config.yml",
			data: "configVersion: 3\nfriendSync:\n  expiry:\n    days: 9\n",
			want: FriendCleanupFile{InactiveDays: 9, Interval: 1800, HistoryPath: "cache/player_history.json"},
		},
		{
			name: "absent",
			file: "config.yml",
			data: "configVersion: 3\n",
			want: FriendCleanupFile{InactiveDays: 15, Interval: 1800, HistoryPath: "cache/player_history.json"},
		},
		{
			name: "toml",
			file: "config.toml",
			data: "configVersion = 3\n[friendSync.expiry]\nenabled = true\ndays = 5\n",
			want: FriendCleanupFile{InactiveDays: 5, Interval: 1800, HistoryPath: "cache/player_history.json"},
		},
		{
			name: "unversioned expiry block",
			file: "config.yml",
			data: "friendSync:\n  expiry:\n    enabled: true\n    days: 4\n",
			want: FriendCleanupFile{InactiveDays: 4, Interval: 1800, HistoryPath: "cache/player_history.json"},
		},
		{
			name: "current version keeps cleanup",
			file: "config.yml",
			data: "configVersion: 4\nfriendSync:\n  cleanup:\n    inactiveDays: 3\n    maxFriends: 900\n    interval: 60\n",
			want: FriendCleanupFile{InactiveDays: 3, MaxFriends: 900, Interval: 60, HistoryPath: "cache/player_history.json"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tt.file)
			if err := os.WriteFile(path, []byte(tt.data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfigFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.FriendSync.Cleanup != tt.want {
				t.Fatalf("cleanup = %#v, want %#v", cfg.FriendSync.Cleanup, tt.want)
			}
			// The rewritten file must load to the same settings.
			reloaded, err := LoadConfigFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if reloaded.FriendSync.Cleanup != tt.want {
				t.Fatalf("reloaded cleanup = %#v, want %#v", reloaded.FriendSync.Cleanup, tt.want)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "expiry") {
				t.Fatalf("migrated file still has an expiry block:\n%s", data)
			}
		})
	}
}

func TestLoadConfigFileNotesFriendLimitWithoutHeadroom(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(path, []byte("configVersion: 4\nfriendSync:\n  cleanup:\n    maxFriends: 1000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(cfg.Notes, func(note string) bool { return strings.Contains(note, "maxFriends 1000") }) {
		t.Fatalf("notes = %q, want a maxFriends headroom note", cfg.Notes)
	}
}
