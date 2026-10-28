package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// A missing config is written with defaults, but the command must not run on them.
func TestLoadConfigFileCreatesDefaultsAndRefusesToRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	if _, err := LoadConfigFile(path); !errors.Is(err, ErrConfigCreated) {
		t.Fatalf("LoadConfigFile() error = %v, want ErrConfigCreated", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("default config was not written: %v", err)
	}
	if perm := info.Mode().Perm(); runtime.GOOS != "windows" && perm != 0o600 {
		t.Fatalf("default config mode = %v, want 0600", perm)
	}
	// Once written, the default only fails on the example target.
	if _, err := LoadConfigFile(path); !errors.Is(err, errExampleServerHost) {
		t.Fatalf("second LoadConfigFile() error = %v, want example host rejection", err)
	}
	cfg := DefaultConfigFile()
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
	if cfg.FriendSync.Expiry.HistoryPath != "cache/player_history.json" {
		t.Fatalf("unexpected history path %q", cfg.FriendSync.Expiry.HistoryPath)
	}
}

// The shipped example must decode strictly at the current version; only its placeholder target is refused.
func TestExampleConfigLoads(t *testing.T) {
	if _, err := LoadConfigFile("config.example.yml"); !errors.Is(err, errExampleServerHost) {
		t.Fatalf("LoadConfigFile(example) error = %v, want example host rejection", err)
	}
	data, err := os.ReadFile("config.example.yml")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.yml")
	data = bytes.Replace(data, []byte("//"+exampleServerHost), []byte("//bedrock.test"), 1)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Notes) != 0 {
		t.Fatalf("example config needed adjustments: %v", cfg.Notes)
	}
	if want := fmt.Sprintf("configVersion: %d\n", CurrentConfigVersion); !bytes.HasPrefix(data, []byte(want)) {
		t.Fatalf("example config does not start with %q", want)
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
	if cfg.Session.JoinURL != "https://ignored.example.net:19134" {
		t.Fatalf("sessionInfo target was not moved to joinUrl: %q", cfg.Session.JoinURL)
	}
	if cfg.FriendSync.UpdateInterval != 75 || cfg.FriendSync.AutoFriend || cfg.FriendSync.InitialInvite {
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

// Unknown or misspelled keys must fail loudly instead of leaving the defaults in place.
func TestLoadConfigFileRejectsUnknownKeys(t *testing.T) {
	for name, tc := range map[string]struct {
		file, data, key string
	}{
		"yaml typo":       {"config.yml", "configVersion: 5\nsession:\n  sesionInfo:\n    hostName: bedrock\n", "sesionInfo"},
		"yaml legacy key": {"config.yml", "config-version: 1\nslack-webhook: https://example.net/hook\n", "config-version"},
		"toml typo":       {"config.toml", "configVersion = 5\n[session.sesionInfo]\nhostName = \"bedrock\"\n", "session.sesionInfo"},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.file)
			if err := os.WriteFile(path, []byte(tc.data), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadConfigFile(path)
			if err == nil || !strings.Contains(err.Error(), tc.key) {
				t.Fatalf("LoadConfigFile() error = %v, want it to name %q", err, tc.key)
			}
		})
	}
}

// Configs written before the advertised-version override was removed keep loading, minus that override.
func TestLoadConfigFileMigratesAdvertisedVersionOverride(t *testing.T) {
	for _, tc := range []struct{ file, data string }{
		{"config.yml", "configVersion: 3\nsession:\n  sessionInfo:\n    ip: bedrock.test\n    protocol: 2168\n    version: 1.26.44\n"},
		{"config.toml", "configVersion = 3\n[session.sessionInfo]\nip = \"bedrock.test\"\nprotocol = 2168\nversion = \"1.26.44\"\n"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.file)
			if err := os.WriteFile(path, []byte(tc.data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfigFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.ConfigVersion != CurrentConfigVersion || cfg.Session.JoinURL != "https://bedrock.test:19132" {
				t.Fatalf("migrated config = version %d join URL %q", cfg.ConfigVersion, cfg.Session.JoinURL)
			}
			if !slices.ContainsFunc(cfg.Notes, func(n string) bool { return strings.Contains(n, "sessionInfo.protocol") }) {
				t.Fatalf("notes = %v, want the removed override reported", cfg.Notes)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("2168")) {
				t.Fatalf("migrated file still carries the override:\n%s", data)
			}
			if _, err := LoadConfigFile(path); err != nil {
				t.Fatalf("migrated file does not reload: %v", err)
			}
		})
	}
}

// Every version after the first gets a migration step, so a version bump cannot skip one.
func TestConfigMigrationsCoverEveryVersion(t *testing.T) {
	for v := 4; v <= CurrentConfigVersion; v++ {
		if configMigrations[v] == nil {
			t.Errorf("no migration to config version %d", v)
		}
	}
	for v := range configMigrations {
		if v > CurrentConfigVersion {
			t.Errorf("migration to version %d is newer than CurrentConfigVersion %d", v, CurrentConfigVersion)
		}
	}
}

func TestConfigFileToConfigMapsOperatorSettings(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.Session.UpdateInterval = 45
	cfg.Session.JoinURL = "https://bedrock.example.net:19133"
	cfg.Session.SessionInfo.Version = "1.26.50"
	cfg.Session.SessionInfo.Protocol = 2193
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
	if runtime.JoinURL != "https://bedrock.example.net:19133" || !runtime.HideOldSessions {
		t.Fatalf("unexpected target %q hide=%v", runtime.JoinURL, runtime.HideOldSessions)
	}
	if runtime.Status.Version != "1.26.50" || runtime.Status.Protocol != 2193 {
		t.Fatalf("advertised pair = %s/%d, want 1.26.50/2193", runtime.Status.Version, runtime.Status.Protocol)
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

func TestConfigFileDisablesFriendSyncWhenNoActionsConfigured(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.FriendSync.AutoFriend = false
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

[session.sessionInfo]
ip = "bedrock.test"
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

// A v4 config moves its target to joinUrl and loses the listener, relay and follower settings.
func TestLoadConfigFileMigratesToHTTPSignalingRelay(t *testing.T) {
	for _, tc := range []struct{ file, data string }{
		{"config.yml", "configVersion: 4\nsession:\n  icePortRange:\n    min: 40000\n    max: 40010\n  sessionInfo:\n    ip: bedrock.test\n    port: 19133\nfriendSync:\n  autoFollow: false\n  autoUnfollow: true\nrelay:\n  enabled: true\n"},
		{"config.toml", "configVersion = 4\n[session.icePortRange]\nmin = 40000\nmax = 40010\n[session.sessionInfo]\nip = \"bedrock.test\"\nport = 19133\n[friendSync]\nautoFollow = false\nautoUnfollow = true\n[relay]\nenabled = true\n"},
	} {
		t.Run(tc.file, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.file)
			if err := os.WriteFile(path, []byte(tc.data), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfigFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Session.JoinURL != "https://bedrock.test:19133" || cfg.FriendSync.AutoFriend || !cfg.Session.HideOldSessions {
				t.Fatalf("migrated config = join %q autoFriend %v hide %v", cfg.Session.JoinURL, cfg.FriendSync.AutoFriend, cfg.Session.HideOldSessions)
			}
			if len(cfg.Notes) != 5 {
				t.Fatalf("notes = %q, want one per migrated key", cfg.Notes)
			}
			if _, err := LoadConfigFile(path); err != nil {
				t.Fatalf("migrated file does not reload: %v", err)
			}
		})
	}
}

func TestRuntimeConfigRejectsHalfAdvertisedPair(t *testing.T) {
	cfg := DefaultConfigFile()
	cfg.Session.JoinURL = "https://bedrock.test:19132"
	cfg.Session.SessionInfo.Version = "1.26.50"
	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(runtime); err == nil {
		t.Fatal("New accepted a version without a protocol")
	}
}
