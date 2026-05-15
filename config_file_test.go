package broadcaster

import (
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
