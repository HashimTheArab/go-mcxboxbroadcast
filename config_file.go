package broadcaster

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/pelletier/go-toml/v2"
	"github.com/pion/webrtc/v4"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"gopkg.in/yaml.v3"
)

const CurrentConfigVersion = 4

type ConfigFile struct {
	ConfigVersion                int                `yaml:"configVersion" toml:"configVersion"`
	DebugMode                    bool               `yaml:"debugMode" toml:"debugMode"`
	SuppressSessionUpdateMessage bool               `yaml:"suppressSessionUpdateMessage" toml:"suppressSessionUpdateMessage"`
	HTTP                         HTTPFileConfig     `yaml:"http" toml:"http"`
	Session                      SessionFileConfig  `yaml:"session" toml:"session"`
	FriendSync                   FriendFileConfig   `yaml:"friendSync" toml:"friendSync"`
	Notifications                NotificationConfig `yaml:"notifications" toml:"notifications"`
	Gallery                      GalleryFileConfig  `yaml:"gallery" toml:"gallery"`
	Relay                        RelayFileConfig    `yaml:"relay" toml:"relay"`
	Accounts                     AccountsConfig     `yaml:"accounts" toml:"accounts"`

	// Notes lists adjustments applied while loading, such as out-of-range
	// values that were clamped. Callers should surface them as warnings.
	Notes []string `yaml:"-" toml:"-"`
}

type HTTPFileConfig struct {
	Proxy string `yaml:"proxy" toml:"proxy"`
}

// SessionFileConfig mirrors MCXboxBroadcast's standalone session settings.
// The Geyser-extension-only remoteAddress/remotePort keys are intentionally
// absent; the broadcast target always comes from sessionInfo.
type SessionFileConfig struct {
	UpdateInterval   int              `yaml:"updateInterval" toml:"updateInterval"`
	SignalingMode    string           `yaml:"signalingMode" toml:"signalingMode"`
	QueryServer      bool             `yaml:"queryServer" toml:"queryServer"`
	WebQueryFallback bool             `yaml:"webQueryFallback" toml:"webQueryFallback"`
	ConfigFallback   bool             `yaml:"configFallback" toml:"configFallback"`
	BroadcastSetting int32            `yaml:"broadcastSetting" toml:"broadcastSetting"`
	WorldType        string           `yaml:"worldType" toml:"worldType"`
	ICEPortRange     ICEPortRangeFile `yaml:"icePortRange" toml:"icePortRange"`
	SessionInfo      SessionInfoFile  `yaml:"sessionInfo" toml:"sessionInfo"`
}

type ICEPortRangeFile struct {
	Min int `yaml:"min" toml:"min"`
	Max int `yaml:"max" toml:"max"`
}

type SessionInfoFile struct {
	HostName   string `yaml:"hostName" toml:"hostName"`
	WorldName  string `yaml:"worldName" toml:"worldName"`
	Players    int    `yaml:"players" toml:"players"`
	MaxPlayers int    `yaml:"maxPlayers" toml:"maxPlayers"`
	IP         string `yaml:"ip" toml:"ip"`
	Port       uint16 `yaml:"port" toml:"port"`
	// Protocol overrides the network protocol advertised in the session.
	// Zero uses the protocol library's current protocol.
	Protocol int32 `yaml:"protocol,omitempty" toml:"protocol,omitempty"`
	// Version overrides the game version advertised in the session document.
	// Empty uses the protocol library's version. Clients hide friend worlds
	// older than their own game version, so set this when a client update
	// ships before the protocol library catches up.
	Version string `yaml:"version,omitempty" toml:"version,omitempty"`
}

type FriendFileConfig struct {
	UpdateInterval int               `yaml:"updateInterval" toml:"updateInterval"`
	AutoFollow     bool              `yaml:"autoFollow" toml:"autoFollow"`
	AutoUnfollow   bool              `yaml:"autoUnfollow" toml:"autoUnfollow"`
	InitialInvite  bool              `yaml:"initialInvite" toml:"initialInvite"`
	Cleanup        FriendCleanupFile `yaml:"cleanup" toml:"cleanup"`
}

// FriendCleanupFile is the file form of FriendCleanupConfig, with Interval in seconds.
type FriendCleanupFile struct {
	InactiveDays int    `yaml:"inactiveDays" toml:"inactiveDays"`
	MaxFriends   int    `yaml:"maxFriends" toml:"maxFriends"`
	Interval     int    `yaml:"interval" toml:"interval"`
	HistoryPath  string `yaml:"historyPath" toml:"historyPath"`
}

// friendExpiryFile is the friendSync.expiry block that configVersion 4
// replaced with friendSync.cleanup.
type friendExpiryFile struct {
	Enabled     bool   `yaml:"enabled" toml:"enabled"`
	Days        int    `yaml:"days" toml:"days"`
	Check       int    `yaml:"check" toml:"check"`
	HistoryPath string `yaml:"historyPath" toml:"historyPath"`
}

const (
	defaultFriendInactiveDays    = 15
	defaultFriendCleanupInterval = 1800
	defaultFriendHistoryPath     = "cache/player_history.json"
)

type NotificationConfig struct {
	Enabled    bool   `yaml:"enabled" toml:"enabled"`
	WebhookURL string `yaml:"webhookUrl" toml:"webhookUrl"`
}

// RelayFileConfig enables relay mode; see RelayConfig for the trust model.
type RelayFileConfig struct {
	Enabled bool `yaml:"enabled" toml:"enabled"`
}

type GalleryFileConfig struct {
	Enabled           bool   `yaml:"enabled" toml:"enabled"`
	ImagePath         string `yaml:"imagePath" toml:"imagePath"`
	DeleteOtherImages bool   `yaml:"deleteOtherImages" toml:"deleteOtherImages"`
}

type AccountsConfig struct {
	PrimaryCachePath string           `yaml:"primaryCachePath" toml:"primaryCachePath"`
	SubAccounts      []SubAccountFile `yaml:"subAccounts" toml:"subAccounts"`
}

type SubAccountFile struct {
	ID        string `yaml:"id" toml:"id"`
	Enabled   bool   `yaml:"enabled" toml:"enabled"`
	CachePath string `yaml:"cachePath" toml:"cachePath"`
}

type RuntimeConfigInput struct {
	XBLClient            *xsapi.Client
	XBLTokenSource       xsapi.TokenSource
	XUID                 string
	MinecraftTokenSource service.TokenSource
	HTTPClient           *http.Client
	Log                  *slog.Logger
	BaseDir              string
}

func DefaultConfigFile() ConfigFile {
	return ConfigFile{
		ConfigVersion: CurrentConfigVersion,
		DebugMode:     false,
		Session: SessionFileConfig{
			UpdateInterval:   30,
			SignalingMode:    string(SignalingModeWebSocket),
			QueryServer:      true,
			WebQueryFallback: false,
			ConfigFallback:   false,
			BroadcastSetting: int32(BroadcastSettingFriendsOfFriends),
			WorldType:        WorldTypeSurvival,
			SessionInfo: SessionInfoFile{
				HostName:   "Minecraft Server",
				WorldName:  "Minecraft World",
				Players:    0,
				MaxPlayers: 20,
				IP:         "play.example.net",
				Port:       19132,
			},
		},
		FriendSync: FriendFileConfig{
			UpdateInterval: 60,
			AutoFollow:     true,
			AutoUnfollow:   true,
			InitialInvite:  true,
			Cleanup: FriendCleanupFile{
				InactiveDays: defaultFriendInactiveDays,
				MaxFriends:   950,
				Interval:     defaultFriendCleanupInterval,
				HistoryPath:  defaultFriendHistoryPath,
			},
		},
		Notifications: NotificationConfig{},
		Gallery: GalleryFileConfig{
			Enabled:           true,
			ImagePath:         "screenshot.jpg",
			DeleteOtherImages: true,
		},
		Accounts: AccountsConfig{
			PrimaryCachePath: "cache/live_token.json",
		},
	}
}

func (h HTTPFileConfig) Client(base *http.Client) (*http.Client, error) {
	if base == nil {
		base = http.DefaultClient
	}
	proxy := strings.TrimSpace(h.Proxy)
	if proxy == "" {
		return base, nil
	}
	proxyURL, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("parse http proxy: %w", err)
	}
	if proxyURL.Scheme == "" || proxyURL.Host == "" {
		return nil, fmt.Errorf("parse http proxy: proxy URL must include scheme and host")
	}
	if proxyURL.Scheme != "http" && proxyURL.Scheme != "https" {
		return nil, fmt.Errorf("parse http proxy: unsupported scheme %q", proxyURL.Scheme)
	}
	transport, err := proxyTransport(base.Transport, proxyURL)
	if err != nil {
		return nil, err
	}
	client := *base
	client.Transport = transport
	return &client, nil
}

func proxyTransport(base http.RoundTripper, proxyURL *url.URL) (http.RoundTripper, error) {
	if base == nil {
		base = http.DefaultTransport
	}
	transport, ok := base.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("configure http proxy: custom transport %T is not supported", base)
	}
	transport = transport.Clone()
	transport.Proxy = http.ProxyURL(proxyURL)
	return transport, nil
}

func LoadConfigFile(path string) (ConfigFile, error) {
	cfg := DefaultConfigFile()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cfg, SaveConfigFile(path, cfg)
	}
	if err != nil {
		return ConfigFile{}, err
	}
	if err := decodeConfig(path, data, &cfg); err != nil {
		return ConfigFile{}, err
	}
	loadedVersion := cfg.ConfigVersion
	migratedExpiry, err := cfg.migrateFriendExpiry(path, data, loadedVersion)
	if err != nil {
		return ConfigFile{}, err
	}
	cfg.migrate()
	if loadedVersion != cfg.ConfigVersion || migratedExpiry {
		if err := SaveConfigFile(path, cfg); err != nil {
			cfg.Notes = append(cfg.Notes, fmt.Sprintf("could not persist migrated config: %v", err))
		}
	}
	return cfg, nil
}

func SaveConfigFile(path string, cfg ConfigFile) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := encodeConfig(path, cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}

func (c *ConfigFile) migrate() {
	if c.ConfigVersion == 0 || c.ConfigVersion < CurrentConfigVersion {
		c.ConfigVersion = CurrentConfigVersion
	}
	if c.Session.UpdateInterval < 20 {
		c.note("session.updateInterval %d is below the 20 second minimum; using 20", c.Session.UpdateInterval)
		c.Session.UpdateInterval = 20
	}
	if c.FriendSync.UpdateInterval < 20 {
		c.note("friendSync.updateInterval %d is below the 20 second minimum; using 20", c.FriendSync.UpdateInterval)
		c.FriendSync.UpdateInterval = 20
	}
	cleanup := &c.FriendSync.Cleanup
	if cleanup.Interval <= 0 {
		c.note("friendSync.cleanup.interval %d is invalid; using %d", cleanup.Interval, defaultFriendCleanupInterval)
		cleanup.Interval = defaultFriendCleanupInterval
	}
	if cleanup.HistoryPath == "" {
		cleanup.HistoryPath = defaultFriendHistoryPath
	}
	if cleanup.MaxFriends == XboxFriendLimit {
		c.note("friendSync.cleanup.maxFriends %d is the Xbox friend limit, so new friend requests can fail until cleanup runs; a lower value keeps room for them", cleanup.MaxFriends)
	}
	if c.Gallery.ImagePath == "" {
		c.Gallery.ImagePath = "screenshot.jpg"
	}
}

// migrateFriendExpiry turns friendSync.expiry, which version 4 replaced, into
// friendSync.cleanup the way the old loader read it. maxFriends stays off so
// existing deployments keep their behaviour until they opt in. It reports
// whether it migrated anything.
func (c *ConfigFile) migrateFriendExpiry(path string, data []byte, loadedVersion int) (bool, error) {
	var present struct {
		FriendSync struct {
			Expiry  *friendExpiryFile  `yaml:"expiry" toml:"expiry"`
			Cleanup *FriendCleanupFile `yaml:"cleanup" toml:"cleanup"`
		} `yaml:"friendSync" toml:"friendSync"`
	}
	if err := decodeConfig(path, data, &present); err != nil {
		return false, err
	}
	if present.FriendSync.Cleanup != nil || (present.FriendSync.Expiry == nil && loadedVersion >= 4) {
		return false, nil
	}
	legacy := struct {
		FriendSync struct {
			Expiry friendExpiryFile `yaml:"expiry" toml:"expiry"`
		} `yaml:"friendSync" toml:"friendSync"`
	}{}
	legacy.FriendSync.Expiry = friendExpiryFile{Enabled: true, Days: defaultFriendInactiveDays, Check: defaultFriendCleanupInterval, HistoryPath: defaultFriendHistoryPath}
	if err := decodeConfig(path, data, &legacy); err != nil {
		return false, err
	}
	expiry := legacy.FriendSync.Expiry
	cleanup := FriendCleanupFile{Interval: expiry.Check, HistoryPath: expiry.HistoryPath}
	if expiry.Enabled {
		cleanup.InactiveDays = expiry.Days
		if cleanup.InactiveDays <= 0 {
			cleanup.InactiveDays = defaultFriendInactiveDays
		}
	}
	c.FriendSync.Cleanup = cleanup
	c.note("migrated friendSync.expiry to friendSync.cleanup (inactiveDays %d); set friendSync.cleanup.maxFriends to keep room for new friends", cleanup.InactiveDays)
	return true, nil
}

func (c *ConfigFile) note(format string, args ...any) {
	c.Notes = append(c.Notes, fmt.Sprintf(format, args...))
}

func (c ConfigFile) RuntimeConfig(in RuntimeConfigInput) (Config, error) {
	if in.BaseDir == "" {
		in.BaseDir = "."
	}
	server := ServerInfo{Host: c.Session.SessionInfo.IP, Port: c.Session.SessionInfo.Port}
	signalingMode, err := normalizeSignalingMode(SignalingMode(c.Session.SignalingMode))
	if err != nil {
		return Config{}, err
	}
	netherNetListenConfig, err := c.Session.ICEPortRange.listenConfig()
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		XBLClient:            in.XBLClient,
		XBLTokenSource:       in.XBLTokenSource,
		XUID:                 in.XUID,
		MinecraftTokenSource: in.MinecraftTokenSource,
		Server:               server,
		Status: Status{
			HostName:         c.Session.SessionInfo.HostName,
			WorldName:        c.Session.SessionInfo.WorldName,
			WorldType:        c.Session.WorldType,
			Protocol:         c.Session.SessionInfo.Protocol,
			Version:          c.Session.SessionInfo.Version,
			Players:          c.Session.SessionInfo.Players,
			MaxPlayers:       c.Session.SessionInfo.MaxPlayers,
			Broadcast:        c.Session.BroadcastSetting,
			QueryTarget:      c.Session.QueryServer,
			WebQueryFallback: c.Session.WebQueryFallback,
			QueryFallback:    c.Session.ConfigFallback,
			WebQueryClient:   in.HTTPClient,
		},
		SignalingMode: signalingMode,
		ListenConfig: minecraft.ListenConfig{
			HTTPClient: in.HTTPClient,
		},
		NetherNetListenConfig:        netherNetListenConfig,
		UpdateInterval:               time.Duration(c.Session.UpdateInterval) * time.Second,
		HTTPClient:                   in.HTTPClient,
		Log:                          in.Log,
		SuppressSessionUpdateMessage: c.SuppressSessionUpdateMessage,
		FriendSync:                   c.FriendSync.runtime(),
	}
	if cfg.FriendSync != nil && cfg.FriendSync.Cleanup.enabled() {
		history := NewFileHistoryStore(resolvePath(in.BaseDir, c.FriendSync.Cleanup.HistoryPath))
		history.Log = in.Log
		cfg.FriendHistory = history
	}
	if c.Relay.Enabled {
		cfg.Relay = &RelayConfig{}
	}
	if c.Gallery.Enabled {
		cfg.Gallery = &GalleryConfig{
			Enabled:           true,
			ImagePath:         resolvePath(in.BaseDir, c.Gallery.ImagePath),
			DeleteOtherImages: c.Gallery.DeleteOtherImages,
			TokenSource:       in.MinecraftTokenSource,
			Client:            in.HTTPClient,
		}
	}
	if c.Notifications.Enabled {
		cfg.Notifier = SlackNotifier{
			WebhookURL: c.Notifications.WebhookURL,
			Client:     in.HTTPClient,
		}
	}
	return cfg, nil
}

func (r ICEPortRangeFile) listenConfig() (nethernet.ListenConfig, error) {
	if r.Min == 0 && r.Max == 0 {
		return nethernet.ListenConfig{}, nil
	}
	if r.Min < 1 || r.Max < 1 || r.Min > 65535 || r.Max > 65535 || r.Min > r.Max {
		return nethernet.ListenConfig{}, fmt.Errorf(
			"session.icePortRange must be disabled with min/max 0 or satisfy 1 <= min <= max <= 65535 (got min=%d max=%d)",
			r.Min, r.Max,
		)
	}
	var settingEngine webrtc.SettingEngine
	if err := settingEngine.SetEphemeralUDPPortRange(uint16(r.Min), uint16(r.Max)); err != nil {
		return nethernet.ListenConfig{}, fmt.Errorf("configure session.icePortRange: %w", err)
	}
	return nethernet.ListenConfig{API: webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))}, nil
}

func (f FriendFileConfig) runtime() *FriendSyncConfig {
	cleanup := FriendCleanupConfig{
		InactiveDays: f.Cleanup.InactiveDays,
		MaxFriends:   f.Cleanup.MaxFriends,
		Interval:     time.Duration(f.Cleanup.Interval) * time.Second,
	}
	if !f.AutoFollow && !f.AutoUnfollow && !cleanup.enabled() {
		return nil
	}
	return &FriendSyncConfig{
		UpdateInterval: time.Duration(f.UpdateInterval) * time.Second,
		AutoFollow:     f.AutoFollow,
		AutoUnfollow:   f.AutoUnfollow,
		InitialInvite:  f.InitialInvite,
		Cleanup:        cleanup,
	}
}

func decodeConfig(path string, data []byte, out any) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".toml":
		return toml.Unmarshal(data, out)
	default:
		return yaml.Unmarshal(data, out)
	}
}

func encodeConfig(path string, cfg ConfigFile) ([]byte, error) {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".toml":
		return toml.Marshal(cfg)
	default:
		return yaml.Marshal(cfg)
	}
}

func resolvePath(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}
