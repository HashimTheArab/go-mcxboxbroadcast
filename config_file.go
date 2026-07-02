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

	"github.com/df-mc/go-xsapi/v2"
	"github.com/pelletier/go-toml/v2"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"gopkg.in/yaml.v3"
)

const CurrentConfigVersion = 3

type ConfigFile struct {
	ConfigVersion                int                `yaml:"configVersion" toml:"configVersion"`
	DebugMode                    bool               `yaml:"debugMode" toml:"debugMode"`
	SuppressSessionUpdateMessage bool               `yaml:"suppressSessionUpdateMessage" toml:"suppressSessionUpdateMessage"`
	HTTP                         HTTPFileConfig     `yaml:"http" toml:"http"`
	Session                      SessionFileConfig  `yaml:"session" toml:"session"`
	FriendSync                   FriendFileConfig   `yaml:"friendSync" toml:"friendSync"`
	Notifications                NotificationConfig `yaml:"notifications" toml:"notifications"`
	Gallery                      GalleryFileConfig  `yaml:"gallery" toml:"gallery"`
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
	UpdateInterval   int             `yaml:"updateInterval" toml:"updateInterval"`
	SignalingMode    string          `yaml:"signalingMode" toml:"signalingMode"`
	QueryServer      bool            `yaml:"queryServer" toml:"queryServer"`
	WebQueryFallback bool            `yaml:"webQueryFallback" toml:"webQueryFallback"`
	ConfigFallback   bool            `yaml:"configFallback" toml:"configFallback"`
	BroadcastSetting int32           `yaml:"broadcastSetting" toml:"broadcastSetting"`
	WorldType        string          `yaml:"worldType" toml:"worldType"`
	SessionInfo      SessionInfoFile `yaml:"sessionInfo" toml:"sessionInfo"`
}

type SessionInfoFile struct {
	HostName   string `yaml:"hostName" toml:"hostName"`
	WorldName  string `yaml:"worldName" toml:"worldName"`
	Players    int    `yaml:"players" toml:"players"`
	MaxPlayers int    `yaml:"maxPlayers" toml:"maxPlayers"`
	IP         string `yaml:"ip" toml:"ip"`
	Port       uint16 `yaml:"port" toml:"port"`
	// Version overrides the game version advertised in the session document.
	// Empty uses the protocol library's version. Clients hide friend worlds
	// older than their own game version, so set this when a client update
	// ships before the protocol library catches up.
	Version string `yaml:"version,omitempty" toml:"version,omitempty"`
}

type FriendFileConfig struct {
	UpdateInterval int              `yaml:"updateInterval" toml:"updateInterval"`
	AutoFollow     bool             `yaml:"autoFollow" toml:"autoFollow"`
	AutoUnfollow   bool             `yaml:"autoUnfollow" toml:"autoUnfollow"`
	InitialInvite  bool             `yaml:"initialInvite" toml:"initialInvite"`
	Expiry         FriendExpiryFile `yaml:"expiry" toml:"expiry"`
}

type FriendExpiryFile struct {
	Enabled     bool   `yaml:"enabled" toml:"enabled"`
	Days        int    `yaml:"days" toml:"days"`
	Check       int    `yaml:"check" toml:"check"`
	HistoryPath string `yaml:"historyPath" toml:"historyPath"`
}

type NotificationConfig struct {
	Enabled    bool   `yaml:"enabled" toml:"enabled"`
	WebhookURL string `yaml:"webhookUrl" toml:"webhookUrl"`
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
			SignalingMode:    string(SignalingModeJSONRPC),
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
			Expiry: FriendExpiryFile{
				Enabled:     true,
				Days:        15,
				Check:       1800,
				HistoryPath: "cache/player_history.json",
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
	cfg.migrate()
	if loadedVersion != cfg.ConfigVersion {
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
	if c.FriendSync.Expiry.Days <= 0 {
		c.note("friendSync.expiry.days %d is invalid; using 15", c.FriendSync.Expiry.Days)
		c.FriendSync.Expiry.Days = 15
	}
	if c.FriendSync.Expiry.Check <= 0 {
		c.note("friendSync.expiry.check %d is invalid; using 1800", c.FriendSync.Expiry.Check)
		c.FriendSync.Expiry.Check = 1800
	}
	if c.FriendSync.Expiry.HistoryPath == "" {
		c.FriendSync.Expiry.HistoryPath = "cache/player_history.json"
	}
	if c.Gallery.ImagePath == "" {
		c.Gallery.ImagePath = "screenshot.jpg"
	}
}

func (c *ConfigFile) note(format string, args ...any) {
	c.Notes = append(c.Notes, fmt.Sprintf(format, args...))
}

func (c ConfigFile) RuntimeConfig(in RuntimeConfigInput) (Config, error) {
	if in.BaseDir == "" {
		in.BaseDir = "."
	}
	server := ServerInfo{Host: c.Session.SessionInfo.IP, Port: c.Session.SessionInfo.Port}
	signalingMode, err := configSignalingMode(c.Session.SignalingMode)
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
		UpdateInterval:               time.Duration(c.Session.UpdateInterval) * time.Second,
		HTTPClient:                   in.HTTPClient,
		Log:                          in.Log,
		SuppressSessionUpdateMessage: c.SuppressSessionUpdateMessage,
		FriendSync:                   c.FriendSync.runtime(),
	}
	if c.FriendSync.Expiry.Enabled {
		cfg.FriendHistory = NewFileHistoryStore(resolvePath(in.BaseDir, c.FriendSync.Expiry.HistoryPath))
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

func configSignalingMode(mode string) (SignalingMode, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "jsonrpc", "json-rpc", "messaging":
		return SignalingModeJSONRPC, nil
	case "websocket", "websockets", "ws":
		return "", errors.New("session.signalingMode websocket is not supported for Minecraft friend-list publishing; set session.signalingMode: jsonrpc")
	default:
		return "", fmt.Errorf("unknown session.signalingMode %q", mode)
	}
}

func (f FriendFileConfig) runtime() *FriendSyncConfig {
	if !f.AutoFollow && !f.AutoUnfollow && !f.Expiry.Enabled {
		return nil
	}
	return &FriendSyncConfig{
		UpdateInterval: time.Duration(f.UpdateInterval) * time.Second,
		AutoFollow:     f.AutoFollow,
		AutoUnfollow:   f.AutoUnfollow,
		InitialInvite:  f.InitialInvite,
		ExpiryEnabled:  f.Expiry.Enabled,
		ExpiryDays:     f.Expiry.Days,
		ExpiryCheck:    time.Duration(f.Expiry.Check) * time.Second,
	}
}

func decodeConfig(path string, data []byte, out *ConfigFile) error {
	normalized, err := normalizeConfigData(path, data)
	if err != nil {
		return err
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".toml":
		return toml.Unmarshal(normalized, out)
	default:
		return yaml.Unmarshal(normalized, out)
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

var configKeyAliases = map[string]string{
	"config-version":                  "configVersion",
	"debug-mode":                      "debugMode",
	"debug-log":                       "debugMode",
	"suppress-session-update-message": "suppressSessionUpdateMessage",
	"suppress-session-update-info":    "suppressSessionUpdateMessage",
	"friend-sync":                     "friendSync",
	"remote-address":                  "remoteAddress",
	"remote-port":                     "remotePort",
	"update-interval":                 "updateInterval",
	"signaling-mode":                  "signalingMode",
	"query-server":                    "queryServer",
	"web-query-fallback":              "webQueryFallback",
	"config-fallback":                 "configFallback",
	"broadcast-setting":               "broadcastSetting",
	"world-type":                      "worldType",
	"session-info":                    "sessionInfo",
	"host-name":                       "hostName",
	"world-name":                      "worldName",
	"max-players":                     "maxPlayers",
	"auto-follow":                     "autoFollow",
	"auto-unfollow":                   "autoUnfollow",
	"initial-invite":                  "initialInvite",
	"history-path":                    "historyPath",
	"webhook-url":                     "webhookUrl",
	"image-path":                      "imagePath",
	"delete-other-images":             "deleteOtherImages",
	"primary-cache-path":              "primaryCachePath",
	"sub-accounts":                    "subAccounts",
	"cache-path":                      "cachePath",
}

func normalizeConfigData(path string, data []byte) ([]byte, error) {
	var root map[string]any
	switch strings.ToLower(filepath.Ext(path)) {
	case ".toml":
		if err := toml.Unmarshal(data, &root); err != nil {
			return nil, err
		}
		if root == nil {
			return data, nil
		}
		normalizeConfigMap(root)
		return toml.Marshal(root)
	default:
		if err := yaml.Unmarshal(data, &root); err != nil {
			return nil, err
		}
		if root == nil {
			return data, nil
		}
		normalizeConfigMap(root)
		return yaml.Marshal(root)
	}
}

func normalizeConfigMap(m map[string]any, path ...string) {
	for from, to := range configKeyAliases {
		renameConfigKey(m, from, to)
	}
	if len(path) == 0 {
		moveRootSessionKeys(m)
		migrateLegacySlackWebhook(m)
	}
	if len(path) == 1 && path[0] == "friendSync" {
		moveFriendExpiryKeys(m)
	}
	for key, value := range m {
		normalizeConfigValue(value, append(path, key)...)
	}
}

func normalizeConfigValue(value any, path ...string) {
	switch v := value.(type) {
	case map[string]any:
		normalizeConfigMap(v, path...)
	case []any:
		for _, item := range v {
			normalizeConfigValue(item, path...)
		}
	case []map[string]any:
		for _, item := range v {
			normalizeConfigMap(item, path...)
		}
	}
}

func renameConfigKey(m map[string]any, from, to string) {
	value, ok := m[from]
	if !ok {
		return
	}
	if _, exists := m[to]; !exists {
		m[to] = value
	}
	delete(m, from)
}

func migrateLegacySlackWebhook(root map[string]any) {
	value, ok := root["slack-webhook"]
	if !ok {
		return
	}
	delete(root, "slack-webhook")
	if strings.TrimSpace(fmt.Sprint(value)) == "" {
		return
	}
	notifications, ok := configChildMap(root, "notifications")
	if !ok {
		return
	}
	if _, exists := notifications["webhookUrl"]; !exists {
		notifications["webhookUrl"] = value
	}
	if _, exists := notifications["enabled"]; !exists {
		notifications["enabled"] = true
	}
}

func moveRootSessionKeys(root map[string]any) {
	if !hasAnyConfigKey(root, "remoteAddress", "remotePort", "updateInterval") {
		return
	}
	session, ok := configChildMap(root, "session")
	if !ok {
		return
	}
	moveConfigKey(root, session, "remoteAddress")
	moveConfigKey(root, session, "remotePort")
	moveConfigKey(root, session, "updateInterval")
}

func moveFriendExpiryKeys(friendSync map[string]any) {
	if !hasAnyConfigKey(friendSync, "should-expire", "shouldExpire", "expire-days", "expireDays", "expire-check", "expireCheck") {
		return
	}
	expiry, ok := configChildMap(friendSync, "expiry")
	if !ok {
		return
	}
	moveAliasedConfigKey(friendSync, expiry, "should-expire", "enabled")
	moveAliasedConfigKey(friendSync, expiry, "shouldExpire", "enabled")
	moveAliasedConfigKey(friendSync, expiry, "expire-days", "days")
	moveAliasedConfigKey(friendSync, expiry, "expireDays", "days")
	moveAliasedConfigKey(friendSync, expiry, "expire-check", "check")
	moveAliasedConfigKey(friendSync, expiry, "expireCheck", "check")
}

func hasAnyConfigKey(m map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := m[key]; ok {
			return true
		}
	}
	return false
}

func configChildMap(parent map[string]any, key string) (map[string]any, bool) {
	if child, ok := parent[key].(map[string]any); ok {
		return child, true
	}
	if _, exists := parent[key]; exists {
		return nil, false
	}
	child := map[string]any{}
	parent[key] = child
	return child, true
}

func moveConfigKey(from, to map[string]any, key string) {
	moveAliasedConfigKey(from, to, key, key)
}

func moveAliasedConfigKey(from, to map[string]any, sourceKey, targetKey string) {
	value, ok := from[sourceKey]
	if !ok {
		return
	}
	if _, exists := to[targetKey]; !exists {
		to[targetKey] = value
	}
	delete(from, sourceKey)
}

func resolvePath(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}
