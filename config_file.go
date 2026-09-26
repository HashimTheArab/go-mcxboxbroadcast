package broadcaster

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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

const CurrentConfigVersion = 5

// exampleServerHost is the placeholder target in generated configs; the
// broadcaster refuses to start until an operator replaces it.
const exampleServerHost = "play.example.net"

// ErrConfigCreated reports that LoadConfigFile wrote a default config because
// none existed; the operator must edit it before starting again.
var ErrConfigCreated = errors.New("created a default config")

var errExampleServerHost = errors.New("session.sessionInfo.ip is still the example host " + exampleServerHost)

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
				IP:         exampleServerHost,
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

// LoadConfigFile returns the config at path, migrated to CurrentConfigVersion.
// Unknown keys are errors; a missing file is written with defaults and
// reported as ErrConfigCreated.
func LoadConfigFile(path string) (ConfigFile, error) {
	cfg := DefaultConfigFile()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := SaveConfigFile(path, cfg); err != nil {
			return ConfigFile{}, fmt.Errorf("write default config: %w", err)
		}
		return ConfigFile{}, fmt.Errorf("%w at %s: set session.sessionInfo.ip and port to your Bedrock server, then start again", ErrConfigCreated, path)
	}
	if err != nil {
		return ConfigFile{}, err
	}
	notes, err := decodeConfig(path, data, &cfg)
	if err != nil {
		return ConfigFile{}, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.Notes = append(cfg.Notes, notes...)
	loadedVersion := cfg.ConfigVersion
	cfg.migrate()
	// An unversioned file decodes at the current version, so also save when a step changed it.
	if loadedVersion != cfg.ConfigVersion || len(notes) > 0 {
		if err := SaveConfigFile(path, cfg); err != nil {
			cfg.Notes = append(cfg.Notes, fmt.Sprintf("could not persist migrated config: %v", err))
		}
	}
	if strings.EqualFold(strings.TrimSpace(cfg.Session.SessionInfo.IP), exampleServerHost) {
		return ConfigFile{}, fmt.Errorf("%s: %w; set it to your Bedrock server", path, errExampleServerHost)
	}
	return cfg, nil
}

// SaveConfigFile atomically replaces path with cfg, readable only by its owner.
func SaveConfigFile(path string, cfg ConfigFile) error {
	data, err := encodeConfig(path, cfg)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

// configMigrations[v] rewrites a raw version v-1 document into version v and
// returns one operator note per change; no note means the document is unchanged.
var configMigrations = map[int]func(doc map[string]any) []string{
	4: migrateDropSessionOverrides,
	5: migrateFriendExpiry,
}

// migrateDropSessionOverrides removes keys this broadcaster never honours or
// no longer supports: the advertised version override, which can advertise a
// version the listener rejects, and the Geyser-extension-only target keys.
func migrateDropSessionOverrides(doc map[string]any) []string {
	var notes []string
	protocol := deleteConfigKey(doc, "session", "sessionInfo", "protocol")
	version := deleteConfigKey(doc, "session", "sessionInfo", "version")
	if protocol || version {
		notes = append(notes, "removed session.sessionInfo.protocol/version: sessions now always advertise the version this build accepts")
	}
	for _, key := range []string{"remoteAddress", "remotePort"} {
		if deleteConfigKey(doc, "session", key) {
			notes = append(notes, fmt.Sprintf("removed unused session.%s: the target is session.sessionInfo.ip/port", key))
		}
	}
	return notes
}

// migrateFriendExpiry turns friendSync.expiry into friendSync.cleanup the way
// the old loader read it. maxFriends stays off so existing deployments keep
// their behaviour until they opt in.
func migrateFriendExpiry(doc map[string]any) []string {
	friendSync, _ := doc["friendSync"].(map[string]any)
	if friendSync == nil {
		friendSync = map[string]any{}
		doc["friendSync"] = friendSync
	}
	if _, ok := friendSync["cleanup"]; ok {
		if deleteConfigKey(friendSync, "expiry") {
			return []string{"removed friendSync.expiry: friendSync.cleanup replaces it"}
		}
		return nil
	}
	expiry, _ := friendSync["expiry"].(map[string]any)
	delete(friendSync, "expiry")
	enabled := true
	if v, ok := expiry["enabled"].(bool); ok {
		enabled = v
	}
	days := configInt(expiry["days"], defaultFriendInactiveDays)
	if days <= 0 {
		days = defaultFriendInactiveDays
	}
	historyPath, _ := expiry["historyPath"].(string)
	if historyPath == "" {
		historyPath = defaultFriendHistoryPath
	}
	cleanup := map[string]any{
		"inactiveDays": 0,
		"maxFriends":   0,
		"interval":     configInt(expiry["check"], defaultFriendCleanupInterval),
		"historyPath":  historyPath,
	}
	if enabled {
		cleanup["inactiveDays"] = days
	}
	friendSync["cleanup"] = cleanup
	return []string{fmt.Sprintf("migrated friendSync.expiry to friendSync.cleanup (inactiveDays %d); set friendSync.cleanup.maxFriends to keep room for new friends", cleanup["inactiveDays"])}
}

// configInt reads a whole number decoded from YAML or TOML, or returns def.
func configInt(v any, def int) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case uint64:
		return int(n)
	case float64:
		return int(n)
	default:
		return def
	}
}

// deleteConfigKey removes the value at the nested key path and reports whether it existed.
func deleteConfigKey(doc map[string]any, path ...string) bool {
	for _, key := range path[:len(path)-1] {
		next, ok := doc[key].(map[string]any)
		if !ok {
			return false
		}
		doc = next
	}
	last := path[len(path)-1]
	if _, ok := doc[last]; !ok {
		return false
	}
	delete(doc, last)
	return true
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
	// Friend sync needs history even with cleanup off, to finish earlier removals.
	if cfg.FriendSync != nil {
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

// decodeConfig strictly decodes data into out after applying the raw-document
// migrations newer than its configVersion, returning their notes.
func decodeConfig(path string, data []byte, out *ConfigFile) ([]string, error) {
	codec := configCodecFor(path)
	var doc map[string]any
	if err := codec.unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if len(doc) == 0 {
		return nil, nil
	}
	var probe struct {
		ConfigVersion int `yaml:"configVersion" toml:"configVersion"`
	}
	if err := codec.unmarshal(data, &probe); err != nil {
		return nil, err
	}
	var notes []string
	for v := probe.ConfigVersion + 1; v <= CurrentConfigVersion; v++ {
		if migrate := configMigrations[v]; migrate != nil {
			notes = append(notes, migrate(doc)...)
		}
	}
	if len(notes) > 0 {
		// Re-encode only when a step changed the document so errors keep the
		// operator's line numbers otherwise.
		migrated, err := codec.marshal(doc)
		if err != nil {
			return nil, err
		}
		data = migrated
	}
	return notes, codec.decodeStrict(data, out)
}

type configCodec struct {
	unmarshal    func([]byte, any) error
	marshal      func(any) ([]byte, error)
	decodeStrict func([]byte, *ConfigFile) error
}

func configCodecFor(path string) configCodec {
	if strings.EqualFold(filepath.Ext(path), ".toml") {
		return configCodec{unmarshal: toml.Unmarshal, marshal: toml.Marshal, decodeStrict: decodeTOMLStrict}
	}
	return configCodec{unmarshal: yaml.Unmarshal, marshal: yaml.Marshal, decodeStrict: decodeYAMLStrict}
}

func decodeYAMLStrict(data []byte, out *ConfigFile) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func decodeTOMLStrict(data []byte, out *ConfigFile) error {
	err := toml.NewDecoder(bytes.NewReader(data)).DisallowUnknownFields().Decode(out)
	var strict *toml.StrictMissingError
	if !errors.As(err, &strict) {
		return err
	}
	keys := make([]string, 0, len(strict.Errors))
	for _, e := range strict.Errors {
		keys = append(keys, strings.Join(e.Key(), "."))
	}
	return fmt.Errorf("unknown config keys: %s", strings.Join(keys, ", "))
}

func encodeConfig(path string, cfg ConfigFile) ([]byte, error) {
	return configCodecFor(path).marshal(cfg)
}

func resolvePath(base, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}
