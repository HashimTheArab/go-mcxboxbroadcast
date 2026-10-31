package broadcaster

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/df-mc/go-xsapi/v2"
	"github.com/pelletier/go-toml/v2"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"gopkg.in/yaml.v3"
)

const CurrentConfigVersion = 7

// exampleServerHost is the placeholder target in generated configs; the
// broadcaster refuses to start until an operator replaces it.
const exampleServerHost = "play.example.net"

const defaultServerPort = 19132

// ErrConfigCreated reports that LoadConfigFile wrote a default config because
// none existed; the operator must edit it before starting again.
var ErrConfigCreated = errors.New("created a default config")

var errExampleServerHost = errors.New("session.joinUrl still points at the example host " + exampleServerHost)

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
	Metrics                      MetricsFileConfig  `yaml:"metrics" toml:"metrics"`

	// Notes lists adjustments applied while loading, such as out-of-range
	// values that were clamped. Callers should surface them as warnings.
	Notes []string `yaml:"-" toml:"-"`
}

// MetricsFileConfig serves Prometheus metrics on Listen (host:port); empty disables them.
type MetricsFileConfig struct {
	Listen string `yaml:"listen" toml:"listen"`
}

type HTTPFileConfig struct {
	Proxy string `yaml:"proxy" toml:"proxy"`
}

// SessionFileConfig mirrors MCXboxBroadcast's standalone session settings.
type SessionFileConfig struct {
	// JoinURL is the target's NetherNet HTTP signaling base URL.
	JoinURL          string          `yaml:"joinUrl" toml:"joinUrl"`
	UpdateInterval   int             `yaml:"updateInterval" toml:"updateInterval"`
	QueryServer      bool            `yaml:"queryServer" toml:"queryServer"`
	WebQueryFallback bool            `yaml:"webQueryFallback" toml:"webQueryFallback"`
	ConfigFallback   bool            `yaml:"configFallback" toml:"configFallback"`
	BroadcastSetting int32           `yaml:"broadcastSetting" toml:"broadcastSetting"`
	WorldType        string          `yaml:"worldType" toml:"worldType"`
	HideOldSessions  bool            `yaml:"hideOldSessions" toml:"hideOldSessions"`
	SessionInfo      SessionInfoFile `yaml:"sessionInfo" toml:"sessionInfo"`
}

type SessionInfoFile struct {
	HostName   string `yaml:"hostName" toml:"hostName"`
	WorldName  string `yaml:"worldName" toml:"worldName"`
	Players    int    `yaml:"players" toml:"players"`
	MaxPlayers int    `yaml:"maxPlayers" toml:"maxPlayers"`
	// Version and Protocol override the advertised pair together.
	Version  string `yaml:"version,omitempty" toml:"version,omitempty"`
	Protocol int32  `yaml:"protocol,omitempty" toml:"protocol,omitempty"`
}

type FriendFileConfig struct {
	UpdateInterval int              `yaml:"updateInterval" toml:"updateInterval"`
	AutoFriend     bool             `yaml:"autoFriend" toml:"autoFriend"`
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

// Notifier returns the configured webhook notifier, or nil when notifications are off.
func (n NotificationConfig) Notifier(client *http.Client) Notifier {
	if !n.Enabled {
		return nil
	}
	return SlackNotifier{WebhookURL: n.WebhookURL, Client: client}
}

type GalleryFileConfig struct {
	Enabled           bool   `yaml:"enabled" toml:"enabled"`
	ImagePath         string `yaml:"imagePath" toml:"imagePath"`
	DeleteOtherImages bool   `yaml:"deleteOtherImages" toml:"deleteOtherImages"`
}

type AccountsConfig struct {
	PrimaryCachePath string `yaml:"primaryCachePath" toml:"primaryCachePath"`
}

// RuntimeConfigInput carries what a Config needs beyond the file; BaseDir resolves relative paths.
type RuntimeConfigInput struct {
	XBLClient  *xsapi.Client
	HTTPClient *http.Client
	Log        *slog.Logger
	BaseDir    string
}

func DefaultConfigFile() ConfigFile {
	return ConfigFile{
		ConfigVersion: CurrentConfigVersion,
		DebugMode:     false,
		Session: SessionFileConfig{
			JoinURL:          "https://" + exampleServerHost + ":19132",
			UpdateInterval:   int(defaultUpdateInterval / time.Second),
			QueryServer:      true,
			WebQueryFallback: false,
			ConfigFallback:   false,
			BroadcastSetting: int32(p2p.BroadcastSettingFriendsOfFriends),
			WorldType:        WorldTypeSurvival,
			HideOldSessions:  true,
			SessionInfo: SessionInfoFile{
				HostName:   "Minecraft Server",
				WorldName:  "Minecraft World",
				Players:    0,
				MaxPlayers: 20,
			},
		},
		FriendSync: FriendFileConfig{
			UpdateInterval: 60,
			AutoFriend:     true,
			InitialInvite:  true,
			Expiry: FriendExpiryFile{
				Enabled:     true,
				Days:        defaultFriendExpiryDays,
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
		return ConfigFile{}, fmt.Errorf("%w at %s: set session.joinUrl to your server's NetherNet HTTP signaling URL, then start again", ErrConfigCreated, path)
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
	if loadedVersion != cfg.ConfigVersion {
		if err := SaveConfigFile(path, cfg); err != nil {
			cfg.Notes = append(cfg.Notes, fmt.Sprintf("could not persist migrated config: %v", err))
		}
	}
	if u, err := url.Parse(cfg.Session.JoinURL); err == nil && strings.EqualFold(u.Hostname(), exampleServerHost) {
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
	5: migrateHTTPSignalingRelay,
	6: migrateDropSignalingMode,
	7: migrateDropSubAccounts,
}

// migrateDropSessionOverrides removes the version 3 advertised-version
// override, which predates the target it must match, and the
// Geyser-extension-only target keys.
func migrateDropSessionOverrides(doc map[string]any) []string {
	var notes []string
	protocol := deleteConfigKey(doc, "session", "sessionInfo", "protocol")
	version := deleteConfigKey(doc, "session", "sessionInfo", "version")
	if protocol || version {
		notes = append(notes, "removed session.sessionInfo.protocol/version from the version 3 config; set both again to advertise another pair")
	}
	for _, key := range []string{"remoteAddress", "remotePort"} {
		if deleteConfigKey(doc, "session", key) {
			notes = append(notes, fmt.Sprintf("removed unused session.%s", key))
		}
	}
	return notes
}

// migrateHTTPSignalingRelay moves the target to session.joinUrl, drops the
// listener and relay settings, and keeps friend sync friends-only like Java.
func migrateHTTPSignalingRelay(doc map[string]any) []string {
	var notes []string
	if session, ok := doc["session"].(map[string]any); ok {
		if info, ok := session["sessionInfo"].(map[string]any); ok {
			if host, ok := info["ip"].(string); ok && strings.TrimSpace(host) != "" {
				port := defaultServerPort
				if p, ok := configInt(info["port"]); ok && p > 0 {
					port = p
				}
				// A panel may already have written joinUrl into the old file; keep it.
				if existing, _ := session["joinUrl"].(string); strings.TrimSpace(existing) != "" {
					notes = append(notes, "removed session.sessionInfo.ip/port: session.joinUrl is already set")
				} else {
					session["joinUrl"] = "https://" + net.JoinHostPort(strings.TrimSpace(host), strconv.Itoa(port))
					notes = append(notes, fmt.Sprintf("moved session.sessionInfo.ip/port to session.joinUrl %v; change the scheme if the server only serves http", session["joinUrl"]))
				}
			}
			delete(info, "ip")
			delete(info, "port")
		}
	}
	if deleteConfigKey(doc, "session", "icePortRange") {
		notes = append(notes, "removed session.icePortRange: the broadcaster no longer accepts game connections")
	}
	if _, ok := doc["relay"]; ok {
		delete(doc, "relay")
		notes = append(notes, "removed relay: every join is relayed through HTTP signaling now")
	}
	if friends, ok := doc["friendSync"].(map[string]any); ok {
		if v, ok := friends["autoFollow"]; ok {
			friends["autoFriend"] = v
			delete(friends, "autoFollow")
			notes = append(notes, "renamed friendSync.autoFollow to autoFriend")
		}
		if deleteConfigKey(doc, "friendSync", "autoUnfollow") {
			notes = append(notes, "removed friendSync.autoUnfollow: friend sync only manages friends now")
		}
	}
	return notes
}

// migrateDropSignalingMode removes session.signalingMode: JSON-RPC is the only
// signaling vanilla clients join through.
func migrateDropSignalingMode(doc map[string]any) []string {
	if deleteConfigKey(doc, "session", "signalingMode") {
		return []string{"removed session.signalingMode: sessions always use JSON-RPC signaling now"}
	}
	return nil
}

// migrateDropSubAccounts removes accounts.subAccounts. Configs saved at
// version 6 still carry the key, so this cannot be part of that step.
func migrateDropSubAccounts(doc map[string]any) []string {
	accounts, ok := doc["accounts"].(map[string]any)
	if !ok {
		return nil
	}
	subAccounts, ok := accounts["subAccounts"]
	if !ok {
		return nil
	}
	delete(accounts, "subAccounts")
	var enabled []string
	if list, ok := subAccounts.([]any); ok {
		for _, entry := range list {
			if account, ok := entry.(map[string]any); ok && account["enabled"] == true {
				id, _ := account["id"].(string)
				enabled = append(enabled, strconv.Quote(id))
			}
		}
	}
	if len(enabled) == 0 {
		return []string{"removed accounts.subAccounts: sub-accounts are no longer supported"}
	}
	return []string{fmt.Sprintf("removed accounts.subAccounts: sub-accounts are no longer supported, so enabled sub-accounts %s are not broadcast; only the primary account is", strings.Join(enabled, ", "))}
}

// configInt reads an integer decoded by either the YAML or TOML codec.
func configInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case uint64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
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

// migrate brings the version current and replaces out-of-range values with the minimum or default.
func (c *ConfigFile) migrate() {
	c.ConfigVersion = max(c.ConfigVersion, CurrentConfigVersion)
	minInterval := int(minUpdateInterval / time.Second)
	if c.Session.UpdateInterval < minInterval {
		c.note("session.updateInterval %d is below the %d second minimum; using %[2]d", c.Session.UpdateInterval, minInterval)
		c.Session.UpdateInterval = minInterval
	}
	if c.FriendSync.UpdateInterval < minInterval {
		c.note("friendSync.updateInterval %d is below the %d second minimum; using %[2]d", c.FriendSync.UpdateInterval, minInterval)
		c.FriendSync.UpdateInterval = minInterval
	}
	def := DefaultConfigFile()
	if c.FriendSync.Expiry.Days <= 0 {
		c.note("friendSync.expiry.days %d is invalid; using %d", c.FriendSync.Expiry.Days, def.FriendSync.Expiry.Days)
		c.FriendSync.Expiry.Days = def.FriendSync.Expiry.Days
	}
	if c.FriendSync.Expiry.Check <= 0 {
		c.note("friendSync.expiry.check %d is invalid; using %d", c.FriendSync.Expiry.Check, def.FriendSync.Expiry.Check)
		c.FriendSync.Expiry.Check = def.FriendSync.Expiry.Check
	}
	if c.FriendSync.Expiry.HistoryPath == "" {
		c.FriendSync.Expiry.HistoryPath = def.FriendSync.Expiry.HistoryPath
	}
	if c.Gallery.ImagePath == "" {
		c.Gallery.ImagePath = def.Gallery.ImagePath
	}
}

func (c *ConfigFile) note(format string, args ...any) {
	c.Notes = append(c.Notes, fmt.Sprintf(format, args...))
}

// RuntimeConfig returns the broadcaster Config the file describes.
func (c ConfigFile) RuntimeConfig(in RuntimeConfigInput) Config {
	if in.BaseDir == "" {
		in.BaseDir = "."
	}
	cfg := Config{
		XBLClient:       in.XBLClient,
		JoinURL:         strings.TrimSuffix(c.Session.JoinURL, "/"),
		HideOldSessions: c.Session.HideOldSessions,
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
			Version:          c.Session.SessionInfo.Version,
			Protocol:         c.Session.SessionInfo.Protocol,
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
			ImagePath:         resolvePath(in.BaseDir, c.Gallery.ImagePath),
			DeleteOtherImages: c.Gallery.DeleteOtherImages,
		}
	}
	cfg.Notifier = c.Notifications.Notifier(in.HTTPClient)
	return cfg
}

func (f FriendFileConfig) runtime() *FriendSyncConfig {
	if !f.AutoFriend && !f.Expiry.Enabled {
		return nil
	}
	return &FriendSyncConfig{
		UpdateInterval: time.Duration(f.UpdateInterval) * time.Second,
		AutoFriend:     f.AutoFriend,
		InitialInvite:  f.InitialInvite,
		ExpiryEnabled:  f.Expiry.Enabled,
		ExpiryDays:     f.Expiry.Days,
		ExpiryCheck:    time.Duration(f.Expiry.Check) * time.Second,
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
