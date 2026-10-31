package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

// Config holds the dependencies and settings for a Broadcaster.
type Config struct {
	// XBLClient is the account's Xbox Live client for MPSD, social, and
	// presence calls.
	XBLClient *xsapi.Client
	// XBLTokenSource supplies Xbox Live tokens when XBLClient is created lazily.
	XBLTokenSource xsapi.TokenSource
	// XUID is the account's Xbox user ID. If empty, it is read from XBLClient
	// when available.
	XUID string
	// MinecraftTokenSource supplies Minecraft franchise service tokens for
	// signaling and gallery/profile image requests.
	MinecraftTokenSource service.TokenSource

	// JoinURL is the base URL of the target server's NetherNet HTTP signaling,
	// such as https://play.example.net:19132. Join offers are relayed to its
	// /v1/join endpoint and status queries are sent to it.
	JoinURL string

	// SessionName is the MPSD session name. If empty, a random UUID is used.
	SessionName string
	// Status is the initial status announced to Xbox Live. Zero fields fall
	// back to defaults.
	Status Status
	// StatusProvider may dynamically provide room status for announcements.
	StatusProvider room.StatusProvider
	// Gallery controls optional Minecraft profile showcase image upload.
	Gallery *GalleryConfig
	// Notifier receives operator-facing notifications.
	Notifier Notifier
	// SuppressSessionUpdateMessage logs each successful session update at
	// debug level instead of info.
	SuppressSessionUpdateMessage bool
	// FriendSync controls optional friend synchronization.
	FriendSync *FriendSyncConfig
	// FriendHistory records player activity for friend expiry.
	FriendHistory HistoryStore
	// Signaling is the Xbox JSON-RPC signaling connection that receives join
	// offers; it must implement p2p.JSONRPCSignaling. If nil, SignalingFactory is called.
	Signaling nethernet.Signaling
	// SignalingFactory creates the NetherNet signaling connection.
	SignalingFactory SignalingFactory

	// PublishConfig customizes MPSD publication.
	PublishConfig mpsd.PublishConfig
	// HideOldSessions makes rotated-out sessions invite-only once their grace
	// period ends, so their members stop advertising the world to friends.
	HideOldSessions bool

	// UpdateInterval controls the background session refresh interval. Zero
	// uses 30 seconds; values below 20 seconds are raised to avoid Xbox rate limits.
	UpdateInterval time.Duration
	// HTTPClient is used by auth/signaling/session requests where supported.
	HTTPClient *http.Client
	// SignalingDialTimeout bounds default signaling startup. If zero, a
	// 15-second timeout is used. Injected Signaling and SignalingFactory values
	// receive the caller's context unchanged.
	SignalingDialTimeout time.Duration
	// Log receives diagnostic events. If nil, slog.Default() is used.
	Log *slog.Logger
}

type SignalingFactory func(ctx context.Context, conf Config) (nethernet.Signaling, error)

const (
	defaultUpdateInterval = 30 * time.Second
	// minUpdateInterval spaces session updates and friend sync passes; Xbox rate-limits faster ones.
	minUpdateInterval       = 20 * time.Second
	defaultFriendExpiryDays = 15
)

// validateJoinURL accepts the base URLs the endpoint client can dial: http or
// https with an explicit port and no path.
func validateJoinURL(raw string) error {
	if raw == "" {
		return errors.New("join URL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse join URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.Port() == "" || u.Path != "" || u.RawQuery != "" {
		return fmt.Errorf("join URL %q must be http(s)://host:port", raw)
	}
	return nil
}

// Status is the server metadata shown in the Xbox friend-list world.
type Status struct {
	HostName         string
	WorldName        string
	WorldType        string
	Players          int
	MaxPlayers       int
	Broadcast        int32
	LevelID          string
	QueryTarget      bool
	QueryTimeout     time.Duration
	WebQueryFallback bool
	QueryFallback    bool
	WebQueryClient   *http.Client
	// Version and Protocol override the advertised pair together; zero values
	// advertise the protocol library's current pair.
	Version  string
	Protocol int32
}

func (s Status) validate() error {
	if (s.Version == "") != (s.Protocol == 0) {
		return errors.New("advertised version and protocol must be set together")
	}
	return nil
}

type BroadcastSetting int32

const (
	BroadcastSettingInviteOnly BroadcastSetting = iota + 1
	BroadcastSettingFriendsOnly
	BroadcastSettingFriendsOfFriends
)

const (
	WorldTypeCreative  = room.WorldTypeCreative
	WorldTypeSurvival  = "Survival"
	WorldTypeAdventure = "Adventure"
)

type GalleryConfig struct {
	Enabled           bool
	ImagePath         string
	DeleteOtherImages bool
	TokenSource       service.TokenSource
	Client            *http.Client
}

type Notifier interface {
	Notify(ctx context.Context, message string) error
}

type FriendSyncConfig struct {
	UpdateInterval time.Duration
	// AutoFriend accepts incoming friend requests.
	AutoFriend    bool
	InitialInvite bool
	ExpiryEnabled bool
	ExpiryDays    int
	ExpiryCheck   time.Duration
}
