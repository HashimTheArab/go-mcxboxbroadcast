package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

// Config holds the dependencies and settings for a Broadcaster.
type Config struct {
	// XBLClient supplies authenticated Xbox Live clients for MPSD, social, and
	// presence calls.
	XBLClient *xsapi.Client
	// XBLTokenSource supplies Xbox Live tokens when XBLClient is created lazily.
	XBLTokenSource xsapi.TokenSource
	// XUID is the primary account's Xbox user ID. If empty, it is read from
	// XBLClient when available.
	XUID string
	// MinecraftTokenSource supplies Minecraft franchise service tokens for
	// signaling and gallery/profile image requests.
	MinecraftTokenSource service.TokenSource

	// Server is the target Bedrock server clients are transferred to.
	Server ServerInfo

	// SessionName is the MPSD session name. If empty, a random UUID is used.
	SessionName string
	// Status is the initial status announced to Xbox Live. Zero fields are
	// filled from Server and protocol defaults.
	Status Status
	// StatusProvider may dynamically provide room status for announcements.
	StatusProvider room.StatusProvider
	// Gallery controls optional Minecraft profile showcase image upload.
	Gallery *GalleryConfig
	// Notifier receives operator-facing notifications.
	Notifier Notifier
	// SuppressSessionUpdateMessage suppresses operator-facing session update
	// notifications.
	SuppressSessionUpdateMessage bool
	// FriendSync controls optional follower/friend synchronization.
	FriendSync *FriendSyncConfig
	// FriendHistory records player activity for friend expiry.
	FriendHistory HistoryStore
	// SubAccounts contains additional accounts that join/publish the same MPSD
	// session to extend friend-list visibility.
	SubAccounts []SubAccountConfig

	// Signaling is the NetherNet signaling connection used to accept clients.
	// If nil, SignalingFactory is called.
	Signaling nethernet.Signaling
	// SignalingFactory creates the NetherNet signaling connection.
	SignalingFactory SignalingFactory
	// SignalingMode controls the default NetherNet signaling transport. Empty
	// uses JSON-RPC messaging for normal Live-token based signaling and
	// preserves websocket signaling for injected Signaling/SignalingFactory.
	SignalingMode SignalingMode

	// ListenConfig customizes the gophertunnel listener.
	ListenConfig minecraft.ListenConfig
	// NetherNetListenConfig customizes WebRTC/NetherNet listener negotiation.
	NetherNetListenConfig nethernet.ListenConfig
	// PublishConfig customizes MPSD publication.
	PublishConfig mpsd.PublishConfig

	// UpdateInterval controls the background session refresh interval. Values
	// lower than 20 seconds are raised to 20 seconds to avoid Xbox rate limits.
	UpdateInterval time.Duration
	// TransferCloseTimeout bounds how long transferred clients are held open
	// waiting for their disconnect. Zero uses a 15-second default; negative
	// values disable the wait.
	TransferCloseTimeout time.Duration
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

type SignalingMode string

const (
	SignalingModeJSONRPC   SignalingMode = "jsonrpc"
	SignalingModeWebSocket SignalingMode = "websocket"
)

type ServerInfo struct {
	Host string
	Port uint16
}

func (s ServerInfo) Address() string {
	return net.JoinHostPort(s.Host, fmt.Sprint(s.Port))
}

func (s ServerInfo) validate() error {
	if s.Host == "" {
		return errors.New("server host is required")
	}
	if s.Port == 0 {
		return errors.New("server port is required")
	}
	return nil
}

// Status is the server metadata shown in the Xbox friend-list world.
type Status struct {
	HostName  string
	WorldName string
	WorldType string
	// Version is the game version advertised in the session document.
	// Clients hide friend worlds whose version is older than their own, so
	// this may need to lead the compiled-in protocol.CurrentVersion when a
	// client update ships before the protocol library catches up.
	Version          string
	Players          int
	MaxPlayers       int
	Broadcast        int32
	LevelID          string
	QueryTarget      bool
	QueryTimeout     time.Duration
	WebQueryFallback bool
	QueryFallback    bool
	WebQueryClient   *http.Client
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
	AutoFollow     bool
	AutoUnfollow   bool
	InitialInvite  bool
	ExpiryEnabled  bool
	ExpiryDays     int
	ExpiryCheck    time.Duration
}

type SubAccountConfig struct {
	ID             string
	Enabled        bool
	XBLClient      *xsapi.Client
	XBLTokenSource xsapi.TokenSource
	XUID           string
	PublishConfig  mpsd.PublishConfig
	// FriendSync overrides the primary account's friend sync configuration for
	// this sub-account. If nil, the primary configuration is used.
	FriendSync *FriendSyncConfig
}
