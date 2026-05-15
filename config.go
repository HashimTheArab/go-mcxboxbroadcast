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
	"github.com/df-mc/go-xsapi"
	"github.com/df-mc/go-xsapi/mpsd"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"golang.org/x/oauth2"
)

// Config holds the dependencies and settings for a Broadcaster.
type Config struct {
	// TokenSource supplies Xbox Live tokens for MPSD/RTA.
	TokenSource xsapi.TokenSource
	// LiveTokenSource supplies Microsoft Live tokens for Minecraft service
	// signaling. It is used by the gophertunnel lunar signaling implementation.
	LiveTokenSource oauth2.TokenSource

	// Server is the target Bedrock server clients are transferred to.
	Server ServerInfo

	// SessionName is the MPSD session name. If empty, a random UUID is used.
	SessionName string
	// Status is the initial status announced to Xbox Live. Zero fields are
	// filled from Server and protocol defaults.
	Status Status
	// StatusProvider may dynamically provide room status for announcements.
	StatusProvider room.StatusProvider

	// Signaling is the NetherNet signaling connection used to accept clients.
	// If nil, SignalingFactory is called.
	Signaling nethernet.Signaling
	// SignalingFactory creates the NetherNet signaling connection.
	SignalingFactory SignalingFactory

	// ListenConfig customizes the gophertunnel listener.
	ListenConfig minecraft.ListenConfig
	// NetherNetListenConfig customizes WebRTC/NetherNet listener negotiation.
	NetherNetListenConfig nethernet.ListenConfig
	// PublishConfig customizes MPSD publication.
	PublishConfig mpsd.PublishConfig

	// UpdateInterval controls the background session refresh interval. Values
	// lower than 20 seconds are raised to 20 seconds to avoid Xbox rate limits.
	UpdateInterval time.Duration
	// HTTPClient is used by auth/signaling/session requests where supported.
	HTTPClient *http.Client
	// Log receives diagnostic events. If nil, slog.Default() is used.
	Log *slog.Logger
}

type SignalingFactory func(ctx context.Context, conf Config) (nethernet.Signaling, error)

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
	HostName      string
	WorldName     string
	WorldType     string
	Players       int
	MaxPlayers    int
	Broadcast     int32
	Joinability   string
	LevelID       string
	QueryTarget   bool
	QueryTimeout  time.Duration
	QueryFallback bool
}
