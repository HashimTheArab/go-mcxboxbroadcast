package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/df-mc/go-nethernet"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

type signalingConnectionAnnouncer struct {
	room.Announcer
	connection p2p.Connection
}

func (a signalingConnectionAnnouncer) Announce(ctx context.Context, status room.Status) error {
	// The session document must advertise exactly the shared signaling
	// connection clients can join through; any caller-provided connections are
	// deliberately replaced.
	status.SupportedConnections = []p2p.Connection{a.connection}
	return a.Announcer.Announce(ctx, status)
}

// signalingConnection builds the direct WebSocket MPSD connection entry for
// the active signaling transport.
func (b *Broadcaster) signalingConnection(sig nethernet.Signaling) (*p2p.Connection, error) {
	if sig == nil {
		return nil, errors.New("signaling connection: signaling is nil")
	}
	networkID := sig.NetworkID()
	if strings.TrimSpace(networkID) == "" {
		return nil, errors.New("signaling connection: nethernet id is empty")
	}
	connection := p2p.Connection{
		Type:        p2p.ConnectionTypeSignalingOverWebSocket,
		NetherNetID: p2p.NetherNetID(networkID),
	}
	if err := connection.Validate(); err != nil {
		return nil, fmt.Errorf("validate websocket signaling connection: %w", err)
	}
	return &connection, nil
}
