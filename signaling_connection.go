package broadcaster

import (
	"context"
	"errors"
	"strings"

	"github.com/df-mc/go-nethernet"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

type signalingConnectionAnnouncer struct {
	room.Announcer
	connection room.Connection
}

func (a signalingConnectionAnnouncer) Announce(ctx context.Context, status room.Status) error {
	// The session document must advertise exactly the shared signaling
	// connection clients can join through; any caller-provided connections are
	// deliberately replaced.
	status.SupportedConnections = []room.Connection{a.connection}
	return a.Announcer.Announce(ctx, status)
}

func (b *Broadcaster) signalingConnection(sig nethernet.Signaling) (*room.Connection, error) {
	if sig == nil {
		return nil, errors.New("signaling connection: signaling is nil")
	}
	networkID := sig.NetworkID()
	if strings.TrimSpace(networkID) == "" {
		return nil, errors.New("signaling connection: nethernet id is empty")
	}
	return &room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverWebSocket,
		NetherNetID:    p2p.NetherNetID(networkID),
	}, nil
}
