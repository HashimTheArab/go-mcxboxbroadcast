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

// signalingConnection builds the MPSD connection entry that advertises sig's
// JSON-RPC identity.
func (b *Broadcaster) signalingConnection(sig nethernet.Signaling) (*p2p.Connection, error) {
	if sig == nil {
		return nil, errors.New("signaling connection: signaling is nil")
	}
	if strings.TrimSpace(sig.NetworkID()) == "" {
		return nil, errors.New("signaling connection: nethernet id is empty")
	}
	jsonRPCSignaling, ok := sig.(p2p.JSONRPCSignaling)
	if !ok {
		return nil, fmt.Errorf("signaling %T does not expose its player messaging identity", sig)
	}
	connection, err := p2p.NewJSONRPCConnection(jsonRPCSignaling)
	if err != nil {
		return nil, fmt.Errorf("validate signaling connection: %w", err)
	}
	return &connection, nil
}
