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
	connection room.Connection
}

func (a signalingConnectionAnnouncer) Announce(ctx context.Context, status room.Status) error {
	// The session document must advertise exactly the shared signaling
	// connection clients can join through; any caller-provided connections are
	// deliberately replaced.
	status.SupportedConnections = []room.Connection{a.connection}
	return a.Announcer.Announce(ctx, status)
}

// signalingMode resolves the configured mode, preserving WebSocket signaling
// for callers that inject their own signaling implementation.
func (b *Broadcaster) signalingMode() (SignalingMode, error) {
	mode := b.conf.SignalingMode
	if mode == "" {
		if b.conf.Signaling == nil && b.conf.SignalingFactory == nil {
			return SignalingModeJSONRPC, nil
		}
		return SignalingModeWebSocket, nil
	}
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case "jsonrpc", "json-rpc", "messaging":
		return SignalingModeJSONRPC, nil
	case "websocket", "websockets", "ws":
		return SignalingModeWebSocket, nil
	default:
		return "", fmt.Errorf("unknown signaling mode %q", mode)
	}
}

// signalingConnection builds the discriminator-aware MPSD connection entry
// for the active signaling transport.
func (b *Broadcaster) signalingConnection(sig nethernet.Signaling) (*room.Connection, error) {
	mode, err := b.signalingMode()
	if err != nil {
		return nil, err
	}
	if sig == nil {
		return nil, errors.New("signaling connection: signaling is nil")
	}
	networkID := sig.NetworkID()
	if strings.TrimSpace(networkID) == "" {
		return nil, errors.New("signaling connection: nethernet id is empty")
	}
	if mode == SignalingModeWebSocket {
		connection := p2p.Connection{
			Type:        p2p.ConnectionTypeSignalingOverWebSocket,
			NetherNetID: p2p.NetherNetID(networkID),
		}
		if err := connection.Validate(); err != nil {
			return nil, fmt.Errorf("validate websocket signaling connection: %w", err)
		}
		return &room.Connection{
			ConnectionType: connection.Type,
			NetherNetID:    connection.NetherNetID,
		}, nil
	}
	jsonRPCSignaling, ok := sig.(p2p.JSONRPCSignaling)
	if !ok {
		return nil, fmt.Errorf("jsonrpc signaling %T does not expose its player messaging identity", sig)
	}
	connection, err := p2p.NewJSONRPCConnection(jsonRPCSignaling)
	if err != nil {
		return nil, fmt.Errorf("validate jsonrpc signaling connection: %w", err)
	}
	return &room.Connection{
		ConnectionType: connection.Type,
		NetherNetID:    connection.NetherNetID,
		PmsgID:         connection.PlayerMessagingID,
	}, nil
}
