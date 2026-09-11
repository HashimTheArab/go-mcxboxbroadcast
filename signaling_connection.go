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

// normalizeSignalingMode validates mode and applies the WebSocket default.
func normalizeSignalingMode(mode SignalingMode) (SignalingMode, error) {
	switch strings.ToLower(strings.TrimSpace(string(mode))) {
	case "", "websocket", "websockets", "ws":
		return SignalingModeWebSocket, nil
	case "jsonrpc", "json-rpc", "messaging":
		return SignalingModeJSONRPC, nil
	default:
		return "", fmt.Errorf("unknown signaling mode %q", mode)
	}
}

// signalingMode returns the broadcaster's validated signaling mode.
func (b *Broadcaster) signalingMode() (SignalingMode, error) {
	return normalizeSignalingMode(b.conf.SignalingMode)
}

// signalingConnection builds the MPSD connection entry for the active
// signaling transport.
func (b *Broadcaster) signalingConnection(sig nethernet.Signaling) (*p2p.Connection, error) {
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
	if mode == SignalingModeJSONRPC {
		jsonRPCSignaling, ok := sig.(p2p.JSONRPCSignaling)
		if !ok {
			return nil, fmt.Errorf("jsonrpc signaling %T does not expose its player messaging identity", sig)
		}
		connection, err := p2p.NewJSONRPCConnection(jsonRPCSignaling)
		if err != nil {
			return nil, fmt.Errorf("validate jsonrpc signaling connection: %w", err)
		}
		return &connection, nil
	}
	connection := p2p.Connection{Type: p2p.ConnectionTypeSignalingOverWebSocket, NetherNetID: p2p.NetherNetID(networkID)}
	if err := connection.Validate(); err != nil {
		return nil, fmt.Errorf("validate websocket signaling connection: %w", err)
	}
	return &connection, nil
}
