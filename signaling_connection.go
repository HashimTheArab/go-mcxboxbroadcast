package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/df-mc/go-nethernet"
	"github.com/google/uuid"
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

func (b *Broadcaster) signalingMode() (SignalingMode, error) {
	mode := b.conf.SignalingMode
	if mode == "" {
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

func (b *Broadcaster) signalingConnection(ctx context.Context, sig nethernet.Signaling) (*room.Connection, error) {
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
		return &room.Connection{
			ConnectionType: p2p.ConnectionTypeSignalingOverWebSocket,
			NetherNetID:    p2p.NetherNetID(networkID),
		}, nil
	}
	pmsgID, err := b.playerMessagingID(ctx)
	if err != nil {
		return nil, err
	}
	return &room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    p2p.NetherNetID(networkID),
		PmsgID:         pmsgID,
	}, nil
}

func (b *Broadcaster) playerMessagingID(ctx context.Context) (uuid.UUID, error) {
	src, err := b.minecraftTokenSource(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("create minecraft token source for jsonrpc signaling: %w", err)
	}
	tok, err := src.ServiceToken(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("request minecraft token for jsonrpc signaling: %w", err)
	}
	if tok.Claims.PlayerMessagingID == uuid.Nil {
		return uuid.Nil, errors.New("minecraft token player messaging id is empty")
	}
	return tok.Claims.PlayerMessagingID, nil
}
