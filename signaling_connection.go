package broadcaster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/df-mc/go-nethernet"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

type signalingConnectionAnnouncer struct {
	room.Announcer
	connection room.Connection
}

func (a signalingConnectionAnnouncer) Announce(ctx context.Context, status room.Status) error {
	// TODO: Remove this override once we use lac's p2p support for publishing
	// JSON-RPC NetherNet connection metadata directly.
	status.SupportedConnections = []room.Connection{a.connection}
	return a.Announcer.Announce(ctx, status)
}

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

func (b *Broadcaster) signalingConnection(ctx context.Context, sig nethernet.Signaling) (*room.Connection, error) {
	mode, err := b.signalingMode()
	if err != nil {
		return nil, err
	}
	if mode != SignalingModeJSONRPC {
		return nil, nil
	}
	if sig == nil {
		return nil, errors.New("jsonrpc signaling connection: signaling is nil")
	}
	networkID := sig.NetworkID()
	if _, err := strconv.ParseUint(networkID, 10, 64); err != nil {
		return nil, fmt.Errorf("jsonrpc signaling connection: invalid nethernet id %q: %w", networkID, err)
	}
	pmsgID, err := b.playerMessagingID(ctx)
	if err != nil {
		return nil, err
	}
	return &room.Connection{
		ConnectionType: room.ConnectionTypeJSONRPCSignaling,
		NetherNetID:    room.NetherNetID(networkID),
		PmsgID:         pmsgID,
	}, nil
}

func (b *Broadcaster) playerMessagingID(ctx context.Context) (uuid.UUID, error) {
	src := b.conf.MinecraftTokenSource
	if src == nil {
		if b.conf.LiveTokenSource == nil {
			return uuid.Nil, errors.New("jsonrpc signaling requires a minecraft token source or live token source")
		}
		tokens, err := NewMinecraftTokenSource(ctx, b.conf.LiveTokenSource, b.conf.HTTPClient)
		if err != nil {
			return uuid.Nil, fmt.Errorf("create minecraft token source for jsonrpc signaling: %w", err)
		}
		src = tokens
	}
	tok, err := src.Token()
	if err != nil {
		return uuid.Nil, fmt.Errorf("request minecraft token for jsonrpc signaling: %w", err)
	}
	pmsgID, err := playerMessagingIDFromAuthorizationHeader(tok.AuthorizationHeader)
	if err != nil {
		return uuid.Nil, fmt.Errorf("read minecraft player messaging id: %w", err)
	}
	return pmsgID, nil
}

func playerMessagingIDFromAuthorizationHeader(header string) (uuid.UUID, error) {
	token := strings.TrimSpace(header)
	if token == "" {
		return uuid.Nil, errors.New("authorization header is empty")
	}
	fields := strings.Fields(token)
	if len(fields) > 0 {
		token = fields[len(fields)-1]
	}
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return uuid.Nil, errors.New("authorization header does not contain a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode jwt payload: %w", err)
	}
	var claims struct {
		PlayerMessagingID uuid.UUID `json:"pmid"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return uuid.Nil, fmt.Errorf("decode jwt claims: %w", err)
	}
	if claims.PlayerMessagingID == uuid.Nil {
		return uuid.Nil, errors.New("pmid claim is empty")
	}
	return claims.PlayerMessagingID, nil
}
