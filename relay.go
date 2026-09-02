package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"sync"
	"time"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
	"github.com/sandertv/gophertunnel/minecraft/text"
)

const defaultRelayDialTimeout = 15 * time.Second

// RelayConfig keeps joined clients inside the NetherNet session and relays
// their traffic to the backend server instead of transferring them. A relayed
// player stays a member of the Xbox session for as long as they play, which is
// what lets friends of that player discover the world. Xbox Live's session
// member limit therefore bounds concurrent relayed players.
//
// The backend sees the relay's address and an unsigned login chain that still
// carries the player's verified XUID, so it must trust this relay: Geyser with
// validate-bedrock-login off, BDS with online-mode off, or a gophertunnel
// listener with AuthenticationDisabled. Public servers that verify chains
// cannot be relayed to.
type RelayConfig struct {
	// ResolveTarget picks the backend address for a client. Nil relays every
	// client to Config.Server.
	ResolveTarget func(ctx context.Context, identity login.IdentityData, client login.ClientData) (string, error)
	// Network is the gophertunnel network used to dial the backend. Empty uses "raknet".
	Network string
	// Dialer customizes the backend dial. It must not authenticate: identity,
	// client data, protocol, and passthrough settings are set per client.
	Dialer minecraft.Dialer
	// DialTimeout bounds target resolution plus the backend dial. Zero uses 15s.
	DialTimeout time.Duration
}

func (c *RelayConfig) validate() error {
	if c == nil {
		return nil
	}
	if c.Dialer.TokenSource != nil || c.Dialer.XBLClient != nil || c.Dialer.PlayFabClient != nil {
		return errors.New("relay dialer must not authenticate; the relay logs in with each client's identity")
	}
	return nil
}

func (c *RelayConfig) dialTimeout() time.Duration {
	if c.DialTimeout <= 0 {
		return defaultRelayDialTimeout
	}
	return c.DialTimeout
}

func (c *RelayConfig) network() string {
	if c.Network == "" {
		return "raknet"
	}
	return c.Network
}

// relayClientConn is the accepted client surface the relay needs; *minecraft.Conn satisfies it.
type relayClientConn interface {
	transferConn
	relayServerConn
	ClientData() login.ClientData
	Proto() minecraft.Protocol
}

// relayServerConn is the batch surface relayed between the two legs.
type relayServerConn interface {
	ReadBatch() ([]packet.Packet, error)
	WritePacket(packet.Packet) error
	Flush() error
	Close() error
	// SetGameData teaches a passthrough conn the item table, which the codec
	// needs to frame shield items correctly.
	SetGameData(minecraft.GameData)
}

// relayDialFunc dials the backend for one client. The broadcaster's default uses d.DialContext.
type relayDialFunc func(ctx context.Context, d minecraft.Dialer, network, address string) (relayServerConn, error)

// relaySet tracks the clients being relayed so session health can tell their
// live memberships from stale ones.
type relaySet struct {
	mu      sync.Mutex
	clients map[relayClientConn]string // conn -> XUID
}

func (s *relaySet) add(conn relayClientConn, xuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clients == nil {
		s.clients = make(map[relayClientConn]string)
	}
	s.clients[conn] = xuid
}

func (s *relaySet) remove(conn relayClientConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, conn)
}

func (s *relaySet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

// xuids returns the set of XUIDs currently being relayed.
func (s *relaySet) xuids() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	xuids := make(map[string]struct{}, len(s.clients))
	for _, xuid := range s.clients {
		if xuid != "" {
			xuids[xuid] = struct{}{}
		}
	}
	return xuids
}

// staleSessionMembers counts session members that are not being relayed. In
// transfer mode that is every member; relayed members are live players, not
// leftovers that block joiners.
func (b *Broadcaster) staleSessionMembers(members iter.Seq2[string, mpsd.MemberDescription]) int {
	relayed := b.relays.xuids()
	count := 0
	for _, member := range members {
		if member.Constants != nil && member.Constants.System != nil {
			if _, ok := relayed[member.Constants.System.XUID]; ok {
				continue
			}
		}
		count++
	}
	return count
}

// handleClient relays conn when relay mode is configured and transfers it otherwise.
func (b *Broadcaster) handleClient(conn relayClientConn) {
	if b.conf.Relay == nil {
		b.transfer(conn)
		return
	}
	b.relay(conn)
}

// relay logs conn's player into the backend with their own identity and pumps
// packets both ways until either side disconnects. A Disconnect from the
// backend is shown to the player rather than degrading to a dropped connection.
func (b *Broadcaster) relay(conn relayClientConn) {
	id := conn.IdentityData()
	b.relays.add(conn, id.XUID)

	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	server, target, err := b.connectRelay(ctx, conn, id)
	if err != nil {
		// Bookkeeping first: closing can block on a stalled peer.
		b.relays.remove(conn)
		b.disconnectRelayClient(conn, &packet.Disconnect{Message: text.Colourf("<red>%v</red>", err)})
		return
	}

	if recorder, ok := b.conf.FriendHistory.(HistoryRecorder); ok && id.XUID != "" {
		if err := recorder.Seen(ctx, id.XUID, time.Now()); err != nil {
			b.log.Error("record player history", "xuid", id.XUID, "err", err)
		}
	}
	b.info("relaying bedrock client", "xuid", id.XUID, "name", id.DisplayName, "target", target)

	end := b.pumpRelay(ctx, conn, server)
	b.relays.remove(conn)
	_ = server.Close()
	if end.err != nil && !errors.Is(end.err, net.ErrClosed) {
		b.debug("relay ended", "xuid", id.XUID, "name", id.DisplayName, "from_server", end.fromServer, "err", end.err)
	}
	var disconnect *minecraft.DisconnectPacketError
	if end.fromServer && errors.As(end.err, &disconnect) {
		b.disconnectRelayClient(conn, &packet.Disconnect{
			Reason:                  disconnect.Reason,
			HideDisconnectionScreen: disconnect.HideDisconnectionScreen,
			Message:                 disconnect.Message,
			FilteredMessage:         disconnect.FilteredMessage,
		})
		return
	}
	_ = conn.Close()
}

// relayError carries the message shown to the player when the relay cannot start.
type relayError struct {
	message string
	err     error
}

func (e *relayError) Error() string { return e.message }
func (e *relayError) Unwrap() error { return e.err }

// connectRelay resolves and dials the backend for conn, returning the backend
// conn and its address. The error's text is safe to show to the player.
func (b *Broadcaster) connectRelay(ctx context.Context, conn relayClientConn, id login.IdentityData) (relayServerConn, string, error) {
	ctx, cancel := context.WithTimeout(ctx, b.conf.Relay.dialTimeout())
	defer cancel()
	target, err := b.resolveRelayTarget(ctx, id, conn.ClientData())
	if err != nil {
		b.log.Error("resolve relay target", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		return nil, "", &relayError{message: "The server is not available right now.", err: err}
	}
	server, err := b.dialRelayTarget(ctx, conn, target)
	if err != nil {
		b.log.Error("dial relay target", "xuid", id.XUID, "name", id.DisplayName, "target", target, "err", err)
		return nil, target, &relayError{message: "Could not reach the server, try again shortly.", err: err}
	}
	return server, target, nil
}

func (b *Broadcaster) resolveRelayTarget(ctx context.Context, id login.IdentityData, client login.ClientData) (string, error) {
	if b.conf.Relay.ResolveTarget == nil {
		return b.conf.Server.Address(), nil
	}
	target, err := b.conf.Relay.ResolveTarget(ctx, id, client)
	if err != nil {
		return "", err
	}
	if target == "" {
		return "", errors.New("resolver returned an empty target")
	}
	return target, nil
}

// dialRelayTarget dials target with the client's identity and client data.
// The backend cannot encrypt to the client's key, so the login chain is
// self-signed while keeping the XUID the listener already verified.
func (b *Broadcaster) dialRelayTarget(ctx context.Context, conn relayClientConn, target string) (relayServerConn, error) {
	cfg := b.conf.Relay
	d := cfg.Dialer
	d.IdentityData = conn.IdentityData()
	d.ClientData = conn.ClientData()
	d.ClientData.ServerAddress = target
	d.KeepXBLIdentityData = true
	d.DisablePacketHandling = true
	d.EnableBatchReading = true
	d.FlushRate = -1 // relayPump flushes once per forwarded batch
	d.ForwardClientCacheStatus = true
	d.DisconnectOnUnknownPackets = false
	d.DisconnectOnInvalidPackets = false
	d.Protocol = conn.Proto()
	if d.ErrorLog == nil {
		d.ErrorLog = b.log
	}
	dial := b.relayDial
	if dial == nil {
		dial = defaultRelayDial
	}
	server, err := dial(ctx, d, cfg.network(), target)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func defaultRelayDial(ctx context.Context, d minecraft.Dialer, network, address string) (relayServerConn, error) {
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// relayEnd reports which leg stopped a relay and why.
type relayEnd struct {
	fromServer bool
	err        error
}

// pumpRelay forwards batches both ways until a leg fails or the broadcaster
// stops. The other pump unblocks once the caller closes both conns.
func (b *Broadcaster) pumpRelay(ctx context.Context, client relayClientConn, server relayServerConn) relayEnd {
	ends := make(chan relayEnd, 2)
	go func() { ends <- relayEnd{err: relayPump(client, server, nil)} }()
	go func() {
		ends <- relayEnd{fromServer: true, err: relayPump(server, client, func(pk packet.Packet) {
			if registry, ok := pk.(*packet.ItemRegistry); ok {
				data := minecraft.GameData{Items: registry.Items}
				server.SetGameData(data)
				client.SetGameData(data)
			}
		})}
	}()
	select {
	case end := <-ends:
		return end
	case <-ctx.Done():
		return relayEnd{err: ctx.Err()}
	}
}

// relayPump forwards each network batch read from src as one batch to dst, so
// the relay adds no coalescing latency of its own. observe, if non-nil, sees
// every packet before it is written.
func relayPump(src, dst relayServerConn, observe func(packet.Packet)) error {
	for {
		batch, err := src.ReadBatch()
		if err != nil {
			return fmt.Errorf("read batch: %w", err)
		}
		for _, pk := range batch {
			if observe != nil {
				observe(pk)
			}
			if err := dst.WritePacket(pk); err != nil {
				return fmt.Errorf("write packet: %w", err)
			}
		}
		if err := dst.Flush(); err != nil {
			return fmt.Errorf("flush batch: %w", err)
		}
	}
}

// disconnectRelayClient shows pk to the player, then waits for the client to
// leave before closing so the message is not cut off by the transport teardown.
func (b *Broadcaster) disconnectRelayClient(conn relayClientConn, pk *packet.Disconnect) {
	defer conn.Close()
	if err := conn.WritePacket(pk); err != nil {
		return
	}
	if err := conn.Flush(); err != nil {
		return
	}
	timeout := b.effectiveTransferCloseTimeout()
	if timeout <= 0 {
		return
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return
	}
	cancelWait := b.closeTransferredClientOnStop(conn)
	defer cancelWait()
	for {
		if _, err := conn.ReadBatch(); err != nil {
			return
		}
	}
}
