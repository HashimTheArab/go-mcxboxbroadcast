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
		xuids[xuid] = struct{}{}
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
// packets both ways until either side disconnects.
func (b *Broadcaster) relay(conn relayClientConn) {
	cfg := b.conf.Relay
	id := conn.IdentityData()
	b.relays.add(conn, id.XUID)
	defer b.relays.remove(conn)
	defer conn.Close()

	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	dialCtx, cancel := context.WithTimeout(ctx, cfg.dialTimeout())
	target, err := b.resolveRelayTarget(dialCtx, id, conn.ClientData())
	if err != nil {
		cancel()
		b.log.Error("resolve relay target", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		b.disconnectRelayClient(conn, "The server is not available right now.")
		return
	}
	server, err := b.dialRelayTarget(dialCtx, conn, target)
	cancel()
	if err != nil {
		b.log.Error("dial relay target", "xuid", id.XUID, "name", id.DisplayName, "target", target, "err", err)
		b.disconnectRelayClient(conn, "Could not reach the server, try again shortly.")
		return
	}
	defer server.Close()

	if recorder, ok := b.conf.FriendHistory.(HistoryRecorder); ok && id.XUID != "" {
		if err := recorder.Seen(ctx, id.XUID, time.Now()); err != nil {
			b.log.Error("record player history", "xuid", id.XUID, "err", err)
		}
	}
	b.info("relaying bedrock client", "xuid", id.XUID, "name", id.DisplayName, "target", target)

	errs := make(chan error, 2)
	go func() { errs <- relayPump(conn, server) }()
	go func() { errs <- relayPump(server, conn) }()
	select {
	case err := <-errs:
		if err != nil && !errors.Is(err, net.ErrClosed) {
			b.debug("relay ended", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		}
	case <-ctx.Done():
	}
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
	if d.IdentityData.XUID != "" {
		// Backends that trust the relay key player data on the client-supplied
		// platform id once the XUID is gone from the chain. Binding it to the
		// verified XUID keeps the record stable and stops a client claiming another
		// player's data.
		d.ClientData.PlatformOnlineID = d.IdentityData.XUID
	}
	d.KeepXBLIdentityData = true
	d.DisablePacketHandling = true
	d.EnableBatchReading = true
	d.FlushRate = -1 // relayPump flushes once per forwarded batch
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

func (b *Broadcaster) disconnectRelayClient(conn relayClientConn, message string) {
	_ = conn.WritePacket(&packet.Disconnect{Message: text.Colourf("<red>%v</red>", message)})
	_ = conn.Flush()
}

// relayPump forwards each network batch read from src as one batch to dst, so
// the relay adds no coalescing latency of its own.
func relayPump(src, dst relayServerConn) error {
	for {
		batch, err := src.ReadBatch()
		if err != nil {
			return fmt.Errorf("read batch: %w", err)
		}
		for _, pk := range batch {
			if err := dst.WritePacket(pk); err != nil {
				return fmt.Errorf("write packet: %w", err)
			}
		}
		if err := dst.Flush(); err != nil {
			return fmt.Errorf("flush batch: %w", err)
		}
	}
}
