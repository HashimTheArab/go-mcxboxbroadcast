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
	"github.com/sandertv/gophertunnel/minecraft/room"
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
// cannot be relayed to. Clients whose transport does not prove their login key
// are relayed only while they are members of a published session.
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

func (c *RelayConfig) validate(listen minecraft.ListenConfig) error {
	if c == nil {
		return nil
	}
	if c.Dialer.TokenSource != nil || c.Dialer.XBLClient != nil || c.Dialer.PlayFabClient != nil {
		return errors.New("relay dialer must not authenticate; the relay logs in with each client's identity")
	}
	if listen.AuthenticationDisabled {
		return errors.New("relay mode requires client authentication; the backend trusts the XUID the relay forwards")
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
	LoginKeyProven() bool
}

// relayServerConn is the batch surface relayed between the two legs.
type relayServerConn interface {
	ReadBatch() ([]packet.Packet, error)
	WritePacket(packet.Packet) error
	Flush() error
	Close() error
	Abort() error
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

// sessionOccupancy counts a session's members, those being relayed right now, and those recreating the
// session would reclaim: members that are neither relayed nor its owner. In transfer mode no member is live.
func (b *Broadcaster) sessionOccupancy(members iter.Seq2[string, mpsd.MemberDescription], ownerXUID string) (total, live, reclaimable int) {
	relayed := b.relays.xuids()
	for _, member := range members {
		total++
		if member.Constants != nil && member.Constants.System != nil {
			xuid := member.Constants.System.XUID
			if _, ok := relayed[xuid]; ok {
				live++
				continue
			}
			if xuid != "" && xuid == ownerXUID {
				continue
			}
		}
		reclaimable++
	}
	return total, live, reclaimable
}

// sessionFullIssue reports why a full session should be recreated, or "" when it should not. Recreating
// drops every member, and MPSD offers the host no way to remove one, so live relayed players would leave
// the session and their friends would lose sight of the world. A full session is therefore only recreated
// when stale members outnumber live ones; otherwise it stays full until players leave.
func (b *Broadcaster) sessionFullIssue(what string, members iter.Seq2[string, mpsd.MemberDescription], ownerXUID string) string {
	total, live, reclaimable := b.sessionOccupancy(members, ownerXUID)
	if total < sessionMemberRestartThreshold || reclaimable <= live {
		return ""
	}
	return fmt.Sprintf("%s has %d/30 members, %d stale and %d relayed", what, total, reclaimable, live)
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
	defer conn.Close()
	defer b.abortOnStop(conn)()
	cfg := b.conf.Relay
	id := conn.IdentityData()

	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.verifyRelayIdentity(ctx, conn); err != nil {
		b.log.Warn("rejected relay client whose login may be replayed", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		b.disconnectRelayClient(conn, "We couldn't check your Xbox account. Join again from your friends list.")
		return
	}
	b.relays.add(conn, id.XUID)
	defer b.relays.remove(conn)

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
	pending := 2
	select {
	case err := <-errs:
		pending--
		if err != nil && !errors.Is(err, net.ErrClosed) {
			b.debug("relay ended", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		}
	case <-ctx.Done():
	}
	// Aborting both legs unblocks a pump stuck writing to a peer that stopped reading.
	_ = conn.Abort()
	_ = server.Abort()
	for ; pending > 0; pending-- {
		<-errs
	}
}

const (
	// relayVerifyTimeout bounds re-reading the published sessions for an anonymous relay client.
	relayVerifyTimeout = 10 * time.Second
	// relayRefreshInterval spaces those re-reads, so repeated joins cannot drive Xbox Live requests.
	relayRefreshInterval = 2 * time.Second
)

// memberSession is the part of a published MPSD session that relay identity checks read.
type memberSession interface {
	MemberByXUID(xuid string) (mpsd.MemberDescription, bool)
	Sync(ctx context.Context) error
}

// ownedSession is a published session and the account that owns it.
type ownedSession struct {
	owner   string
	session memberSession
}

// verifyRelayIdentity returns an error when conn's login may be a replay: the transport did not prove the
// login key, and the player is not a member of any published session, which only their own account can join.
func (b *Broadcaster) verifyRelayIdentity(ctx context.Context, conn relayClientConn) error {
	if conn.LoginKeyProven() {
		return nil
	}
	xuid := conn.IdentityData().XUID
	if xuid == "" {
		return errors.New("login carries no xuid")
	}
	sessions := b.publishedSessions()
	if sessionsHaveMember(sessions, xuid) {
		return nil
	}
	// The join can reach the listener before the session change reaches us over RTA. Sessions are re-read in
	// parallel, so a slow one cannot starve the one that lists the player, and at most once per interval.
	ctx, cancel := context.WithTimeout(ctx, relayVerifyTimeout)
	defer cancel()
	b.relayRefreshMu.Lock()
	defer b.relayRefreshMu.Unlock()
	if sessionsHaveMember(sessions, xuid) {
		return nil // a refresh for another client listed this one
	}
	if wait := time.Until(b.relayRefreshed.Add(relayRefreshInterval)); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return fmt.Errorf("not a member of any published session; refresh not started: %w", ctx.Err())
		}
	}
	defer func() { b.relayRefreshed = time.Now() }()
	type refresh struct {
		member bool
		err    error
	}
	refreshed := make(chan refresh, len(sessions))
	for _, s := range sessions {
		go func() {
			err := s.session.Sync(ctx)
			refreshed <- refresh{member: err == nil && sessionsHaveMember([]ownedSession{s}, xuid), err: err}
		}()
	}
	var syncErr error
	for range sessions {
		r := <-refreshed
		if r.member {
			return nil
		}
		syncErr = errors.Join(syncErr, r.err)
	}
	if syncErr != nil {
		return fmt.Errorf("not a member of any published session; refresh failed: %w", syncErr)
	}
	return errors.New("not a member of any published session")
}

// sessionsHaveMember reports whether xuid is a member of a session it does not own.
func sessionsHaveMember(sessions []ownedSession, xuid string) bool {
	for _, s := range sessions {
		if s.owner == xuid {
			continue
		}
		if _, ok := s.session.MemberByXUID(xuid); ok {
			return true
		}
	}
	return false
}

// publishedSessions returns the primary and sub-account sessions currently published.
func (b *Broadcaster) publishedSessions() []ownedSession {
	if b.sessionsOverride != nil {
		return b.sessionsOverride()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var sessions []ownedSession
	add := func(announcer room.Announcer, owner string) {
		xbl, ok := xblAnnouncer(announcer)
		if !ok {
			return
		}
		xbl.Lock()
		session := xbl.Session
		xbl.Unlock()
		if session != nil && session.Context().Err() == nil {
			sessions = append(sessions, ownedSession{owner: owner, session: session})
		}
	}
	add(b.announcer, b.primaryXUID())
	for _, sub := range b.subAnnouncers {
		add(sub.announcer, sub.xuid)
	}
	return sessions
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
	d.ForwardClientCacheStatus = true // the real client's own status follows through the relay
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
