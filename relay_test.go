package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"golang.org/x/oauth2"
)

type relayOAuthTokenSource struct{}

func (relayOAuthTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "token"}, nil
}

// fakeRelayConn serves scripted batches and records what the relay writes.
type fakeRelayConn struct {
	mu       sync.Mutex
	batches  chan []packet.Packet
	written  []packet.Packet
	flushes  int
	closed   chan struct{}
	identity login.IdentityData
	client   login.ClientData
	// anonymous reports the login key as unproven, as for a NetherNet peer without an identity.
	anonymous bool
}

func newFakeRelayConn(batches ...[]packet.Packet) *fakeRelayConn {
	c := &fakeRelayConn{batches: make(chan []packet.Packet, len(batches)), closed: make(chan struct{})}
	for _, batch := range batches {
		c.batches <- batch
	}
	return c
}

// ReadBatch serves every queued batch before reporting the conn closed.
func (c *fakeRelayConn) ReadBatch() ([]packet.Packet, error) {
	select {
	case batch := <-c.batches:
		return batch, nil
	default:
	}
	select {
	case batch := <-c.batches:
		return batch, nil
	case <-c.closed:
		return nil, net.ErrClosed
	}
}

// SendStartGame allows the fake client to be used by the transfer-mode routing test.
func (c *fakeRelayConn) SendStartGame(minecraft.GameData) error {
	return nil
}

func (c *fakeRelayConn) WritePacket(pk packet.Packet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.written = append(c.written, pk)
	return nil
}

func (c *fakeRelayConn) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.flushes++
	return nil
}

func (c *fakeRelayConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

func (c *fakeRelayConn) Abort() error                       { return c.Close() }
func (c *fakeRelayConn) LoginKeyProven() bool               { return !c.anonymous }
func (c *fakeRelayConn) ReadPacket() (packet.Packet, error) { return nil, net.ErrClosed }
func (c *fakeRelayConn) SetReadDeadline(time.Time) error    { return nil }
func (c *fakeRelayConn) IdentityData() login.IdentityData   { return c.identity }
func (c *fakeRelayConn) ClientData() login.ClientData       { return c.client }
func (c *fakeRelayConn) Proto() minecraft.Protocol          { return minecraft.DefaultProtocol }
func (c *fakeRelayConn) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
func (c *fakeRelayConn) snapshot() ([]packet.Packet, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]packet.Packet(nil), c.written...), c.flushes
}
func (c *fakeRelayConn) waitClosed(t *testing.T, what string) {
	t.Helper()
	select {
	case <-c.closed:
	case <-time.After(time.Second):
		t.Fatalf("%s was not closed", what)
	}
}

func relayTestBroadcaster(relay *RelayConfig, dial relayDialFunc) *Broadcaster {
	return &Broadcaster{
		log:       testBroadcasterLogger(),
		conf:      Config{Server: ServerInfo{Host: "backend.example.net", Port: 19133}, Relay: relay},
		relayDial: dial,
	}
}

func TestRelayPumpForwardsEachBatchWithOneFlush(t *testing.T) {
	first := []packet.Packet{&packet.Text{Message: "a"}, &packet.Text{Message: "b"}}
	second := []packet.Packet{&packet.Text{Message: "c"}}
	src := newFakeRelayConn(first, second)
	dst := newFakeRelayConn()
	src.Close()

	err := relayPump(src, dst)
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("pump error = %v, want net.ErrClosed", err)
	}
	written, flushes := dst.snapshot()
	if len(written) != 3 || flushes != 2 {
		t.Fatalf("forwarded %d packets with %d flushes, want 3 packets and 2 flushes", len(written), flushes)
	}
}

func TestBroadcasterRelayDialsWithClientIdentityAndForwardsBothWays(t *testing.T) {
	client := newFakeRelayConn([]packet.Packet{&packet.Text{Message: "from client"}})
	client.identity = login.IdentityData{XUID: "visitor", DisplayName: "Visitor"}
	client.client = login.ClientData{GameVersion: "1.26.50", ServerAddress: "nethernet", PlatformOnlineID: "forged", SelfSignedID: "device-uuid"}
	server := newFakeRelayConn([]packet.Packet{&packet.Text{Message: "from server"}})

	var (
		gotDialer  minecraft.Dialer
		gotNetwork string
		gotAddress string
	)
	b := relayTestBroadcaster(&RelayConfig{}, func(_ context.Context, d minecraft.Dialer, network, address string) (relayServerConn, error) {
		gotDialer, gotNetwork, gotAddress = d, network, address
		return server, nil
	})

	done := make(chan struct{})
	go func() {
		b.relay(client)
		close(done)
	}()
	// Both legs forwarded their batch; ending the server leg tears the relay down.
	waitFor(t, func() bool {
		cw, _ := client.snapshot()
		sw, _ := server.snapshot()
		return len(cw) == 1 && len(sw) == 1
	}, "batches were not forwarded both ways")
	if b.relays.count() != 1 {
		t.Fatalf("relayed clients = %d, want 1 while active", b.relays.count())
	}
	server.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not stop after the server closed")
	}
	client.waitClosed(t, "client")

	if gotNetwork != "raknet" || gotAddress != "backend.example.net:19133" {
		t.Fatalf("dialed %s %s, want raknet backend.example.net:19133", gotNetwork, gotAddress)
	}
	if gotDialer.IdentityData.XUID != "visitor" || !gotDialer.KeepXBLIdentityData {
		t.Fatalf("dialer identity %#v keep=%v, want the client's XUID kept", gotDialer.IdentityData, gotDialer.KeepXBLIdentityData)
	}
	if gotDialer.ClientData.GameVersion != "1.26.50" || gotDialer.ClientData.ServerAddress != "backend.example.net:19133" {
		t.Fatalf("dialer client data %#v, want the client's data pointed at the backend", gotDialer.ClientData)
	}
	if gotDialer.ClientData.PlatformOnlineID != "visitor" || gotDialer.ClientData.SelfSignedID != "device-uuid" {
		t.Fatalf("dialer platform ids online=%q self=%q, want the verified XUID and the client's own self-signed id", gotDialer.ClientData.PlatformOnlineID, gotDialer.ClientData.SelfSignedID)
	}
	if !gotDialer.DisablePacketHandling || !gotDialer.EnableBatchReading || gotDialer.FlushRate != -1 {
		t.Fatalf("dialer passthrough flags = %v/%v/%v, want passthrough batch reading with relay-owned flushing", gotDialer.DisablePacketHandling, gotDialer.EnableBatchReading, gotDialer.FlushRate)
	}
	if !gotDialer.ForwardClientCacheStatus {
		t.Fatal("dialer sends its own ClientCacheStatus; the backend would get the client's as a second one")
	}
	if gotDialer.Protocol == nil || gotDialer.Protocol.ID() != minecraft.DefaultProtocol.ID() {
		t.Fatalf("dialer protocol %v, want the client's", gotDialer.Protocol)
	}
	if b.relays.count() != 0 {
		t.Fatalf("relayed clients = %d after the relay ended, want 0", b.relays.count())
	}
}

func TestBroadcasterRelayUsesResolvedTarget(t *testing.T) {
	client := newFakeRelayConn()
	client.identity = login.IdentityData{XUID: "visitor"}
	server := newFakeRelayConn()
	server.Close()

	var gotAddress string
	b := relayTestBroadcaster(&RelayConfig{
		Network: "nethernet",
		ResolveTarget: func(_ context.Context, id login.IdentityData, _ login.ClientData) (string, error) {
			return "instance-" + id.XUID + ":19140", nil
		},
	}, func(_ context.Context, _ minecraft.Dialer, network, address string) (relayServerConn, error) {
		if network != "nethernet" {
			t.Errorf("network = %s, want nethernet", network)
		}
		gotAddress = address
		return server, nil
	})

	b.relay(client)
	if gotAddress != "instance-visitor:19140" {
		t.Fatalf("dialed %q, want the resolved per-client target", gotAddress)
	}
}

func TestBroadcasterRelayDisconnectsClientWhenTargetResolutionFails(t *testing.T) {
	client := newFakeRelayConn()
	dialed := false
	b := relayTestBroadcaster(&RelayConfig{
		ResolveTarget: func(context.Context, login.IdentityData, login.ClientData) (string, error) {
			return "", errors.New("no session")
		},
	}, func(context.Context, minecraft.Dialer, string, string) (relayServerConn, error) {
		dialed = true
		return nil, nil
	})

	b.relay(client)
	if dialed {
		t.Fatal("backend was dialed without a target")
	}
	assertDisconnected(t, client)
}

func TestBroadcasterRelayDisconnectsClientWhenDialFails(t *testing.T) {
	client := newFakeRelayConn()
	b := relayTestBroadcaster(&RelayConfig{}, func(context.Context, minecraft.Dialer, string, string) (relayServerConn, error) {
		return nil, errors.New("connection refused")
	})

	b.relay(client)
	assertDisconnected(t, client)
	if b.relays.count() != 0 {
		t.Fatalf("relayed clients = %d after a failed dial, want 0", b.relays.count())
	}
}

func assertDisconnected(t *testing.T, client *fakeRelayConn) {
	t.Helper()
	written, flushes := client.snapshot()
	if len(written) != 1 {
		t.Fatalf("client received %d packets, want one Disconnect", len(written))
	}
	if _, ok := written[0].(*packet.Disconnect); !ok {
		t.Fatalf("client received %T, want Disconnect", written[0])
	}
	if flushes == 0 || !client.isClosed() {
		t.Fatalf("disconnect flushed=%d closed=%v, want flushed and closed", flushes, client.isClosed())
	}
}

func TestHandleClientTransfersWithoutRelayConfig(t *testing.T) {
	client := newFakeRelayConn()
	b := relayTestBroadcaster(nil, func(context.Context, minecraft.Dialer, string, string) (relayServerConn, error) {
		t.Fatal("backend dialed in transfer mode")
		return nil, nil
	})
	b.transferCloseTimeout = -1

	b.handleClient(client)
	written, _ := client.snapshot()
	var transferred bool
	for _, pk := range written {
		if _, ok := pk.(*packet.Transfer); ok {
			transferred = true
		}
	}
	if !transferred {
		t.Fatalf("client was not transferred: %#v", written)
	}
}

// sessionMembers yields a member per XUID, then one without constants.
func sessionMembers(xuids ...string) iter.Seq2[string, mpsd.MemberDescription] {
	return func(yield func(string, mpsd.MemberDescription) bool) {
		for _, xuid := range xuids {
			if !yield(xuid, mpsd.MemberDescription{Constants: &mpsd.MemberConstants{System: &mpsd.MemberConstantsSystem{XUID: xuid}}}) {
				return
			}
		}
		yield("anonymous", mpsd.MemberDescription{})
	}
}

// Relayed players and the owner occupy the session but cannot be reclaimed by recreating it.
func TestSessionOccupancyCountsRelayedPlayersAsLive(t *testing.T) {
	b := relayTestBroadcaster(&RelayConfig{}, nil)
	live := newFakeRelayConn()
	b.relays.add(live, "relayed")

	members := sessionMembers("relayed", "stale", "host")
	if total, reclaimable := b.sessionOccupancy(members, "host"); total != 4 || reclaimable != 2 {
		t.Fatalf("occupancy = %d total, %d reclaimable; want 4 and 2 (stale and anonymous)", total, reclaimable)
	}
	b.relays.remove(live)
	if total, reclaimable := b.sessionOccupancy(members, "host"); total != 4 || reclaimable != 3 {
		t.Fatalf("occupancy = %d total, %d reclaimable after the relay ended; want 4 and 3", total, reclaimable)
	}
}

// A full session is recovered even when live relayed players are among its members.
func TestSessionFullIssueCountsLiveRelayedMembers(t *testing.T) {
	b := relayTestBroadcaster(&RelayConfig{}, nil)
	xuids := make([]string, 29)
	for i := range xuids {
		xuids[i] = fmt.Sprint(i)
	}
	for _, xuid := range xuids[:3] {
		b.relays.add(newFakeRelayConn(), xuid)
	}
	if reason := b.sessionFullIssue("session", sessionMembers(xuids...), "0"); reason == "" {
		t.Fatal("30-member session with 3 live relayed players was not reported full")
	}
}

// A session whose only members are live relayed players and its owner has nothing to reclaim.
func TestSessionFullIssueIgnoresSessionOfLivePlayers(t *testing.T) {
	b := relayTestBroadcaster(&RelayConfig{}, nil)
	xuids := make([]string, 30)
	for i := range xuids {
		xuids[i] = fmt.Sprint(i)
		if i > 0 {
			b.relays.add(newFakeRelayConn(), xuids[i])
		}
	}
	members := func(yield func(string, mpsd.MemberDescription) bool) {
		for _, xuid := range xuids {
			if !yield(xuid, mpsd.MemberDescription{Constants: &mpsd.MemberConstants{System: &mpsd.MemberConstantsSystem{XUID: xuid}}}) {
				return
			}
		}
	}
	if reason := b.sessionFullIssue("session", members, "0"); reason != "" {
		t.Fatalf("session of live players reported as %q; recreating it reclaims nothing", reason)
	}
}

func TestMinecraftListenConfigRelayModeUsesPassthroughBatches(t *testing.T) {
	b := relayTestBroadcaster(&RelayConfig{}, nil)
	conf := b.minecraftListenConfig(room.Status{})
	if !conf.DisablePacketHandling || !conf.EnableBatchReading || conf.FlushRate != -1 {
		t.Fatalf("relay listen config = handling off %v, batches %v, flush %v; want passthrough batch reading with relay-owned flushing", conf.DisablePacketHandling, conf.EnableBatchReading, conf.FlushRate)
	}
	if !conf.AllowUnknownPackets || !conf.AllowInvalidPackets {
		t.Fatal("relay listen config must pass unknown and invalid packets through")
	}
	if conf.CompressionThreshold == -1 {
		t.Fatal("relay listen config should keep compression for gameplay traffic")
	}

	b.conf.Relay = nil
	conf = b.minecraftListenConfig(room.Status{})
	if conf.DisablePacketHandling || conf.EnableBatchReading || conf.CompressionThreshold != -1 {
		t.Fatalf("transfer listen config changed: %#v", conf)
	}
}

func TestStatusReportsRelayedPlayersWhenNotQuerying(t *testing.T) {
	b, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
		Relay:          &RelayConfig{},
		Status:         Status{Players: 1, MaxPlayers: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		b.relays.add(newFakeRelayConn(), string(rune('a'+i)))
	}
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.MemberCount != 3 {
		t.Fatalf("member count = %d, want the 3 relayed players", status.MemberCount)
	}
}

func TestNewRejectsRelayWithAuthenticationDisabled(t *testing.T) {
	_, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
		Relay:          &RelayConfig{},
		ListenConfig:   minecraft.ListenConfig{AuthenticationDisabled: true},
	})
	if err == nil {
		t.Fatal("relay mode accepted unauthenticated clients whose XUID the backend would trust")
	}
}

func TestNewRejectsAuthenticatingRelayDialer(t *testing.T) {
	_, err := New(Config{
		XBLTokenSource: staticTokenSource{},
		XUID:           "123",
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
		Relay:          &RelayConfig{Dialer: minecraft.Dialer{TokenSource: relayOAuthTokenSource{}}},
	})
	if err == nil {
		t.Fatal("expected an error for a relay dialer that authenticates as the bot")
	}
}

func TestConfigFileMapsRelay(t *testing.T) {
	cfg := DefaultConfigFile()
	runtime, err := cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Relay != nil {
		t.Fatal("relay mode enabled by default")
	}

	cfg.Relay.Enabled = true
	runtime, err = cfg.RuntimeConfig(RuntimeConfigInput{XBLTokenSource: staticTokenSource{}})
	if err != nil {
		t.Fatal(err)
	}
	if runtime.Relay == nil {
		t.Fatal("relay.enabled was not mapped")
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// fakeMemberSession is a published session whose members appear on Sync once joined is set.
type fakeMemberSession struct {
	mu      sync.Mutex
	members map[string]bool
	joined  string
	syncs   int
}

func (s *fakeMemberSession) MemberByXUID(xuid string) (mpsd.MemberDescription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return mpsd.MemberDescription{}, s.members[xuid]
}

func (s *fakeMemberSession) Sync(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncs++
	if s.joined != "" {
		s.members[s.joined] = true
	}
	return nil
}

// relayWithSessions relays an anonymous client claiming xuid and reports whether the backend was dialed.
func relayWithSessions(t *testing.T, xuid string, sessions ...ownedSession) (dialed bool, client *fakeRelayConn) {
	t.Helper()
	client = newFakeRelayConn()
	client.anonymous = true
	client.identity = login.IdentityData{XUID: xuid}
	server := newFakeRelayConn()
	server.Close()
	b := relayTestBroadcaster(&RelayConfig{}, func(context.Context, minecraft.Dialer, string, string) (relayServerConn, error) {
		dialed = true
		return server, nil
	})
	b.sessionsOverride = func() []ownedSession { return sessions }
	b.relay(client)
	return dialed, client
}

// An anonymous transport cannot prove the login key, so a replayed login must not reach the backend.
func TestRelayRejectsAnonymousLoginOutsidePublishedSessions(t *testing.T) {
	session := &fakeMemberSession{members: map[string]bool{"host": true, "other": true}}
	dialed, client := relayWithSessions(t, "visitor", ownedSession{owner: "host", session: session})
	if dialed {
		t.Fatal("anonymous login from a non-member was relayed to the backend")
	}
	assertDisconnected(t, client)
	if session.syncs != 1 {
		t.Fatalf("session synced %d times, want one refresh before rejecting", session.syncs)
	}
}

// Only the player's own account can join the session, so membership vouches for an anonymous login.
func TestRelayAcceptsAnonymousSessionMember(t *testing.T) {
	session := &fakeMemberSession{members: map[string]bool{"host": true, "visitor": true}}
	if dialed, _ := relayWithSessions(t, "visitor", ownedSession{owner: "host", session: session}); !dialed {
		t.Fatal("anonymous login from a session member was not relayed")
	}
}

// A join can reach the listener before the membership update does, so sessions are re-read first.
func TestRelayRefreshesSessionsBeforeRejectingAnonymousLogin(t *testing.T) {
	primary := &fakeMemberSession{members: map[string]bool{"host": true}}
	sub := &fakeMemberSession{members: map[string]bool{"sub": true}, joined: "visitor"}
	dialed, _ := relayWithSessions(t, "visitor", ownedSession{owner: "host", session: primary}, ownedSession{owner: "sub", session: sub})
	if !dialed {
		t.Fatal("anonymous login was rejected although the refreshed sub-account session lists the player")
	}
}

// Every session lists its owner, so a login claiming an owner's XUID proves nothing.
func TestRelayRejectsAnonymousLoginClaimingSessionOwner(t *testing.T) {
	session := &fakeMemberSession{members: map[string]bool{"host": true}}
	if dialed, _ := relayWithSessions(t, "host", ownedSession{owner: "host", session: session}); dialed {
		t.Fatal("anonymous login claiming the session owner was relayed")
	}
}

// stallingRelayConn is a peer that stopped reading: writes and graceful closes block until aborted.
type stallingRelayConn struct {
	*fakeRelayConn
	aborted   chan struct{}
	abortOnce sync.Once
}

func newStallingRelayConn() *stallingRelayConn {
	return &stallingRelayConn{fakeRelayConn: newFakeRelayConn(), aborted: make(chan struct{})}
}

func (c *stallingRelayConn) WritePacket(packet.Packet) error {
	<-c.aborted
	return net.ErrClosed
}

func (c *stallingRelayConn) Close() error {
	<-c.aborted
	return c.fakeRelayConn.Close()
}

func (c *stallingRelayConn) Abort() error {
	c.abortOnce.Do(func() { close(c.aborted) })
	return c.fakeRelayConn.Close()
}

// When one leg ends while the other is stuck writing to a peer that stopped reading, teardown must not hang.
func TestRelayTeardownAbortsStalledLeg(t *testing.T) {
	client := newFakeRelayConn([]packet.Packet{&packet.Text{Message: "to a backend that stopped reading"}})
	client.identity = login.IdentityData{XUID: "visitor"}
	server := newStallingRelayConn()
	b := relayTestBroadcaster(&RelayConfig{}, func(context.Context, minecraft.Dialer, string, string) (relayServerConn, error) {
		return server, nil
	})

	done := make(chan struct{})
	go func() {
		b.relay(client)
		close(done)
	}()
	waitFor(t, func() bool { return b.relays.count() == 1 && len(client.batches) == 0 }, "relay did not start forwarding")
	server.fakeRelayConn.Close() // the backend's read side ends
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay teardown hung on the leg blocked writing to a stalled backend")
	}
	if b.relays.count() != 0 {
		t.Fatalf("relayed clients = %d after teardown, want 0", b.relays.count())
	}
}

// Close must stop relays blocked on stalled peers and return only after their handlers have.
func TestCloseWaitsForStalledRelay(t *testing.T) {
	client := newStallingRelayConn()
	client.identity = login.IdentityData{XUID: "visitor"}
	server := newStallingRelayConn()
	b := relayTestBroadcaster(&RelayConfig{}, func(context.Context, minecraft.Dialer, string, string) (relayServerConn, error) {
		return server, nil
	})
	b.announcer = &fakeAnnouncer{}
	b.started = true
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.done = make(chan struct{})
	close(b.done)

	relayDone := make(chan struct{})
	b.clientWg.Add(1)
	go func() {
		defer b.clientWg.Done()
		b.relay(client)
		close(relayDone)
	}()
	waitFor(t, func() bool { return b.relays.count() == 1 }, "relay did not start")

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close hung on a relay blocked on stalled peers")
	}
	select {
	case <-relayDone:
	default:
		t.Fatal("Close returned before the relay handler did")
	}
}
