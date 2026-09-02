package broadcaster

import (
	"context"
	"errors"
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
}

func newFakeRelayConn(batches ...[]packet.Packet) *fakeRelayConn {
	c := &fakeRelayConn{batches: make(chan []packet.Packet, len(batches)), closed: make(chan struct{})}
	for _, batch := range batches {
		c.batches <- batch
	}
	return c
}

func (c *fakeRelayConn) ReadBatch() ([]packet.Packet, error) {
	select {
	case batch := <-c.batches:
		return batch, nil
	case <-c.closed:
		return nil, net.ErrClosed
	}
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
	go func() {
		time.Sleep(20 * time.Millisecond)
		src.Close()
	}()

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
	client.client = login.ClientData{GameVersion: "1.26.45", ServerAddress: "nethernet", PlatformOnlineID: "forged", SelfSignedID: "device-uuid"}
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
	if gotDialer.ClientData.GameVersion != "1.26.45" || gotDialer.ClientData.ServerAddress != "backend.example.net:19133" {
		t.Fatalf("dialer client data %#v, want the client's data pointed at the backend", gotDialer.ClientData)
	}
	if gotDialer.ClientData.PlatformOnlineID != "visitor" || gotDialer.ClientData.SelfSignedID != "device-uuid" {
		t.Fatalf("dialer platform ids online=%q self=%q, want the verified XUID and the client's own self-signed id", gotDialer.ClientData.PlatformOnlineID, gotDialer.ClientData.SelfSignedID)
	}
	if !gotDialer.DisablePacketHandling || !gotDialer.EnableBatchReading || gotDialer.FlushRate != -1 {
		t.Fatalf("dialer passthrough flags = %v/%v/%v, want passthrough batch reading with relay-owned flushing", gotDialer.DisablePacketHandling, gotDialer.EnableBatchReading, gotDialer.FlushRate)
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

func TestStaleSessionMembersIgnoresRelayedPlayers(t *testing.T) {
	b := relayTestBroadcaster(&RelayConfig{}, nil)
	live := newFakeRelayConn()
	b.relays.add(live, "relayed")

	members := func(yield func(string, mpsd.MemberDescription) bool) {
		for _, xuid := range []string{"relayed", "stale", "host"} {
			if !yield(xuid, mpsd.MemberDescription{Constants: &mpsd.MemberConstants{System: &mpsd.MemberConstantsSystem{XUID: xuid}}}) {
				return
			}
		}
		yield("anonymous", mpsd.MemberDescription{})
	}
	if got := b.staleSessionMembers(iter.Seq2[string, mpsd.MemberDescription](members)); got != 3 {
		t.Fatalf("stale members = %d, want 3 (everyone but the relayed player)", got)
	}
	b.relays.remove(live)
	if got := b.staleSessionMembers(iter.Seq2[string, mpsd.MemberDescription](members)); got != 4 {
		t.Fatalf("stale members = %d after the relay ended, want 4", got)
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
