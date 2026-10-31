package broadcaster

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestBroadcasterBuildsJSONRPCSignalingConnection(t *testing.T) {
	pmid := uuid.MustParse("11111111-2222-3333-4444-555555555555")
	b := &Broadcaster{}
	connection, err := b.signalingConnection(&fakeSignaling{networkID: "123456789", pmid: pmid})
	if err != nil {
		t.Fatal(err)
	}
	if connection.Type != p2p.ConnectionTypeSignalingOverJSONRPC || connection.NetherNetID != "123456789" || connection.PlayerMessagingID != pmid {
		t.Fatalf("JSON-RPC connection = %#v", connection)
	}
}

func TestBroadcasterRejectsInvalidNetworkID(t *testing.T) {
	t.Parallel()

	for _, networkID := range []string{"0", "01"} {
		t.Run(networkID, func(t *testing.T) {
			t.Parallel()
			b := &Broadcaster{}
			if _, err := b.signalingConnection(&fakeSignaling{networkID: networkID}); err == nil {
				t.Fatalf("signalingConnection() accepted network ID %q", networkID)
			}
		})
	}
}

func TestSignalingConnectionAnnouncerReplacesConnections(t *testing.T) {
	inner := &fakeAnnouncer{}
	announcer := signalingConnectionAnnouncer{
		Announcer: inner,
		connection: p2p.Connection{
			Type:              p2p.ConnectionTypeSignalingOverJSONRPC,
			NetherNetID:       p2p.NetherNetID("123456789"),
			PlayerMessagingID: fakePlayerMessagingID,
		},
	}

	err := announcer.Announce(context.Background(), room.Status{
		SupportedConnections: []p2p.Connection{{
			Type:        99,
			NetherNetID: p2p.NetherNetID("old"),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	status := inner.Status()
	if len(status.SupportedConnections) != 1 {
		t.Fatalf("unexpected connections: %#v", status.SupportedConnections)
	}
	if got := status.SupportedConnections[0]; got != announcer.connection {
		t.Fatalf("connection = %+v, want %+v", got, announcer.connection)
	}
}
