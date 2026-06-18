package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestMarshalStatusWithNoncesIncludesJavaSessionFields(t *testing.T) {
	pmsgID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	custom, err := marshalStatusWithNonces(room.Status{
		HostName:       "Host",
		OwnerID:        "100",
		WorldName:      "World",
		WorldType:      room.WorldTypeCreative,
		Protocol:       1001,
		Version:        "1.26.30",
		TitleID:        0,
		LevelID:        "level",
		TransportLayer: room.TransportLayerNetherNet,
		SupportedConnections: []room.Connection{{
			ConnectionType: room.ConnectionTypeJSONRPCSignaling,
			NetherNetID:    room.NetherNetID("123456789"),
			PmsgID:         pmsgID,
		}},
	}, map[string]string{"200": "0102030405060708"})
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(custom, &got); err != nil {
		t.Fatal(err)
	}
	if got["ownerId"] != "100" {
		t.Fatalf("ownerId missing from custom properties: %s", custom)
	}
	nonces, ok := got["nonces"].(map[string]any)
	if !ok {
		t.Fatalf("nonces missing from custom properties: %s", custom)
	}
	if nonces["200"] != "0102030405060708" {
		t.Fatalf("unexpected nonce map: %#v", nonces)
	}
	connections := got["SupportedConnections"].([]any)
	connection := connections[0].(map[string]any)
	if connection["ConnectionType"] != float64(room.ConnectionTypeJSONRPCSignaling) {
		t.Fatalf("unexpected connection type: %#v", connection)
	}
	if connection["PmsgId"] != pmsgID.String() {
		t.Fatalf("unexpected pmsg id: %#v", connection)
	}
}

func TestMarshalStatusWithNoncesWritesEmptyObject(t *testing.T) {
	custom, err := marshalStatusWithNonces(room.Status{WorldName: "World"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]json.RawMessage
	if err := json.Unmarshal(custom, &got); err != nil {
		t.Fatal(err)
	}
	if string(got["nonces"]) != "{}" {
		t.Fatalf("expected empty nonces object, got %s in %s", got["nonces"], custom)
	}
}

func TestSyncSessionNoncesMatchesActiveMembers(t *testing.T) {
	nonces := map[string]string{
		"200":   "existing",
		"stale": "remove-me",
	}
	var generated int
	changed, err := syncSessionNonces(nonces, []string{"100", "200", "300", "300", ""}, "100", func() (string, error) {
		generated++
		return fmt.Sprintf("generated-%d", generated), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected nonce map to change")
	}
	if generated != 1 {
		t.Fatalf("expected one generated nonce, got %d", generated)
	}
	want := map[string]string{"200": "existing", "300": "generated-1"}
	if !reflect.DeepEqual(nonces, want) {
		t.Fatalf("unexpected nonce map\n got: %v\nwant: %v", nonces, want)
	}

	changed, err = syncSessionNonces(nonces, []string{"100", "200", "300"}, "100", func() (string, error) {
		t.Fatal("generator should not be called when nonces are current")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("expected nonce map to stay unchanged")
	}
}

func TestSessionNonceAnnouncerUpdateNoncesWritesCustomProperties(t *testing.T) {
	session := &mpsd.Session{}
	announcer := newSessionNonceAnnouncer(&room.XBLAnnouncer{Session: session}, "100", nil)
	announcer.lastStatus = room.Status{
		OwnerID:   "100",
		WorldName: "World",
	}
	announcer.nonces["stale"] = "remove-me"

	var writes []json.RawMessage
	err := announcer.updateNonces(context.Background(), session, []string{"100", "200"}, func(_ context.Context, custom json.RawMessage) error {
		writes = append(writes, append(json.RawMessage(nil), custom...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(writes) != 1 {
		t.Fatalf("expected one custom property write, got %d", len(writes))
	}

	var got struct {
		OwnerID string            `json:"ownerId"`
		Nonces  map[string]string `json:"nonces"`
	}
	if err := json.Unmarshal(writes[0], &got); err != nil {
		t.Fatal(err)
	}
	if got.OwnerID != "100" {
		t.Fatalf("owner id = %q, want 100", got.OwnerID)
	}
	if _, ok := got.Nonces["100"]; ok {
		t.Fatalf("host XUID should not receive a nonce: %#v", got.Nonces)
	}
	if _, ok := got.Nonces["stale"]; ok {
		t.Fatalf("stale nonce was not removed: %#v", got.Nonces)
	}
	if len(got.Nonces["200"]) != 16 {
		t.Fatalf("client nonce should be 8 random bytes as hex, got %#v", got.Nonces)
	}

	err = announcer.updateNonces(context.Background(), session, []string{"100", "200"}, func(context.Context, json.RawMessage) error {
		t.Fatal("custom properties should not be written when nonce state is unchanged")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
