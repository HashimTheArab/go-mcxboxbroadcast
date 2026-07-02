package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
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
		TransportLayer: p2p.TransportLayerNetherNet,
		SupportedConnections: []room.Connection{{
			ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
			NetherNetID:    p2p.NetherNetID("123456789"),
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
	if connection["ConnectionType"] != float64(p2p.ConnectionTypeSignalingOverJSONRPC) {
		t.Fatalf("unexpected connection type: %#v", connection)
	}
	if connection["PmsgId"] != pmsgID.String() {
		t.Fatalf("unexpected pmsg id: %#v", connection)
	}
	// Java serializes NetherNetId as a number and always sends isHardcore.
	if connection["NetherNetId"] != float64(123456789) {
		t.Fatalf("NetherNetId should be a JSON number: %#v", connection)
	}
	hardcore, ok := got["isHardcore"].(bool)
	if !ok || hardcore {
		t.Fatalf("isHardcore missing or true in custom properties: %s", custom)
	}
}

func TestMarshalStatusWithNoncesKeepsOpaqueNetherNetIDAsString(t *testing.T) {
	custom, err := marshalStatusWithNonces(room.Status{
		WorldName: "World",
		SupportedConnections: []room.Connection{{
			NetherNetID: p2p.NetherNetID("opaque-id"),
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		SupportedConnections []map[string]any `json:"SupportedConnections"`
	}
	if err := json.Unmarshal(custom, &got); err != nil {
		t.Fatal(err)
	}
	if got.SupportedConnections[0]["NetherNetId"] != "opaque-id" {
		t.Fatalf("opaque NetherNetId should stay a string: %s", custom)
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

func TestSessionNonceAnnouncerRepublishClearsNonceState(t *testing.T) {
	session := &mpsd.Session{}
	announcer := newSessionNonceAnnouncer(&room.XBLAnnouncer{Session: session}, "100", nil)
	announcer.Session = session
	announcer.handledSession = session
	announcer.custom = []byte("cached")
	announcer.readRestriction = mpsd.SessionRestrictionFollowed
	announcer.joinRestriction = mpsd.SessionRestrictionFollowed
	announcer.nonces["200"] = "stale"

	announcer.resetForRepublishLocked()

	if announcer.Session != nil {
		t.Fatal("session should be cleared before republish")
	}
	if announcer.handledSession != nil {
		t.Fatal("handled session should be cleared before republish")
	}
	if announcer.custom != nil || announcer.readRestriction != "" || announcer.joinRestriction != "" {
		t.Fatal("cached publish state should be cleared before republish")
	}
	custom, err := marshalStatusWithNonces(room.Status{WorldName: "World"}, announcer.nonces)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Nonces map[string]string `json:"nonces"`
	}
	if err := json.Unmarshal(custom, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Nonces) != 0 {
		t.Fatalf("stale nonce state survived republish reset: %#v", got.Nonces)
	}
}

func TestSessionNonceAnnouncerDoesNotNoOpWithoutSession(t *testing.T) {
	status := room.Status{
		WorldName:        "World",
		BroadcastSetting: p2p.BroadcastSettingFriendsOfFriends,
	}
	custom, err := marshalStatusWithNonces(status, nil)
	if err != nil {
		t.Fatal(err)
	}
	announcer := newSessionNonceAnnouncer(&room.XBLAnnouncer{}, "100", nil)
	announcer.custom = custom
	announcer.readRestriction = mpsd.SessionRestrictionFollowed
	announcer.joinRestriction = mpsd.SessionRestrictionFollowed

	err = announcer.Announce(context.Background(), status)
	if err == nil {
		t.Fatal("Announce returned nil without an active session")
	}
	if !strings.Contains(err.Error(), "XBLAnnouncer.Client is nil") {
		t.Fatalf("Announce error = %v, want missing client publish attempt", err)
	}
}
