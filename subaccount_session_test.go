package broadcaster

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestSubAccountStatusOwnsIndependentActivity(t *testing.T) {
	connection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "shared-nethernet",
		PmsgID:         uuid.New(),
	}
	primary := room.Status{
		OwnerID:              "primary",
		LevelID:              accountLevelID("primary"),
		HostName:             "Lunar",
		WorldName:            "Lunar",
		SupportedConnections: []room.Connection{connection},
	}

	got := subAccountStatus(primary, "sub")

	if got.OwnerID != "sub" {
		t.Fatalf("OwnerID = %q, want sub", got.OwnerID)
	}
	if got.LevelID != accountLevelID("sub") {
		t.Fatalf("LevelID = %q, want account-specific level id", got.LevelID)
	}
	if fmt.Sprint(got.SupportedConnections) != fmt.Sprint(primary.SupportedConnections) {
		t.Fatalf("SupportedConnections = %#v, want shared primary connection %#v", got.SupportedConnections, primary.SupportedConnections)
	}
	if primary.OwnerID != "primary" || primary.LevelID != accountLevelID("primary") {
		t.Fatalf("subAccountStatus mutated primary status: %#v", primary)
	}
}

func TestBroadcasterStartSubAccountPublishesIndependentSession(t *testing.T) {
	connection := room.Connection{
		ConnectionType: p2p.ConnectionTypeSignalingOverJSONRPC,
		NetherNetID:    "shared-nethernet",
		PmsgID:         uuid.New(),
	}
	sub := &fakeAnnouncer{}
	var ref mpsd.SessionReference
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "peoplehub.xboxlive.com" {
			t.Fatalf("unexpected social request: %s %s", req.Method, req.URL)
		}
		return broadcasterResponse(http.StatusOK, `{"people":[{"xuid":"primary","isFollowingCaller":true,"isFollowedByCaller":true}]}`), nil
	})}
	b := &Broadcaster{
		log:               testBroadcasterLogger(),
		sessionRef:        mpsd.SessionReference{ServiceConfigID: serviceConfigUUID, TemplateName: TemplateName, Name: "PRIMARY"},
		sessionConnection: &connection,
		conf: Config{
			XBLClient:  &xsapi.Client{},
			XUID:       "primary",
			HTTPClient: client,
		},
		subAccountAnnouncerFactory: func(_ context.Context, _ SubAccountConfig, got mpsd.SessionReference) (room.Announcer, error) {
			ref = got
			return sub, nil
		},
	}
	account := &SubAccountConfig{ID: "sub1", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "sub"}
	status := room.Status{OwnerID: "primary", LevelID: accountLevelID("primary")}

	if err := b.startSubAccount(context.Background(), account, status); err != nil {
		t.Fatal(err)
	}

	if ref.Name == "" || ref.Name == b.sessionRef.Name {
		t.Fatalf("sub-account session name = %q, want non-empty name distinct from primary %q", ref.Name, b.sessionRef.Name)
	}
	got := sub.Status()
	if got.OwnerID != "sub" || got.LevelID != accountLevelID("sub") {
		t.Fatalf("sub-account published primary-owned status: %#v", got)
	}
	if len(got.SupportedConnections) != 1 || got.SupportedConnections[0] != connection {
		t.Fatalf("sub-account connection = %#v, want shared connection %#v", got.SupportedConnections, connection)
	}
	if b.subAnnouncersByID["sub1"] == nil {
		t.Fatal("sub-account announcer was not retained for updates and invites")
	}
}

func TestBroadcasterUpdateRefreshesSubAccountSession(t *testing.T) {
	primary := &fakeAnnouncer{}
	sub := &fakeAnnouncer{}
	b := &Broadcaster{
		log:               testBroadcasterLogger(),
		announcer:         primary,
		subAnnouncersByID: map[string]room.Announcer{"sub1": sub},
		started:           true,
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:   "primary",
			Status: Status{HostName: "Host", WorldName: "World"},
			SubAccounts: []SubAccountConfig{{
				ID:      "sub1",
				Enabled: true,
				XUID:    "sub",
			}},
		},
	}

	if err := b.Update(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := primary.Status(); got.OwnerID != "primary" {
		t.Fatalf("primary OwnerID = %q, want primary", got.OwnerID)
	}
	if got := sub.Status(); got.OwnerID != "sub" || got.LevelID != accountLevelID("sub") {
		t.Fatalf("sub-account update used primary ownership: %#v", got)
	}
}

func TestBroadcasterCleanupClosesIndependentSubAccountSessions(t *testing.T) {
	sub := &fakeAnnouncer{}
	b := &Broadcaster{subAnnouncersByID: map[string]room.Announcer{"sub1": sub}}

	if err := b.cleanupPublishedSessions(false); err != nil {
		t.Fatal(err)
	}
	if !sub.Closed() {
		t.Fatal("sub-account announcer was not closed")
	}
	if len(b.subAnnouncersByID) != 0 {
		t.Fatalf("sub-account announcers retained after cleanup: %#v", b.subAnnouncersByID)
	}
}
