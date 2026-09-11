package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestSubAccountStatusOwnsIndependentActivity(t *testing.T) {
	connection := p2p.Connection{
		Type:        p2p.ConnectionTypeSignalingOverWebSocket,
		NetherNetID: "shared-nethernet",
	}
	primary := room.Status{
		OwnerID:              "primary",
		LevelID:              accountLevelID("primary"),
		SupportedConnections: []p2p.Connection{connection},
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
	connection := p2p.Connection{
		Type:        p2p.ConnectionTypeSignalingOverWebSocket,
		NetherNetID: "shared-nethernet",
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
}

func TestBroadcasterUpdateRefreshesSubAccountSession(t *testing.T) {
	primary := &fakeAnnouncer{}
	sub := &fakeAnnouncer{}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		announcer: primary,
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: sub,
		}},
		started: true,
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:   "primary",
			Status: Status{HostName: "Host", WorldName: "World"},
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

func TestBroadcasterUpdateReplacesFailedSubAccountSession(t *testing.T) {
	primary := &fakeAnnouncer{}
	stale := &fakeAnnouncer{
		announceErr: errors.New("sub update failed"),
		closeErr:    errors.New("stale close failed"),
	}
	replacement := &fakeAnnouncer{}
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "peoplehub.xboxlive.com" {
			t.Fatalf("unexpected social request: %s %s", req.Method, req.URL)
		}
		return broadcasterResponse(http.StatusOK, `{"people":[{"xuid":"primary","isFollowingCaller":true,"isFollowedByCaller":true}]}`), nil
	})}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		announcer: primary,
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: stale,
		}},
		subAnnouncersByID: map[string]room.Announcer{"sub1": stale},
		started:           true,
		conf: Config{
			Server:     ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:       "primary",
			Status:     Status{HostName: "Host", WorldName: "World"},
			HTTPClient: client,
			SubAccounts: []SubAccountConfig{{
				ID:        "sub1",
				Enabled:   true,
				XBLClient: &xsapi.Client{},
				XUID:      "sub",
			}},
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			return replacement, nil
		},
	}

	if err := b.Update(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !stale.Closed() {
		t.Fatal("stale sub-account session was not closed")
	}
	got, ok := b.subAnnouncersByID["sub1"].(loggingAnnouncer)
	if !ok || got.Announcer != replacement {
		t.Fatalf("retained sub-account announcer = %#v, want wrapped replacement", b.subAnnouncersByID["sub1"])
	}
	if len(b.subAnnouncers) != 1 {
		t.Fatalf("sub-account announcers = %#v, want only replacement", b.subAnnouncers)
	}
	retained, ok := b.subAnnouncers[0].announcer.(loggingAnnouncer)
	if !ok || retained.Announcer != replacement {
		t.Fatalf("sub-account announcers = %#v, want only wrapped replacement", b.subAnnouncers)
	}
	if got := replacement.Status(); got.OwnerID != "sub" || got.LevelID != accountLevelID("sub") {
		t.Fatalf("replacement published wrong ownership: %#v", got)
	}
	if len(b.staleSubAnnouncers) != 1 {
		t.Fatalf("stale cleanup queue = %#v, want failed-to-close announcer", b.staleSubAnnouncers)
	}
	stale.closeErr = nil
	b.cleanupStaleSubAccountSessions()
	if len(b.staleSubAnnouncers) != 0 {
		t.Fatalf("stale cleanup queue = %#v, want empty after retry", b.staleSubAnnouncers)
	}
}

func TestBroadcasterSubAccountRecoveryFailureDoesNotCountAsPrimaryFailure(t *testing.T) {
	primary := &fakeAnnouncer{}
	stale := &fakeAnnouncer{announceErr: errors.New("sub update failed")}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		announcer: primary,
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: stale,
		}},
		subAnnouncersByID: map[string]room.Announcer{"sub1": stale},
		started:           true,
		conf: Config{
			Server: ServerInfo{Host: "play.example.net", Port: 19132},
			XUID:   "primary",
			Status: Status{HostName: "Host", WorldName: "World"},
			SubAccounts: []SubAccountConfig{{
				ID:        "sub1",
				Enabled:   true,
				XBLClient: &xsapi.Client{},
				XUID:      "sub",
			}},
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			return nil, errors.New("sub credentials revoked")
		},
	}

	err := b.Update(context.Background())
	var subErr *subAccountUpdateError
	if !errors.As(err, &subErr) {
		t.Fatalf("Update() error = %v, want subAccountUpdateError", err)
	}
	if countsAsPrimaryUpdateFailure(err) {
		t.Fatalf("sub-account recovery error counted as primary failure: %v", err)
	}
	if got := primary.Status(); got.OwnerID != "primary" {
		t.Fatalf("primary session was not updated before sub-account recovery failed: %#v", got)
	}
}

func TestBroadcasterRecoversSubAccountHealthWithoutClosingPrimary(t *testing.T) {
	primary := &fakeAnnouncer{}
	stale := &fakeAnnouncer{}
	replacement := &fakeAnnouncer{}
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "peoplehub.xboxlive.com" {
			t.Fatalf("unexpected social request: %s %s", req.Method, req.URL)
		}
		return broadcasterResponse(http.StatusOK, `{"people":[{"xuid":"primary","isFollowingCaller":true,"isFollowedByCaller":true}]}`), nil
	})}
	b := &Broadcaster{
		log:       testBroadcasterLogger(),
		announcer: primary,
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: stale,
		}},
		subAnnouncersByID: map[string]room.Announcer{"sub1": stale},
		conf: Config{
			XBLClient:  &xsapi.Client{},
			XUID:       "primary",
			HTTPClient: client,
			SubAccounts: []SubAccountConfig{{
				ID:        "sub1",
				Enabled:   true,
				XBLClient: &xsapi.Client{},
				XUID:      "sub",
			}},
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			return replacement, nil
		},
	}

	issue := sessionHealthIssue{reason: "sub-account session lost", subAccountID: "sub1"}
	if err := b.recoverSessionHealthIssue(context.Background(), issue); err != nil {
		t.Fatal(err)
	}
	if primary.Closed() {
		t.Fatal("targeted sub-account recovery closed the primary session")
	}
	if !stale.Closed() {
		t.Fatal("targeted sub-account recovery did not close the stale session")
	}
	if len(b.subAnnouncers) != 1 {
		t.Fatalf("sub-account announcers = %#v, want one replacement", b.subAnnouncers)
	}
}

func TestBroadcasterCleanupClosesIndependentSubAccountSessions(t *testing.T) {
	sub := &fakeAnnouncer{}
	b := &Broadcaster{
		subAnnouncers: []publishedSubAccount{{
			id:        "sub1",
			xuid:      "sub",
			announcer: sub,
		}},
		subAnnouncersByID: map[string]room.Announcer{"sub1": sub},
	}

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

func TestBroadcasterSkipsDuplicateIDsBeforePublishing(t *testing.T) {
	first := &fakeAnnouncer{}
	second := &fakeAnnouncer{}
	announcers := []room.Announcer{first, second}
	factoryCalls := 0
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			XBLClient: &xsapi.Client{},
			XUID:      "same",
			SubAccounts: []SubAccountConfig{
				{ID: "duplicate", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "same"},
				{ID: "duplicate", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "same"},
			},
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			factoryCalls++
			next := announcers[0]
			announcers = announcers[1:]
			return next, nil
		},
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	if err := b.cleanupPublishedSessions(false); err != nil {
		t.Fatal(err)
	}
	if factoryCalls != 1 {
		t.Fatalf("announcer factory calls = %d, want 1", factoryCalls)
	}
	if !first.Closed() {
		t.Fatal("published session was not closed")
	}
	if second.Closed() {
		t.Fatal("duplicate-ID session was published and retained")
	}
}

func TestBroadcasterRejectsDuplicateIDBeforeCredentialFiltering(t *testing.T) {
	factoryCalls := 0
	b := &Broadcaster{
		log: testBroadcasterLogger(),
		conf: Config{
			XBLClient: &xsapi.Client{},
			XUID:      "same",
			SubAccounts: []SubAccountConfig{
				{ID: "duplicate", Enabled: true},
				{ID: "duplicate", Enabled: true, XBLClient: &xsapi.Client{}, XUID: "same"},
			},
		},
		subAccountAnnouncerFactory: func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error) {
			factoryCalls++
			return &fakeAnnouncer{}, nil
		},
	}

	if err := b.startSubAccounts(context.Background(), room.Status{}); err != nil {
		t.Fatal(err)
	}
	if factoryCalls != 0 {
		t.Fatalf("announcer factory calls = %d, want 0 for ambiguous duplicate ID", factoryCalls)
	}
}
