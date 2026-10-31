package broadcaster

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/df-mc/go-nethernet"
	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// Brief Xbox errors keep the last answer; long errors and checks that stop running do not.
func TestHealthAdvertisedAtExpires(t *testing.T) {
	checked := time.Unix(1_000, 0)
	for name, tc := range map[string]struct {
		health Health
		at     time.Time
		want   bool
	}{
		"never checked":     {Health{Advertised: true}, checked, false},
		"found missing":     {Health{Advertised: false, CheckedAt: checked, AttemptedAt: checked}, checked, false},
		"found open":        {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked}, checked, true},
		"checks stopped":    {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked}, checked.Add(advertisedFreshness + time.Second), false},
		"brief xbox errors": {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked.Add(10 * time.Minute)}, checked.Add(10 * time.Minute), true},
		"long xbox errors":  {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked.Add(time.Hour)}, checked.Add(time.Hour), false},
	} {
		if got := tc.health.AdvertisedAt(tc.at); got != tc.want {
			t.Errorf("%s: AdvertisedAt = %v, want %v", name, got, tc.want)
		}
	}
}

// A metrics scrape must not wait for Update, which holds the lifecycle lock across Xbox calls.
func TestHealthDoesNotWaitForLifecycleLock(t *testing.T) {
	b := activityTestBroadcaster(t, func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, `{"results":[]}`), nil
	})
	b.health.setIdentity("123", "Bot")
	b.mu.Lock()
	defer b.mu.Unlock()
	done := make(chan Health, 1)
	go func() { done <- b.Health() }()
	select {
	case health := <-done:
		if health.XUID == "" {
			t.Fatalf("health = %+v, want the account identity", health)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Health blocked on the lifecycle lock")
	}
}

// Two players refused for a full list with no accept since mark the account's own list full in Health.
func TestHealthReportsFriendListFullFromSync(t *testing.T) {
	b := &Broadcaster{}
	refused := []Person{{XUID: "1"}}
	syncer := FriendSyncer{
		Client: &syncFriendClient{
			accept: func(context.Context) ([]Person, error) {
				return nil, &AcceptFriendRequestsError{Failed: refused}
			},
			add: func(context.Context, string) error {
				return &xblsocial.ResponseError{StatusCode: 403, Code: 1028}
			},
		},
		Config:      FriendSyncConfig{AutoFriend: true},
		friendCount: &b.health.friends,
	}
	var state friendSyncRunState
	syncer.runSync(t.Context(), &state, false)
	if b.Health().FriendListFull {
		t.Fatal("one refused player marked the account's list full")
	}
	refused = []Person{{XUID: "2"}}
	syncer.runSync(t.Context(), &state, false)
	if !b.Health().FriendListFull {
		t.Fatal("two refused players did not mark the account's list full")
	}
}

// Each answered join offer counts in Health, which the relayed_joins_total metric exports.
func TestHealthCountsRelayedJoins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target := &fakeJoinEndpoint{answer: "answer", start: time.Now()}
		b, err := New(Config{
			JoinURL:    testJoinURL,
			XBLClient:  testXBLClient(t, "123"),
			HTTPClient: &http.Client{Transport: target},
			Log:        testBroadcasterLogger(),
		})
		if err != nil {
			t.Fatal(err)
		}
		b.ctx, b.cancel = context.WithCancel(t.Context())
		defer b.cancel()
		// A ready verifier skips service discovery; the unsigned offer then fails verification.
		b.tokens.verifier = oidc.NewVerifier("https://authorization.test/", &oidc.StaticKeySet{}, &oidc.Config{SkipClientIDCheck: true})
		sig := &relaySignaling{}
		r := b.startRelay(sig)
		defer r.close()
		for range 2 {
			sig.signal(nethernet.SignalTypeOffer, "v=0")
			time.Sleep(offerGatherQuiet)
			synctest.Wait()
		}
		if got := b.Health().RelayedJoins; got != 2 {
			t.Fatalf("relayed joins = %d, want 2", got)
		}
	})
}

// Adds and removals only adjust a count a full friend list has established; with expiry on, only a missing count needs its own list.
func TestFriendCount(t *testing.T) {
	var count friendCount
	count.add(1)
	if n, at, _ := count.snapshot(); !at.IsZero() || n != 0 {
		t.Fatalf("count before a full list = %d at %v, want unknown", n, at)
	}
	count.set(10)
	count.add(2)
	count.add(-1)
	if n, at, _ := count.snapshot(); n != 11 || at.IsZero() {
		t.Fatalf("count = %d, want 11", n)
	}
	count.countedAt = time.Now().Add(-friendCountRefresh - time.Minute)
	if count.needsList(true) || !count.needsList(false) {
		t.Fatal("an old count must be re-listed only when expiry is off")
	}
	var standalone *friendCount
	standalone.set(1)
	standalone.add(1)
	if standalone.needsList(false) {
		t.Fatal("a nil count asked for a list")
	}
}
