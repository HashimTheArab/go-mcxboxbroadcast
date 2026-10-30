package broadcaster

import (
	"testing"
	"time"
)

// A rotation's unchecked grace period must not read as a lost advertisement, but a stalled check must.
func TestAccountHealthAdvertisedAtExpires(t *testing.T) {
	checked := time.Unix(1_000, 0)
	for name, tc := range map[string]struct {
		health AccountHealth
		at     time.Time
		want   bool
	}{
		"never checked":       {AccountHealth{Advertised: true}, checked, false},
		"found missing":       {AccountHealth{Advertised: false, CheckedAt: checked}, checked, false},
		"found open":          {AccountHealth{Advertised: true, CheckedAt: checked}, checked, true},
		"within rotation gap": {AccountHealth{Advertised: true, CheckedAt: checked}, checked.Add(activityGracePeriod + activityCheckInterval), true},
		"stale":               {AccountHealth{Advertised: true, CheckedAt: checked}, checked.Add(advertisedFreshness + time.Second), false},
	} {
		if got := tc.health.AdvertisedAt(tc.at); got != tc.want {
			t.Errorf("%s: AdvertisedAt = %v, want %v", name, got, tc.want)
		}
	}
}

// Adds and removals only adjust a count a full friend list has established.
func TestAccountHealthFriendCount(t *testing.T) {
	var state accountHealthState
	state.addFriends(1)
	var got AccountHealth
	state.fill(&got)
	if !got.FriendsCheckedAt.IsZero() || got.Friends != 0 {
		t.Fatalf("count before a full list = %d at %v, want unknown", got.Friends, got.FriendsCheckedAt)
	}
	state.setFriends(10)
	state.addFriends(2)
	state.addFriends(-1)
	state.fill(&got)
	if got.Friends != 11 || got.FriendsCheckedAt.IsZero() {
		t.Fatalf("count = %d, want 11", got.Friends)
	}
	var standalone *accountHealthState
	standalone.setFriends(1)
	standalone.addFriends(1)
}
