package broadcaster

import (
	"testing"
	"time"
)

// A rotation's grace period and Xbox errors keep the last answer; checks that stop running do not.
func TestHealthAdvertisedAtExpires(t *testing.T) {
	checked := time.Unix(1_000, 0)
	for name, tc := range map[string]struct {
		health Health
		at     time.Time
		want   bool
	}{
		"never checked":       {Health{Advertised: true}, checked, false},
		"found missing":       {Health{Advertised: false, CheckedAt: checked, AttemptedAt: checked}, checked, false},
		"found open":          {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked}, checked, true},
		"within rotation gap": {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked}, checked.Add(activityGracePeriod + activityCheckInterval), true},
		"checks stopped":      {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked}, checked.Add(advertisedFreshness + time.Second), false},
		"xbox errors since":   {Health{Advertised: true, CheckedAt: checked, AttemptedAt: checked.Add(time.Hour)}, checked.Add(time.Hour), true},
	} {
		if got := tc.health.AdvertisedAt(tc.at); got != tc.want {
			t.Errorf("%s: AdvertisedAt = %v, want %v", name, got, tc.want)
		}
	}
}

// Adds and removals only adjust a count a full friend list has established; with expiry on, only a missing count needs its own list.
func TestFriendCount(t *testing.T) {
	var count friendCount
	count.add(1)
	if n, at := count.snapshot(); !at.IsZero() || n != 0 {
		t.Fatalf("count before a full list = %d at %v, want unknown", n, at)
	}
	count.set(10)
	count.add(2)
	count.add(-1)
	if n, at := count.snapshot(); n != 11 || at.IsZero() {
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
