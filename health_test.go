package broadcaster

import (
	"testing"
	"time"
)

// A rotation's unchecked grace period must not read as a lost advertisement, but a stalled check must.
func TestHealthAdvertisedAtExpires(t *testing.T) {
	checked := time.Unix(1_000, 0)
	for name, tc := range map[string]struct {
		health Health
		at     time.Time
		want   bool
	}{
		"never checked":       {Health{Advertised: true}, checked, false},
		"found missing":       {Health{Advertised: false, CheckedAt: checked}, checked, false},
		"found open":          {Health{Advertised: true, CheckedAt: checked}, checked, true},
		"within rotation gap": {Health{Advertised: true, CheckedAt: checked}, checked.Add(activityGracePeriod + activityCheckInterval), true},
		"stale":               {Health{Advertised: true, CheckedAt: checked}, checked.Add(advertisedFreshness + time.Second), false},
	} {
		if got := tc.health.AdvertisedAt(tc.at); got != tc.want {
			t.Errorf("%s: AdvertisedAt = %v, want %v", name, got, tc.want)
		}
	}
}

// Adds and removals only adjust a count a full friend list has established.
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
	var standalone *friendCount
	standalone.set(1)
	standalone.add(1)
	if standalone.stale() {
		t.Fatal("a nil count asked for a list")
	}
}
