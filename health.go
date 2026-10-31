package broadcaster

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/df-mc/go-xsapi/v2"
)

// friendCountRefresh re-lists friends when expiry scans do not, so removals by other players are seen.
const friendCountRefresh = 30 * time.Minute

// advertisedFreshness spans a new publication's unchecked grace period and a
// few check intervals, so a session rotation does not read as a lost advertisement.
const advertisedFreshness = activityGracePeriod + 3*activityCheckInterval

// Health is the broadcaster's own view of its published account. It cannot prove
// that another account can see or join the session; Xbox decides that per viewer.
// Zero times mean not yet known.
type Health struct {
	XUID             string
	Gamertag         string
	Version          string
	Protocol         int32
	Advertised       bool // whether the last own-handle check found the session open
	CheckedAt        time.Time
	Friends          int
	FriendsCheckedAt time.Time
	RelayedJoins     uint64 // join offers answered since start
}

// AdvertisedAt reports whether the session was found open recently enough to trust at now.
func (h Health) AdvertisedAt(now time.Time) bool {
	return h.Advertised && !h.CheckedAt.IsZero() && now.Sub(h.CheckedAt) <= advertisedFreshness
}

// Health returns a snapshot of the broadcaster's account. It never waits on the lifecycle
// lock, which Update holds across Xbox calls; the identity appears once the session loop runs.
func (b *Broadcaster) Health() Health {
	health := b.health.snapshot()
	health.Friends, health.FriendsCheckedAt = b.health.friends.snapshot()
	health.Version, health.Protocol = b.advertisedVersion(), b.advertisedProtocol()
	health.RelayedJoins = b.health.joins.Load()
	return health
}

// recordHealthIdentity publishes the account identity Health reports.
func (b *Broadcaster) recordHealthIdentity() {
	b.mu.Lock()
	xuid, gamertag := b.primaryXUID(), clientGamertag(b.conf.XBLClient, b.xblClient)
	b.mu.Unlock()
	b.health.mu.Lock()
	defer b.health.mu.Unlock()
	b.health.xuid, b.health.gamertag = xuid, gamertag
}

func clientGamertag(clients ...*xsapi.Client) string {
	for _, client := range clients {
		if client != nil {
			if gamertag := client.UserInfo().GamerTag; gamertag != "" {
				return gamertag
			}
		}
	}
	return ""
}

// healthState collects what Health reports, apart from config-derived fields.
type healthState struct {
	joins   atomic.Uint64
	friends friendCount

	mu         sync.Mutex
	xuid       string
	gamertag   string
	advertised bool
	checkedAt  time.Time
}

func (h *healthState) checked(advertised bool, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.advertised, h.checkedAt = advertised, at
}

func (h *healthState) snapshot() Health {
	h.mu.Lock()
	defer h.mu.Unlock()
	return Health{XUID: h.xuid, Gamertag: h.gamertag, Advertised: h.advertised, CheckedAt: h.checkedAt}
}

// friendCount is the friend list size: exact at each full list, adjusted by adds and removals between lists.
// A nil friendCount, as in a standalone FriendSyncer, ignores every call.
type friendCount struct {
	mu        sync.Mutex
	friends   int
	countedAt time.Time
}

// set records a full friend list.
func (c *friendCount) set(n int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.friends, c.countedAt = n, time.Now()
}

// stale reports whether the count is missing or older than friendCountRefresh.
func (c *friendCount) stale() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.countedAt.IsZero() || time.Since(c.countedAt) > friendCountRefresh
}

// add adjusts a known count between full lists.
func (c *friendCount) add(delta int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.countedAt.IsZero() {
		c.friends = max(c.friends+delta, 0)
	}
}

func (c *friendCount) snapshot() (int, time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.friends, c.countedAt
}
