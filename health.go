package broadcaster

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/df-mc/go-xsapi/v2"
)

// friendCountRefresh re-lists friends when expiry is off, so removals by other players are seen.
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
	Advertised       bool      // whether the last answered own-handle check found the session open
	CheckedAt        time.Time // last answered check
	AttemptedAt      time.Time // last check, answered or not; Xbox errors keep the last answer
	Friends          int
	FriendsCheckedAt time.Time
	RelayedJoins     uint64 // join offers answered since start
}

// AdvertisedAt reports whether the last answer found the session open and checks were still running at now.
func (h Health) AdvertisedAt(now time.Time) bool {
	return h.Advertised && !h.CheckedAt.IsZero() && now.Sub(h.AttemptedAt) <= advertisedFreshness
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

// recordHealthIdentity fills in the account identity once. A recorded XUID or gamertag is never
// replaced, so the identity labels on exported series stay stable.
func (b *Broadcaster) recordHealthIdentity() {
	b.health.mu.Lock()
	known := b.health.xuid != "" && b.health.gamertag != ""
	b.health.mu.Unlock()
	if known {
		return
	}
	b.mu.Lock()
	xuid, gamertag := b.primaryXUID(), clientGamertag(b.conf.XBLClient, b.xblClient)
	b.mu.Unlock()
	b.health.mu.Lock()
	defer b.health.mu.Unlock()
	if b.health.xuid == "" {
		b.health.xuid = xuid
	}
	if b.health.gamertag == "" {
		b.health.gamertag = gamertag
	}
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

	mu          sync.Mutex
	xuid        string
	gamertag    string
	advertised  bool
	checkedAt   time.Time
	attemptedAt time.Time
}

// checked records a check at at; answered is false when Xbox returned an error.
func (h *healthState) checked(answered, advertised bool, at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.attemptedAt = at
	if answered {
		h.advertised, h.checkedAt = advertised, at
	}
}

func (h *healthState) snapshot() Health {
	h.mu.Lock()
	defer h.mu.Unlock()
	return Health{XUID: h.xuid, Gamertag: h.gamertag, Advertised: h.advertised, CheckedAt: h.checkedAt, AttemptedAt: h.attemptedAt}
}

// friendCount is the friend list size: exact at each full list, adjusted by adds and removals between lists.
// A nil friendCount, as in a standalone FriendSyncer, ignores every call.
type friendCount struct {
	mu        sync.Mutex
	friends   int
	countedAt time.Time
}

func (c *friendCount) set(n int) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.friends, c.countedAt = n, time.Now()
}

// needsList reports whether the syncer must list friends for the count alone; expiry
// scans already re-count, so a known count is only refreshed when expiry is off.
func (c *friendCount) needsList(expiryOn bool) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.countedAt.IsZero() || (!expiryOn && time.Since(c.countedAt) > friendCountRefresh)
}

// add ignores a count no full list has established yet.
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
