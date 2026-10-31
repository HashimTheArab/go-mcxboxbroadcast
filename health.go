package broadcaster

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/df-mc/go-xsapi/v2"
)

// friendCountRefresh re-lists friends when expiry is off, so removals by other players are seen.
const friendCountRefresh = 30 * time.Minute

// friendCountRetry spaces out a failing list taken only for the count, which must not spam sync errors.
const friendCountRetry = 5 * time.Minute

// advertisedFreshness is how long checks may stop before the gauge drops. It allows for the
// session loop running slow updates or rotations between checks.
const advertisedFreshness = 5 * activityCheckInterval

// answerTolerance bounds how long Xbox errors may keep the last answer.
const answerTolerance = 15 * time.Minute

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
	FriendListFull   bool   // Xbox is refusing friend requests because the account's own list is full
	RelayedJoins     uint64 // join offers answered since start
}

// AdvertisedAt reports whether, at now, checks are still running and their last answer, recent
// enough to survive brief Xbox errors, found the session open.
func (h Health) AdvertisedAt(now time.Time) bool {
	return h.Advertised && !h.CheckedAt.IsZero() &&
		now.Sub(h.AttemptedAt) <= advertisedFreshness && now.Sub(h.CheckedAt) <= answerTolerance
}

// Health returns a snapshot of the broadcaster's account. It never waits on the lifecycle
// lock, which Update holds across Xbox calls; the identity is empty until Start succeeds.
func (b *Broadcaster) Health() Health {
	health := b.health.snapshot()
	health.Friends, health.FriendsCheckedAt, health.FriendListFull = b.health.friends.snapshot()
	health.Version, health.Protocol = b.advertisedVersion(), b.advertisedProtocol()
	health.RelayedJoins = b.health.joins.Load()
	return health
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

// setIdentity records the account once; it never changes afterwards, so series labels stay stable.
func (h *healthState) setIdentity(xuid, gamertag string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.xuid == "" {
		h.xuid, h.gamertag = xuid, gamertag
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
	mu         sync.Mutex
	friends    int
	countedAt  time.Time
	retryAfter time.Time // after a failed count-only list
	full       bool      // the syncer's own-list-full backoff is active
}

func (c *friendCount) setFull(full bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.full = full
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
	if time.Now().Before(c.retryAfter) {
		return false
	}
	return c.countedAt.IsZero() || (!expiryOn && time.Since(c.countedAt) > friendCountRefresh)
}

// listFailed spaces out retries of a list taken only for the count.
func (c *friendCount) listFailed() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retryAfter = time.Now().Add(friendCountRetry)
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

func (c *friendCount) snapshot() (int, time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.friends, c.countedAt, c.full
}
