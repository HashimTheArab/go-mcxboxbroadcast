package broadcaster

import (
	"slices"
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

// Health is the broadcaster's own view of what it publishes. It cannot prove
// that another account can see or join a session; Xbox decides that per viewer.
type Health struct {
	Version  string
	Protocol int32
	// RelayedJoins counts join offers answered since start. Sub-accounts share
	// the primary's signaling, so joins are not attributed per account.
	RelayedJoins uint64
	Accounts     []AccountHealth
}

// AccountHealth describes one published account. Zero times mean not yet known.
type AccountHealth struct {
	ID               string // empty for the primary account
	XUID             string
	Gamertag         string
	Advertised       bool // whether the last own-handle check found the session open
	CheckedAt        time.Time
	Friends          int
	FriendsCheckedAt time.Time
}

// AdvertisedAt reports whether the account's session was found open recently enough to trust at now.
func (a AccountHealth) AdvertisedAt(now time.Time) bool {
	return a.Advertised && !a.CheckedAt.IsZero() && now.Sub(a.CheckedAt) <= advertisedFreshness
}

// Health returns a snapshot of the broadcaster's published accounts. It never waits on the
// lifecycle lock, which Update holds across Xbox calls; accounts appear once the session loop runs.
func (b *Broadcaster) Health() Health {
	accounts := b.health.identities()
	for i := range accounts {
		b.health.account(accounts[i].ID).fill(&accounts[i])
	}
	return Health{
		Version:      b.advertisedVersion(),
		Protocol:     b.advertisedProtocol(),
		RelayedJoins: b.health.joins.Load(),
		Accounts:     accounts,
	}
}

// recordHealthAccounts publishes the account identities Health reports, primary first.
func (b *Broadcaster) recordHealthAccounts() {
	b.mu.Lock()
	accounts := []AccountHealth{{XUID: b.primaryXUID(), Gamertag: clientGamertag(b.conf.XBLClient, b.xblClient)}}
	subs, _ := b.enabledSubAccounts()
	for _, account := range subs {
		accounts = append(accounts, AccountHealth{ID: account.ID, XUID: accountXUID(*account), Gamertag: clientGamertag(account.XBLClient)})
	}
	b.mu.Unlock()
	b.health.setIdentities(accounts)
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

// healthState collects what Health reports, keyed by account ID.
type healthState struct {
	joins    atomic.Uint64
	mu       sync.Mutex
	accounts map[string]*accountHealthState
	ids      []AccountHealth // identity fields only, primary first
}

func (h *healthState) setIdentities(ids []AccountHealth) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ids = ids
}

func (h *healthState) identities() []AccountHealth {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.ids)
}

func (h *healthState) account(id string) *accountHealthState {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.accounts == nil {
		h.accounts = make(map[string]*accountHealthState)
	}
	state, ok := h.accounts[id]
	if !ok {
		state = &accountHealthState{}
		h.accounts[id] = state
	}
	return state
}

// accountHealthState is one account's latest advertisement check and friend count.
// Friend counts are exact at each full list and adjusted by adds and removals between lists.
type accountHealthState struct {
	mu         sync.Mutex
	advertised bool
	checkedAt  time.Time
	friends    int
	friendsAt  time.Time
}

func (s *accountHealthState) checked(advertised bool, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advertised, s.checkedAt = advertised, at
}

// setFriends records a full friend list; nil s is a no-op for standalone syncers.
func (s *accountHealthState) setFriends(n int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.friends, s.friendsAt = n, time.Now()
}

// needsFriends reports whether the count is missing or older than friendCountRefresh; nil s never needs one.
func (s *accountHealthState) needsFriends() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.friendsAt.IsZero() || time.Since(s.friendsAt) > friendCountRefresh
}

// addFriends adjusts a known count between full lists.
func (s *accountHealthState) addFriends(delta int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.friendsAt.IsZero() {
		s.friends = max(s.friends+delta, 0)
	}
}

func (s *accountHealthState) fill(a *AccountHealth) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a.Advertised, a.CheckedAt = s.advertised, s.checkedAt
	a.Friends, a.FriendsCheckedAt = s.friends, s.friendsAt
}
