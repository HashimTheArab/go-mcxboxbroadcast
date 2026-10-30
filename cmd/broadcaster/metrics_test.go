package main

import (
	"strings"
	"testing"
	"time"

	"github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Unknown friend counts are omitted rather than exported as zero, and joins carry the primary account.
func TestHealthCollectorExportsAccounts(t *testing.T) {
	now := time.Unix(10_000, 0)
	collector := healthCollector{now: func() time.Time { return now }, health: func() broadcaster.Health {
		return broadcaster.Health{
			Version:      "1.26.60",
			Protocol:     2225,
			RelayedJoins: 7,
			Accounts: []broadcaster.AccountHealth{
				{XUID: "1", Gamertag: "Primary", Advertised: true, CheckedAt: now, Friends: 812, FriendsCheckedAt: now},
				{ID: "sub", XUID: "2", Gamertag: "Sub"},
			},
		}
	}}
	want := `
# HELP mcxboxbroadcast_friend_limit Xbox friend list limit.
# TYPE mcxboxbroadcast_friend_limit gauge
mcxboxbroadcast_friend_limit{gamertag="Primary",xuid="1"} 1000
mcxboxbroadcast_friend_limit{gamertag="Sub",xuid="2"} 1000
# HELP mcxboxbroadcast_friends Friends on the account's Xbox friend list.
# TYPE mcxboxbroadcast_friends gauge
mcxboxbroadcast_friends{gamertag="Primary",xuid="1"} 812
# HELP mcxboxbroadcast_relayed_joins_total Join offers relayed to the target since start, labelled with the primary account.
# TYPE mcxboxbroadcast_relayed_joins_total counter
mcxboxbroadcast_relayed_joins_total{gamertag="Primary",xuid="1"} 7
# HELP mcxboxbroadcast_session_advertised Whether the account's own session was recently found open. Says nothing about another player's access.
# TYPE mcxboxbroadcast_session_advertised gauge
mcxboxbroadcast_session_advertised{gamertag="Primary",protocol="2225",version="1.26.60",xuid="1"} 1
mcxboxbroadcast_session_advertised{gamertag="Sub",protocol="2225",version="1.26.60",xuid="2"} 0
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}
