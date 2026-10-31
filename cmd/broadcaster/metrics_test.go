package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	broadcaster "github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// An unknown friend count is omitted rather than exported as zero.
func TestHealthCollectorExportsAccount(t *testing.T) {
	now := time.Unix(10_000, 0)
	health := broadcaster.Health{XUID: "1", Gamertag: "Bot", Version: "1.26.60", Protocol: 2225, Advertised: true, CheckedAt: now, AttemptedAt: now, RelayedJoins: 7}
	collector := healthCollector{now: func() time.Time { return now }, health: func() broadcaster.Health { return health }}
	header := `
# HELP mcxboxbroadcast_friend_list_full 1 while Xbox refuses the account's friend requests because its own list is full.
# TYPE mcxboxbroadcast_friend_list_full gauge
mcxboxbroadcast_friend_list_full{gamertag="Bot",xuid="1"} 0
# HELP mcxboxbroadcast_relayed_joins_total Join offers relayed to the target since start.
# TYPE mcxboxbroadcast_relayed_joins_total counter
mcxboxbroadcast_relayed_joins_total{gamertag="Bot",xuid="1"} 7
# HELP mcxboxbroadcast_session_advertised Whether the account's own session was recently found open. Says nothing about another player's access.
# TYPE mcxboxbroadcast_session_advertised gauge
mcxboxbroadcast_session_advertised{gamertag="Bot",protocol="2225",version="1.26.60",xuid="1"} 1
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(header)); err != nil {
		t.Fatal(err)
	}
	health.Friends, health.FriendsCheckedAt = 812, now
	withFriends := header + `# HELP mcxboxbroadcast_friends Friends on the account's Xbox friend list.
# TYPE mcxboxbroadcast_friends gauge
mcxboxbroadcast_friends{gamertag="Bot",xuid="1"} 812
`
	if err := testutil.CollectAndCompare(collector, strings.NewReader(withFriends)); err != nil {
		t.Fatal(err)
	}
	// Before the session loop knows the account, nothing is exported.
	health = broadcaster.Health{}
	if n := testutil.CollectAndCount(collector); n != 0 {
		t.Fatalf("metrics before identity = %d, want 0", n)
	}
}

// serveMetrics answers scrapes on its address and fails at once when the address is taken.
func TestServeMetrics(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	health := func() broadcaster.Health { return broadcaster.Health{XUID: "1", Gamertag: "Bot", RelayedJoins: 3} }
	stop, err := serveMetrics(t.Context(), "127.0.0.1:0", health, log)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	addr := regexp.MustCompile(`addr=(\S+)`).FindStringSubmatch(logs.String())
	if addr == nil {
		t.Fatalf("listen address not logged: %s", logs.String())
	}
	resp, err := http.Get("http://" + addr[1] + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `mcxboxbroadcast_relayed_joins_total{gamertag="Bot",xuid="1"} 3`) {
		t.Fatalf("GET /metrics = %s:\n%s", resp.Status, body)
	}
	if _, err := serveMetrics(t.Context(), addr[1], health, log); err == nil {
		t.Fatal("serveMetrics bound an address already in use")
	}
}
