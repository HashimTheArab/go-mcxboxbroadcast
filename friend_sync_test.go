package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

func TestFriendSyncerAcceptsPendingRequestsAndInvites(t *testing.T) {
	var invited []string
	history := &syncHistoryStore{lastSeen: map[string]time.Time{"1": time.Now().Add(-30 * 24 * time.Hour)}}
	syncer := FriendSyncer{
		Client: &syncFriendClient{accept: func(context.Context) ([]Person, error) {
			return []Person{{XUID: "1", Gamertag: "One"}}, nil
		}},
		History: history,
		Inviter: fakeSyncInviter{invite: func(xuid string) { invited = append(invited, xuid) }},
		Config:  FriendSyncConfig{AutoFriend: true, InitialInvite: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(invited, []string{"1"}) {
		t.Fatalf("invited = %v, want [1]", invited)
	}
	// Expiry counts from when the friendship started, not an older visit.
	if seen := history.lastSeen["1"]; time.Since(seen) > time.Minute {
		t.Fatalf("last seen = %v, want reset on add", seen)
	}
}

// Without expiry the syncer still counts friends, adjusts for accepts, and re-lists an old count.
func TestFriendSyncerCountsFriendsWithoutExpiry(t *testing.T) {
	client := &syncFriendClient{
		people: []Person{{XUID: "1"}, {XUID: "2"}, {XUID: "3"}},
		accept: func(context.Context) ([]Person, error) { return []Person{{XUID: "4"}}, nil },
	}
	count := &friendCount{}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}, friendCount: count}
	for range 2 {
		if err := syncer.Sync(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if friends, _, _ := count.snapshot(); client.lists != 1 || friends != 4 {
		t.Fatalf("lists = %d, friends = %d; want one list and 3 listed + 1 accepted", client.lists, friends)
	}
	// An old count is listed again, so friends who left are dropped.
	count.countedAt = time.Now().Add(-friendCountRefresh - time.Minute)
	client.people = client.people[:2]
	client.accept = nil
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if friends, _, _ := count.snapshot(); client.lists != 2 || friends != 2 {
		t.Fatalf("after refresh lists = %d, friends = %d; want 2 and 2", client.lists, friends)
	}
}

// A failing list taken only for the count is not a sync error and waits before retrying.
func TestFriendSyncerCountFailureIsQuiet(t *testing.T) {
	client := &failingFriendsClient{}
	count := &friendCount{}
	syncer := FriendSyncer{Client: client, friendCount: count}
	for range 2 {
		if err := syncer.Sync(context.Background()); err != nil {
			t.Fatalf("count-only list failure surfaced: %v", err)
		}
	}
	if client.lists != 1 {
		t.Fatalf("lists = %d, want one attempt before the retry delay", client.lists)
	}
}

type failingFriendsClient struct {
	syncFriendClient
}

func (c *failingFriendsClient) Friends(context.Context) ([]Person, error) {
	c.lists++
	return nil, errors.New("peoplehub unavailable")
}

func TestFriendSyncerSkipsPendingRequestsWithoutAutoFriend(t *testing.T) {
	client := &syncFriendClient{accept: func(context.Context) ([]Person, error) {
		t.Fatal("accepted requests with autoFriend off")
		return nil, nil
	}}
	if err := (FriendSyncer{Client: client}).Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// People the bulk accept could not update are retried alone; restricted ones are declined and reported.
func TestFriendSyncerDeclinesRestrictedRequests(t *testing.T) {
	var notified []string
	client := &syncFriendClient{
		accept: func(context.Context) ([]Person, error) {
			return nil, &AcceptFriendRequestsError{Failed: []Person{{XUID: "2", Gamertag: "Kid"}, {XUID: "3", Gamertag: "Ok"}}}
		},
		add: func(_ context.Context, xuid string) error {
			if xuid == "2" {
				return &xblsocial.ResponseError{StatusCode: 403, Code: 1049}
			}
			return nil
		},
	}
	syncer := FriendSyncer{
		Client:   client,
		Config:   FriendSyncConfig{AutoFriend: true},
		Notifier: fakeNotifier{notify: func(_ context.Context, m string) { notified = append(notified, m) }},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(client.added, []string{"2", "3"}) || !slices.Equal(client.removed, []string{"2"}) {
		t.Fatalf("added = %v removed = %v, want both retried and 2 declined", client.added, client.removed)
	}
	if len(notified) != 1 || !strings.Contains(notified[0], "Kid (2)") {
		t.Fatalf("notifications = %q", notified)
	}
}

func TestFriendSyncerStopsRetriesWhenFriendListIsFull(t *testing.T) {
	client := &syncFriendClient{
		accept: func(context.Context) ([]Person, error) {
			return nil, &AcceptFriendRequestsError{Failed: []Person{{XUID: "1"}, {XUID: "2"}, {XUID: "3"}}}
		},
		add: func(context.Context, string) error {
			return &xblsocial.ResponseError{StatusCode: 403, Code: 1028}
		},
	}
	var result friendSyncResult
	FriendSyncer{Client: client}.acceptPending(context.Background(), friendSyncOptions{}, &result)
	if len(result.fullPlayers) != fullListPlayers || len(client.added) != fullListPlayers {
		t.Fatalf("refused=%v added=%v, want the pass to stop after %d refusals", result.fullPlayers, client.added, fullListPlayers)
	}
}

// One player's full list must not stop the bot accepting everyone else, and their request waits.
func TestFriendSyncerSkipsPlayerWithFullList(t *testing.T) {
	client := &syncFriendClient{
		accept: func(context.Context) ([]Person, error) {
			return nil, &AcceptFriendRequestsError{Failed: []Person{{XUID: "full"}, {XUID: "2"}, {XUID: "3"}}}
		},
		add: func(_ context.Context, xuid string) error {
			if xuid == "full" {
				return &xblsocial.ResponseError{StatusCode: 403, Code: 1028}
			}
			return nil
		},
	}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	now := time.Now()
	for pass := range 2 {
		result, err := syncer.syncWithOptions(context.Background(), state.options(now, false))
		if err != nil {
			t.Fatal(err)
		}
		if state.record(now, result) {
			t.Fatalf("pass %d: one player's full list paused the bot", pass)
		}
	}
	if !slices.Equal(client.added, []string{"full", "2", "3", "2", "3"}) {
		t.Fatalf("attempts = %v, want the full player skipped on the second pass", client.added)
	}
	if opts := state.options(now, false); !opts.add {
		t.Fatal("adds paused for one player's full list")
	}
}

func TestFriendSyncerExpiresInactiveFriendsOnly(t *testing.T) {
	old := time.Now().Add(-48 * time.Hour)
	history := &syncHistoryStore{lastSeen: map[string]time.Time{"old": old, "recent": time.Now()}}
	client := &syncFriendClient{people: []Person{{XUID: "old"}, {XUID: "recent"}, {XUID: "new"}, {XUID: "4503599627370497"}}}
	syncer := FriendSyncer{
		Client:  client,
		History: history,
		Config:  FriendSyncConfig{ExpiryEnabled: true, ExpiryDays: 1},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(client.removed, []string{"old"}) {
		t.Fatalf("removed = %v, want [old]", client.removed)
	}
	if _, ok := history.lastSeen["old"]; ok {
		t.Fatal("expired friend's history was kept")
	}
	if _, ok := history.lastSeen["new"]; !ok {
		t.Fatal("new friend's expiry clock was not started")
	}
}

func TestFriendSyncerDefaultsExpiryDays(t *testing.T) {
	client := &syncFriendClient{people: []Person{{XUID: "1"}}}
	syncer := FriendSyncer{
		Client:  client,
		History: &syncHistoryStore{lastSeen: map[string]time.Time{"1": time.Now().Add(-24 * time.Hour)}},
		Config:  FriendSyncConfig{ExpiryEnabled: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(client.removed) != 0 {
		t.Fatal("zero expiry days should default instead of removing recent friends")
	}
}

func TestFriendSyncerStopsExpiryWhenRemovalsAreRateLimited(t *testing.T) {
	old := time.Now().Add(-48 * time.Hour)
	client := &syncFriendClient{
		people: []Person{{XUID: "1"}, {XUID: "2"}},
		remove: func(context.Context, string) error {
			return &xblsocial.ResponseError{StatusCode: 429, RetryAfter: time.Minute}
		},
	}
	syncer := FriendSyncer{
		Client:  client,
		History: &syncHistoryStore{lastSeen: map[string]time.Time{"1": old, "2": old}},
		Config:  FriendSyncConfig{ExpiryEnabled: true, ExpiryDays: 1},
	}
	result, err := syncer.syncWithOptions(context.Background(), friendSyncOptions{expire: true, remove: true})
	if err != nil {
		t.Fatal(err)
	}
	if result.removeRetryAfter != time.Minute || len(client.removed) != 1 {
		t.Fatalf("retry=%s removals=%v, want one attempt and a minute backoff", result.removeRetryAfter, client.removed)
	}
}

func TestFriendSyncerPrunesHistoryForExFriends(t *testing.T) {
	history := &syncHistoryStore{lastSeen: map[string]time.Time{"gone": time.Now(), "kept": time.Now()}}
	syncer := FriendSyncer{
		Client:  &syncFriendClient{people: []Person{{XUID: "kept"}}},
		History: history,
		Config:  FriendSyncConfig{ExpiryEnabled: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := history.lastSeen["gone"]; ok {
		t.Fatal("history for an ex-friend was not pruned")
	}
	if _, ok := history.lastSeen["kept"]; !ok {
		t.Fatal("history for a current friend was pruned")
	}
}

func TestFriendSyncerLogsInitialInviteFailure(t *testing.T) {
	var log bytes.Buffer
	syncer := FriendSyncer{
		Client: &syncFriendClient{accept: func(context.Context) ([]Person, error) {
			return []Person{{XUID: "1", Gamertag: "One"}}, nil
		}},
		Inviter: fakeSyncInviter{err: errors.New("invite failed")},
		Config:  FriendSyncConfig{AutoFriend: true, InitialInvite: true},
		Log:     slog.New(slog.NewTextHandler(&log, nil)),
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(log.String(), "send initial invite") {
		t.Fatalf("invite failure not logged: %q", log.String())
	}
}

func TestFriendSyncRunStateTracksSeparateAddAndRemoveBackoff(t *testing.T) {
	now := time.Now()
	var state friendSyncRunState
	state.record(now, friendSyncResult{addRetryAfter: time.Minute})
	if opts := state.options(now.Add(time.Second), false); opts.add || !opts.remove {
		t.Fatalf("options = %+v, want adds paused and removals running", opts)
	}
	if !state.record(now, friendSyncResult{fullPlayers: []string{"1", "2"}}) {
		t.Fatal("two refused players with no accept did not mark the list full")
	}
	if opts := state.options(now.Add(2*time.Minute), false); opts.add {
		t.Fatal("adds resumed while the friend list is full")
	}
	if opts := state.options(now.Add(friendListFullBackoff), false); !opts.add {
		t.Fatal("adds did not resume after the full-list backoff")
	}
}

// Refusals separated by a successful accept are separate players' full lists, not the bot's.
func TestFriendSyncRunStateNeedsRefusalsSinceLastAccept(t *testing.T) {
	now := time.Now()
	var state friendSyncRunState
	if state.record(now, friendSyncResult{fullPlayers: []string{"1"}}) {
		t.Fatal("one refusal marked the list full")
	}
	if state.record(now.Add(time.Minute), friendSyncResult{accepted: 1}) {
		t.Fatal("an accept marked the list full")
	}
	if state.record(now.Add(2*time.Minute), friendSyncResult{fullPlayers: []string{"2"}}) {
		t.Fatal("refusals on both sides of an accept marked the list full")
	}
}

// TestFriendSyncerRunSyncsOnTrigger checks that events can run ahead of polling.
func TestFriendSyncerRunSyncsOnTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		accepts := 0
		client := &syncFriendClient{
			accept: func(context.Context) ([]Person, error) {
				accepts++
				return nil, nil
			},
		}
		trigger := make(chan struct{}, 1)
		syncer := FriendSyncer{
			Client:  client,
			Config:  FriendSyncConfig{AutoFriend: true, UpdateInterval: time.Hour},
			Trigger: trigger,
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go syncer.Run(ctx)
		synctest.Wait()
		if accepts != 1 {
			t.Fatalf("initial passes = %d, want 1", accepts)
		}
		time.Sleep(30 * time.Second)
		trigger <- struct{}{}
		synctest.Wait()
		if accepts != 2 {
			t.Fatalf("passes after event = %d, want 2", accepts)
		}
	})
}

// A closed Trigger must not spin Run; a spinning loop never lets the bubble's clock advance.
func TestFriendSyncerRunHandlesClosedTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var accepts atomic.Int32
		trigger := make(chan struct{})
		syncer := FriendSyncer{
			Client: &syncFriendClient{accept: func(context.Context) ([]Person, error) {
				accepts.Add(1)
				return nil, nil
			}},
			Config:  FriendSyncConfig{AutoFriend: true, UpdateInterval: time.Hour},
			Trigger: trigger,
		}
		go syncer.Run(t.Context())
		synctest.Wait()
		close(trigger)
		time.Sleep(time.Minute)
		synctest.Wait()
		if got := accepts.Load(); got != 1 {
			t.Fatalf("passes = %d, want only the initial one", got)
		}
	})
}

// A social event during add backoff must not accept requests early, while expiry keeps running.
func TestReactiveFriendSyncPreservesAddBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var accepts, expiries atomic.Int32
		client := &syncFriendClient{
			people: []Person{{XUID: "123"}},
			accept: func(context.Context) ([]Person, error) {
				accepts.Add(1)
				return nil, &xblsocial.ResponseError{StatusCode: 429, RetryAfter: time.Minute}
			},
		}
		trigger := make(chan struct{}, 1)
		syncer := FriendSyncer{
			Client: client,
			History: &syncHistoryStore{
				lastSeen:   map[string]time.Time{"123": time.Now().Add(-48 * time.Hour)},
				onLastSeen: func(string) { expiries.Add(1) },
			},
			Config: FriendSyncConfig{AutoFriend: true, ExpiryEnabled: true, ExpiryDays: 1,
				UpdateInterval: time.Hour, ExpiryCheck: 20 * time.Second},
			Trigger: trigger,
		}
		go syncer.Run(t.Context())
		synctest.Wait()
		if accepts.Load() != 1 || expiries.Load() != 1 || len(client.removals()) != 1 {
			t.Fatalf("initial accepts=%d expiries=%d removals=%v, want one each", accepts.Load(), expiries.Load(), client.removals())
		}
		trigger <- struct{}{}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if accepts.Load() != 1 || expiries.Load() != 2 {
			t.Fatalf("during backoff accepts=%d expiries=%d, want 1 and 2", accepts.Load(), expiries.Load())
		}
		time.Sleep(40 * time.Second)
		trigger <- struct{}{}
		synctest.Wait()
		if accepts.Load() != 2 {
			t.Fatalf("after backoff accepts=%d, want 2", accepts.Load())
		}
	})
}

func TestFriendSyncReadRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name, url, retry string
		status           int
		delay            time.Duration
		initialReads     int
	}{
		{"pending", pendingRequestsURL, "45", 429, 45 * time.Second, 1},
		{"friends", peopleHubFriendsURL, "45", 429, 45 * time.Second, 2},
		{"no retry header", peopleHubFriendsURL, "", 429, 20 * time.Second, 2},
		{"short retry header", pendingRequestsURL, "5", 429, 20 * time.Second, 1},
		{"service unavailable", pendingRequestsURL, "30", 503, 30 * time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reads := make(chan timedFriendRead, 64)
				failed := false
				client := newFriendRateClient(reads, func(req *http.Request) *http.Response {
					if req.URL.String() == tc.url && !failed {
						failed = true
						resp := response(tc.status, "")
						resp.Header.Set("Retry-After", tc.retry)
						return resp
					}
					return response(200, `{"people":[]}`)
				})
				trigger := make(chan struct{}, 16)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go newRateSyncer(client, trigger).Run(ctx)
				synctest.Wait()
				assertFriendRateReads(t, reads, 0, friendRateReadURLs[:tc.initialReads]...)
				for range cap(trigger) {
					trigger <- struct{}{}
				}
				synctest.Wait()
				time.Sleep(tc.delay - time.Nanosecond)
				synctest.Wait()
				assertFriendRateReads(t, reads, 0)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assertFriendRateReads(t, reads, tc.delay, friendRateReadURLs...)
				time.Sleep(20 * time.Second)
				synctest.Wait()
				assertFriendRateReads(t, reads, 0)
			})
		})
	}
}

func TestFriendSyncReadRetryRunsWithoutAnotherTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reads := make(chan timedFriendRead, 16)
		first := true
		client := newFriendRateClient(reads, func(*http.Request) *http.Response {
			if first {
				first = false
				resp := response(429, "")
				resp.Header.Set("Retry-After", "40")
				return resp
			}
			return response(200, `{"people":[]}`)
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go newRateSyncer(client, nil).Run(ctx)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0, pendingRequestsURL)
		time.Sleep(40 * time.Second)
		synctest.Wait()
		assertFriendRateReads(t, reads, 40*time.Second, friendRateReadURLs...)
	})
}

func TestFriendSyncTriggerSpacingStartsAfterPassCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reads := make(chan timedFriendRead, 64)
		delayed := false
		client := newFriendRateClient(reads, func(req *http.Request) *http.Response {
			if req.URL.String() == peopleHubFriendsURL && !delayed {
				delayed = true
				time.Sleep(5 * time.Second)
			}
			return response(200, `{"people":[]}`)
		})
		trigger := make(chan struct{}, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go newRateSyncer(client, trigger).Run(ctx)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0, friendRateReadURLs...)
		for range cap(trigger) {
			trigger <- struct{}{}
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		time.Sleep(20*time.Second - time.Nanosecond)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assertFriendRateReads(t, reads, 25*time.Second, pendingRequestsURL)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0)
		trigger <- struct{}{}
		synctest.Wait()
		assertFriendRateReads(t, reads, 45*time.Second, pendingRequestsURL)
		trigger <- struct{}{}
		synctest.Wait()
		close(trigger)
		cancel()
		synctest.Wait()
		time.Sleep(time.Minute)
		assertFriendRateReads(t, reads, 0)
	})
}

func TestFriendSyncCoalescesPollExpiryAndTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		passes := make(chan timedFriendRead, 16)
		expiries := make(chan timedFriendRead, 16)
		start := time.Now()
		client := &syncFriendClient{people: []Person{{XUID: "123"}},
			accept: func(context.Context) ([]Person, error) {
				passes <- timedFriendRead{"pass", time.Since(start)}
				return nil, nil
			}}
		trigger := make(chan struct{}, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		history := &syncHistoryStore{
			lastSeen:   map[string]time.Time{"123": time.Now().Add(time.Hour)},
			onLastSeen: func(string) { expiries <- timedFriendRead{"expiry", time.Since(start)} },
		}
		go (FriendSyncer{Client: client, Trigger: trigger, History: history,
			Config: FriendSyncConfig{AutoFriend: true, ExpiryEnabled: true,
				UpdateInterval: 20 * time.Second, ExpiryCheck: 20 * time.Second}}).Run(ctx)
		synctest.Wait()
		assertFriendRateReads(t, passes, 0, "pass")
		assertFriendRateReads(t, expiries, 0, "expiry")
		time.Sleep(19 * time.Second)
		for range cap(trigger) {
			trigger <- struct{}{}
		}
		synctest.Wait()
		assertFriendRateReads(t, passes, 0)
		for _, at := range []time.Duration{20 * time.Second, 40 * time.Second} {
			time.Sleep(at - time.Since(start))
			synctest.Wait()
			assertFriendRateReads(t, passes, at, "pass")
			assertFriendRateReads(t, expiries, at, "expiry")
		}
	})
}

// friendRateReadURLs lists the reads in an empty pass that also checks expiry.
var friendRateReadURLs = []string{pendingRequestsURL, peopleHubFriendsURL}

// newRateSyncer starts with an expiry pass and then only accepts requests for an hour.
func newRateSyncer(client FriendAPI, trigger <-chan struct{}) FriendSyncer {
	return FriendSyncer{Client: client, Trigger: trigger, History: &syncHistoryStore{}, Config: FriendSyncConfig{
		AutoFriend: true, ExpiryEnabled: true, UpdateInterval: time.Hour, ExpiryCheck: time.Hour}}
}

// timedFriendRead records the request and elapsed time at the transport boundary.
type timedFriendRead struct {
	url string
	at  time.Duration
}

// newFriendRateClient keeps the real social API/error parsing with a local transport.
func newFriendRateClient(reads chan<- timedFriendRead, respond func(*http.Request) *http.Response) FriendClient {
	start := time.Now()
	return FriendClient{Social: newTestSocialClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		reads <- timedFriendRead{req.URL.String(), time.Since(start)}
		resp := respond(req)
		resp.Request = req
		return resp, nil
	})})}
}

// assertFriendRateReads checks all requests since the previous assertion, including timing.
func assertFriendRateReads(t *testing.T, reads <-chan timedFriendRead, at time.Duration, urls ...string) {
	t.Helper()
	for _, url := range urls {
		select {
		case got := <-reads:
			if got.url != url || got.at != at {
				t.Fatalf("request = %+v, want %s at %s", got, url, at)
			}
		default:
			t.Fatalf("missing request %s at %s", url, at)
		}
	}
	select {
	case extra := <-reads:
		t.Fatalf("unexpected request %+v", extra)
	default:
	}
}

type syncFriendClient struct {
	people []Person
	accept func(context.Context) ([]Person, error)
	add    func(context.Context, string) error
	remove func(context.Context, string) error

	mu      sync.Mutex
	added   []string
	removed []string
	lists   int
}

func (c *syncFriendClient) removals() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.removed)
}

func (c *syncFriendClient) Friends(context.Context) ([]Person, error) {
	c.mu.Lock()
	c.lists++
	c.mu.Unlock()
	return c.people, nil
}

func (c *syncFriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	if c.accept != nil {
		return c.accept(ctx)
	}
	return nil, nil
}

func (c *syncFriendClient) AddFriend(ctx context.Context, xuid string) error {
	c.mu.Lock()
	c.added = append(c.added, xuid)
	c.mu.Unlock()
	if c.add != nil {
		return c.add(ctx, xuid)
	}
	return nil
}

func (c *syncFriendClient) RemoveFriend(ctx context.Context, xuid string) error {
	c.mu.Lock()
	c.removed = append(c.removed, xuid)
	c.mu.Unlock()
	if c.remove != nil {
		return c.remove(ctx, xuid)
	}
	return nil
}

type fakeSyncInviter struct {
	invite func(string)
	err    error
}

func (f fakeSyncInviter) Invite(_ context.Context, xuid string) error {
	if f.invite != nil {
		f.invite(xuid)
	}
	return f.err
}
