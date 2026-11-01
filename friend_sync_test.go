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
		add: func(_ context.Context, xuid string) error {
			if xuid == "full" {
				return &xblsocial.ResponseError{StatusCode: 403, Code: 1028}
			}
			return nil
		},
	}
	client.accept = func(context.Context) ([]Person, error) {
		return nil, &AcceptFriendRequestsError{Failed: client.pending("full", "2", "3")}
	}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	now := time.Now()
	for pass := range 2 {
		result, err := syncer.syncWithOptions(context.Background(), state.options(now, false))
		if err != nil {
			t.Fatal(err)
		}
		state.record(now, result)
		if state.full {
			t.Fatalf("pass %d: one player's full list marked the bot full", pass)
		}
	}
	if !slices.Equal(client.added, []string{"full", "2", "3", "2", "3"}) {
		t.Fatalf("attempts = %v, want the full player skipped on the second pass", client.added)
	}
	if opts := state.options(now, false); !opts.add {
		t.Fatal("adds paused for one player's full list")
	}
}

// A delay Xbox puts on refused players holds only them; a 429 still pauses every add.
func TestFriendSyncerDelayHoldsOnlyRefusedPlayers(t *testing.T) {
	client := &syncFriendClient{
		add: func(_ context.Context, xuid string) error {
			switch xuid {
			case "slow":
				return &xblsocial.ResponseError{StatusCode: http.StatusBadRequest, RetryAfter: 2 * time.Hour}
			case "limited":
				return &xblsocial.ResponseError{StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute}
			}
			return nil
		},
	}
	refused := true
	client.accept = func(context.Context) ([]Person, error) {
		if refused {
			return nil, &AcceptFriendRequestsError{Failed: client.pending("held"), Waits: map[string]time.Duration{"held": 12 * time.Hour}}
		}
		return nil, &AcceptFriendRequestsError{Failed: client.pending("held", "slow", "ok")}
	}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	now := time.Now()
	pass := func() {
		t.Helper()
		result, err := syncer.syncWithOptions(context.Background(), state.options(now, false))
		if err != nil {
			t.Fatal(err)
		}
		state.record(now, result)
	}

	pass()
	if len(client.added) != 0 {
		t.Fatalf("retried %v one by one, want the refused batch's delay honoured", client.added)
	}
	if !state.options(now, false).add {
		t.Fatal("one player's delay paused every add")
	}
	refused = false
	pass()
	pass()
	if !slices.Equal(client.added, []string{"slow", "ok", "ok"}) {
		t.Fatalf("attempts = %v, want held skipped and slow tried once", client.added)
	}
	if !state.options(now, false).add {
		t.Fatal("one player's delay paused every add")
	}

	client.accept = func(context.Context) ([]Person, error) {
		return nil, &AcceptFriendRequestsError{Failed: client.pending("limited")}
	}
	pass()
	if state.options(now, false).add {
		t.Fatal("a 429 must pause every add")
	}
}

// Only a 429 or a server fault on the bulk add pauses every add; other errors' delays do not.
func TestFriendSyncerBulkErrorPausesOnlyForAccountLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		err   error
		pause bool
	}{
		"coded 400":   {&xblsocial.ResponseError{Method: http.MethodPost, StatusCode: http.StatusBadRequest, Code: 1049, RetryAfter: 12 * time.Hour}, false},
		"uncoded 400": {&xblsocial.ResponseError{Method: http.MethodPost, StatusCode: http.StatusBadRequest, RetryAfter: 12 * time.Hour}, false},
		"503":         {&xblsocial.ResponseError{Method: http.MethodPost, StatusCode: http.StatusServiceUnavailable, RetryAfter: time.Minute}, true},
		"429":         {&xblsocial.ResponseError{Method: http.MethodPost, StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute}, true},
	} {
		t.Run(name, func(t *testing.T) {
			client := &syncFriendClient{accept: func(context.Context) ([]Person, error) { return nil, tc.err }}
			syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
			var state friendSyncRunState
			now := time.Now()
			result, _ := syncer.syncWithOptions(context.Background(), state.options(now, false))
			state.record(now, result)
			if paused := !state.options(now, false).add; paused != tc.pause {
				t.Fatalf("adds paused = %v, want %v", paused, tc.pause)
			}
		})
	}
}

// A player refused with no reason or delay waits refusedBackoff, then is asked again.
func TestFriendSyncerHoldsRefusedPlayerUntilBackoffEnds(t *testing.T) {
	client := &syncFriendClient{
		add: func(context.Context, string) error {
			return &xblsocial.ResponseError{StatusCode: http.StatusBadRequest}
		},
	}
	client.accept = func(context.Context) ([]Person, error) {
		return nil, &AcceptFriendRequestsError{Failed: client.pending("refused")}
	}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	start := time.Now()
	for _, at := range []time.Time{start, start.Add(time.Minute), start.Add(refusedBackoff)} {
		result, err := syncer.syncWithOptions(context.Background(), state.options(at, false))
		if err != nil {
			t.Fatal(err)
		}
		state.record(at, result)
	}
	if !slices.Equal(client.added, []string{"refused", "refused"}) {
		t.Fatalf("attempts = %v, want one before and one after the backoff", client.added)
	}
}

// An auth error on a one-by-one add is not about the player, so it does not hold them.
func TestFriendSyncerDoesNotHoldPlayerForAuthError(t *testing.T) {
	client := &syncFriendClient{
		add: func(context.Context, string) error {
			return &xblsocial.ResponseError{StatusCode: http.StatusUnauthorized}
		},
	}
	client.accept = func(context.Context) ([]Person, error) {
		return nil, &AcceptFriendRequestsError{Failed: client.pending("p")}
	}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	now := time.Now()
	result, _ := syncer.syncWithOptions(context.Background(), state.options(now, false))
	state.record(now, result)
	if _, held := state.options(now.Add(time.Minute), false).skip["p"]; held {
		t.Fatal("a 401 held the player")
	}
}

// A player held for a full list and an Xbox delay stays skipped until the later hold ends.
func TestFriendSyncRunStateSkipsUntilLaterHoldEnds(t *testing.T) {
	now := time.Now()
	var state friendSyncRunState
	state.record(now, friendSyncResult{
		accepted:    1,
		fullPlayers: []string{"p"},
		waits:       map[string]time.Duration{"p": 2 * time.Hour},
	})
	for _, tc := range []struct {
		at   time.Duration
		held bool
	}{{90 * time.Minute, true}, {2 * time.Hour, false}} {
		_, held := state.options(now.Add(tc.at), false).skip["p"]
		if held != tc.held {
			t.Fatalf("held at %s = %v, want %v", tc.at, held, tc.held)
		}
	}
	state.record(now.Add(2*time.Hour), friendSyncResult{})
	if len(state.waiting) != 0 || len(state.fullPlayers) != 0 {
		t.Fatalf("expired holds kept: waiting %v, full %v", state.waiting, state.fullPlayers)
	}
}

// Delays from earlier batches hold their players even when a later batch's rate limit pauses adds.
func TestFriendSyncerAppliesWaitsAndLaterBatchFailure(t *testing.T) {
	client := &syncFriendClient{accept: func(context.Context) ([]Person, error) {
		return nil, &AcceptFriendRequestsError{
			Failed: []Person{{XUID: "held"}},
			Waits:  map[string]time.Duration{"held": 12 * time.Hour},
			Err:    &xblsocial.ResponseError{Method: http.MethodPost, StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute},
		}
	}}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	now := time.Now()
	result, _ := syncer.syncWithOptions(context.Background(), state.options(now, false))
	state.record(now, result)
	if len(client.added) != 0 {
		t.Fatalf("retried %v after the rate limit", client.added)
	}
	later := state.options(now.Add(2*time.Minute), false)
	if _, held := later.skip["held"]; !held || !later.add || state.options(now, false).add {
		t.Fatalf("want adds paused for the rate limit only and held waiting 12h; add=%v held=%v", later.add, held)
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
}

// New players refused after the first pass, with no accept between, report the list full
// without pausing adds; the next accept clears it.
func TestFriendSyncRunStateReportsFullFromNewRefusals(t *testing.T) {
	now := time.Now()
	var state friendSyncRunState
	state.record(now, friendSyncResult{pendingRead: true})
	state.record(now, friendSyncResult{pendingRead: true, fullPlayers: []string{"1", "2"}})
	if !state.full {
		t.Fatal("two new refused players with no accept did not report the list full")
	}
	if !state.options(now, false).add {
		t.Fatal("a full report paused adds")
	}
	state.record(now, friendSyncResult{pendingRead: true, accepted: 1})
	if state.full {
		t.Fatal("an accept did not clear the full report")
	}
}

// Refusals separated by a successful accept are separate players' full lists, not the bot's.
func TestFriendSyncRunStateNeedsRefusalsSinceLastAccept(t *testing.T) {
	now := time.Now()
	var state friendSyncRunState
	state.record(now, friendSyncResult{pendingRead: true})
	state.record(now, friendSyncResult{pendingRead: true, fullPlayers: []string{"1"}})
	state.record(now.Add(time.Minute), friendSyncResult{pendingRead: true, accepted: 1})
	state.record(now.Add(2*time.Minute), friendSyncResult{pendingRead: true, fullPlayers: []string{"2"}})
	if state.full {
		t.Fatal("refusals on both sides of an accept marked the list full")
	}
}

// A first pass that could not read the pending list is not the baseline.
func TestFriendSyncerBaselineWaitsForPendingRead(t *testing.T) {
	readFails := true
	client := &syncFriendClient{
		add: func(context.Context, string) error { return &xblsocial.ResponseError{StatusCode: 403, Code: 1028} },
	}
	client.accept = func(context.Context) ([]Person, error) {
		if readFails {
			return nil, &xblsocial.ResponseError{Method: http.MethodGet, StatusCode: http.StatusServiceUnavailable}
		}
		return nil, &AcceptFriendRequestsError{Failed: client.pending("chronic1", "chronic2")}
	}
	syncer := FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFriend: true}}
	var state friendSyncRunState
	syncer.runSync(t.Context(), &state, false)
	readFails = false
	syncer.runSync(t.Context(), &state, false)
	if state.full {
		t.Fatal("players refused at the first readable pass marked the bot full")
	}
}

// Players whose own lists stay full are refused at the first pass and after every hold;
// neither marks the bot full, but a bot that starts full is still found by new players.
func TestFriendSyncRunStateIgnoresFirstPassAndRepeatRefusals(t *testing.T) {
	now := time.Now()
	var state friendSyncRunState
	state.record(now, friendSyncResult{pendingRead: true, fullPlayers: []string{"chronic1", "chronic2"}})
	if state.full {
		t.Fatal("first-pass refusals marked the bot full")
	}
	later := now.Add(friendListFullBackoff)
	state.record(later, friendSyncResult{pendingRead: true, fullPlayers: []string{"chronic1", "chronic2"}})
	if state.full {
		t.Fatal("repeat refusals marked the bot full")
	}
	state.record(later, friendSyncResult{pendingRead: true, fullPlayers: []string{"new1", "new2"}})
	if !state.full {
		t.Fatal("two new refused players did not mark the bot full")
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
	skip    func(string) bool // from the last accept
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

func (c *syncFriendClient) AcceptPendingFriendRequests(ctx context.Context, skip func(string) bool) ([]Person, error) {
	c.mu.Lock()
	c.skip = skip
	c.mu.Unlock()
	if c.accept != nil {
		return c.accept(ctx)
	}
	return nil, nil
}

// pending returns the people the last accept did not skip, as the real client would send them.
func (c *syncFriendClient) pending(xuids ...string) []Person {
	c.mu.Lock()
	defer c.mu.Unlock()
	var people []Person
	for _, xuid := range xuids {
		if c.skip == nil || !c.skip(xuid) {
			people = append(people, Person{XUID: xuid})
		}
	}
	return people
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
