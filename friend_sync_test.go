package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
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
	if friends, _ := count.snapshot(); client.lists != 1 || friends != 4 {
		t.Fatalf("lists = %d, friends = %d; want one list and 3 listed + 1 accepted", client.lists, friends)
	}
	// An old count is listed again, so friends who left are dropped.
	count.countedAt = time.Now().Add(-friendCountRefresh - time.Minute)
	client.people = client.people[:2]
	client.accept = nil
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if friends, _ := count.snapshot(); client.lists != 2 || friends != 2 {
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
		Notifier: notifierFunc(func(_ context.Context, m string) error { notified = append(notified, m); return nil }),
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
			return nil, &AcceptFriendRequestsError{Failed: []Person{{XUID: "1"}, {XUID: "2"}}}
		},
		add: func(context.Context, string) error {
			return &xblsocial.ResponseError{StatusCode: 403, Code: 1028}
		},
	}
	var result friendSyncResult
	FriendSyncer{Client: client}.acceptPending(context.Background(), &result)
	if !result.friendListFull || len(client.added) != 1 {
		t.Fatalf("full=%v added=%v, want one attempt then stop", result.friendListFull, client.added)
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
	state.record(now, friendSyncResult{friendListFull: true})
	if opts := state.options(now.Add(2*time.Minute), false); opts.add {
		t.Fatal("adds resumed while the friend list is full")
	}
	if opts := state.options(now.Add(friendListFullBackoff), false); !opts.add {
		t.Fatal("adds did not resume after the full-list backoff")
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

type notifierFunc func(context.Context, string) error

func (f notifierFunc) Notify(ctx context.Context, message string) error {
	return f(ctx, message)
}

type syncHistoryStore struct {
	lastSeen map[string]time.Time
}

func (s *syncHistoryStore) LastSeen(_ context.Context, xuid string) (time.Time, bool, error) {
	when, ok := s.lastSeen[xuid]
	return when, ok, nil
}

func (s *syncHistoryStore) Seen(_ context.Context, xuid string, when time.Time) error {
	if s.lastSeen == nil {
		s.lastSeen = map[string]time.Time{}
	}
	s.lastSeen[xuid] = when
	return nil
}

func (s *syncHistoryStore) Clear(_ context.Context, xuid string) error {
	delete(s.lastSeen, xuid)
	return nil
}

func (s *syncHistoryStore) XUIDs(context.Context) ([]string, error) {
	xuids := make([]string, 0, len(s.lastSeen))
	for xuid := range s.lastSeen {
		xuids = append(xuids, xuid)
	}
	return xuids, nil
}

type fakeSyncInviter struct {
	invite func(string)
	err    error
}

func (f fakeSyncInviter) Invite(_ context.Context, xuid, _ string) error {
	if f.invite != nil {
		f.invite(xuid)
	}
	return f.err
}
