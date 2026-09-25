package broadcaster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

func TestFriendSyncerAcceptsPendingIncomingRequests(t *testing.T) {
	var accepted bool
	var invited []string
	client := syncFriendClient{
		people: []Person{{XUID: "9", Gamertag: "Pending", IsFollowingCaller: true, IsFollowedByCaller: true}},
		accept: func(context.Context) ([]Person, error) {
			accepted = true
			return []Person{{XUID: "9", Gamertag: "Pending"}}, nil
		},
	}
	syncer := FriendSyncer{
		Client: &client,
		Config: FriendSyncConfig{
			AutoFollow:    true,
			InitialInvite: true,
		},
		Inviter: fakeSyncInviter{invite: func(xuid string) {
			invited = append(invited, xuid)
		}},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !accepted {
		t.Fatal("expected pending requests to be accepted")
	}
	if len(invited) != 1 || invited[0] != "9" {
		t.Fatalf("invited xuids = %v, want [9]", invited)
	}
}

func TestFriendSyncerBoundsXboxOperations(t *testing.T) {
	client := &deadlineFriendClient{}
	syncer := FriendSyncer{
		Client: client,
		Config: FriendSyncConfig{
			AutoFollow:    true,
			AutoUnfollow:  true,
			InitialInvite: true,
			Cleanup:       FriendCleanupConfig{MaxFriends: 1},
		},
		History: newMemoryHistory(),
		Inviter: deadlineInviter{record: client.record},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"accept", "friends", "follow", "unfollow", "remove_friend", "remove_follower", "invite"} {
		if !client.called[operation] {
			t.Fatalf("%s was not called", operation)
		}
		if !client.deadlined[operation] {
			t.Fatalf("%s did not receive a bounded context", operation)
		}
	}
}

func TestFriendSyncerContinuesAutoFollowWhenPendingAcceptFails(t *testing.T) {
	acceptErr := errors.New("pending requests unavailable")
	client := syncFriendClient{
		people: []Person{{XUID: "1", Gamertag: "Follower", IsFollowingCaller: true}},
		accept: func(context.Context) ([]Person, error) {
			return nil, acceptErr
		},
	}
	syncer := FriendSyncer{
		Client: &client,
		Config: FriendSyncConfig{
			AutoFollow: true,
		},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync() error = %v, want nil", err)
	}
	if client.followCalls != 1 {
		t.Fatalf("follow calls = %d, want 1", client.followCalls)
	}
}

func TestFriendSyncerDebugLogsFriendSyncProgress(t *testing.T) {
	var log bytes.Buffer
	client := syncFriendClient{
		people: []Person{
			{XUID: "1", Gamertag: "FollowerOne", IsFollowingCaller: true},
			{XUID: "2", Gamertag: "FollowerTwo", IsFollowingCaller: true},
			{XUID: "3", Gamertag: "Stale", IsFollowedByCaller: true},
		},
	}
	syncer := FriendSyncer{
		Client: &client,
		Config: FriendSyncConfig{
			AutoFollow:   true,
			AutoUnfollow: true,
		},
		Log: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	output := log.String()
	for _, want := range []string{
		`msg="friend sync scan"`,
		`followers=2`,
		`following=1`,
		`followers_only=2`,
		`following_only=1`,
		`mutual=0`,
		`neither=0`,
		`msg="adding friends" count=2`,
		`msg="added friend" xuid=1`,
		`msg="added friend" xuid=2`,
		`msg="added friends" count=2`,
		`msg="removing friends" count=1`,
		`msg="removed friend" xuid=3`,
		`msg="removed friends" count=1`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("debug log missing %q in:\n%s", want, output)
		}
	}
}

func TestFriendSyncerDebugLogsCleanupScan(t *testing.T) {
	var log bytes.Buffer
	history := newMemoryHistory()
	history.set("me", "stale", time.Now().Add(-16*24*time.Hour))
	history.set("me", "recent", time.Now())
	client := &syncFriendClient{
		people: []Person{
			{XUID: "stale", Gamertag: "Stale", IsFollowingCaller: true, IsFollowedByCaller: true},
			{XUID: "recent", Gamertag: "Recent", IsFollowingCaller: true, IsFollowedByCaller: true},
		},
	}
	syncer := FriendSyncer{
		Client:  client,
		History: history,
		Account: "me",
		Config: FriendSyncConfig{
			AutoUnfollow: true,
			Cleanup:      FriendCleanupConfig{InactiveDays: 15},
		},
		Log: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	output := log.String()
	for _, want := range []string{
		`msg="friend cleanup scan"`,
		`inactive=1`,
		`over_capacity=0`,
		`msg="removed friend" xuid=stale gamertag=Stale reason=inactive`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("debug log missing %q in:\n%s", want, output)
		}
	}
	if got := strings.Join(client.removedFriends, ","); got != "stale" {
		t.Fatalf("removed friends = %q, want stale", got)
	}
}

func TestFriendSyncerDebugLogsPendingFriendAccepts(t *testing.T) {
	var log bytes.Buffer
	client := syncFriendClient{
		people: []Person{
			{XUID: "9", IsFollowingCaller: true, IsFollowedByCaller: true},
			{XUID: "10", IsFollowingCaller: true, IsFollowedByCaller: true},
		},
		accept: func(context.Context) ([]Person, error) {
			return []Person{
				{XUID: "9", Gamertag: "PendingOne"},
				{XUID: "10", Gamertag: "PendingTwo"},
			}, nil
		},
	}
	syncer := FriendSyncer{
		Client: &client,
		Config: FriendSyncConfig{
			AutoFollow: true,
		},
		Log: slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	output := log.String()
	for _, want := range []string{
		`msg="accepting pending friend requests"`,
		`msg="added friend" xuid=9 gamertag=PendingOne source=pending_requests`,
		`msg="added friend" xuid=10 gamertag=PendingTwo source=pending_requests`,
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("log missing %q in:\n%s", want, output)
		}
	}
}

func TestFriendSyncerLogsInitialInviteFailure(t *testing.T) {
	var log bytes.Buffer
	inviteErr := errors.New("invite rejected")
	client := syncFriendClient{
		people: []Person{{XUID: "1", Gamertag: "Follower", IsFollowingCaller: true}},
	}
	syncer := FriendSyncer{
		Client: &client,
		Config: FriendSyncConfig{
			AutoFollow:    true,
			InitialInvite: true,
		},
		Inviter: fakeSyncInviter{err: inviteErr},
		Log:     slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	output := log.String()
	if !strings.Contains(output, `msg="send initial invite"`) || !strings.Contains(output, `err="invite rejected"`) {
		t.Fatalf("expected invite failure log, got:\n%s", output)
	}
	if strings.Contains(output, `msg="sent initial invite"`) {
		t.Fatalf("should not log sent invite on failure:\n%s", output)
	}
}

func TestFriendSyncerStopsAutoFollowPassWhenFriendListIsFull(t *testing.T) {
	var log bytes.Buffer
	fullErr := &xblsocial.ResponseError{Code: 1028}
	client := &syncFriendClient{
		people: []Person{
			{XUID: "1", IsFollowingCaller: true},
			{XUID: "2", IsFollowingCaller: true},
		},
		follow: func(context.Context, string) error {
			return fullErr
		},
	}
	syncer := FriendSyncer{
		Client: client,
		Config: FriendSyncConfig{
			AutoFollow: true,
		},
		Log: slog.New(slog.NewTextHandler(&log, nil)),
	}
	result, err := syncer.syncWithOptions(context.Background(), friendSyncOptions{cleanup: true, autoFollow: true, autoUnfollow: true})
	if err != nil {
		t.Fatalf("syncWithOptions() error = %v, want nil", err)
	}
	if !result.friendListFull {
		t.Fatal("expected friend-list-full result")
	}
	if client.followCalls != 1 {
		t.Fatalf("follow calls = %d, want 1", client.followCalls)
	}
	if !strings.Contains(log.String(), "friend list full") {
		t.Fatalf("expected friend-list-full log, got %q", log.String())
	}
}

func TestFriendSyncerContinuesPastSingleFollowFailure(t *testing.T) {
	client := &syncFriendClient{
		people: []Person{
			{XUID: "1", IsFollowingCaller: true},
			{XUID: "2", IsFollowingCaller: true},
			{XUID: "3", IsFollowedByCaller: true},
		},
		follow: func(_ context.Context, xuid string) error {
			if xuid == "1" {
				return errors.New("transient failure")
			}
			return nil
		},
	}
	syncer := FriendSyncer{
		Client: client,
		Config: FriendSyncConfig{AutoFollow: true, AutoUnfollow: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync() error = %v, want nil", err)
	}
	if client.followCalls != 2 {
		t.Fatalf("follow calls = %d, want 2 (continue past failure)", client.followCalls)
	}
	if client.unfollowCalls != 1 {
		t.Fatalf("unfollow calls = %d, want 1 (removals not aborted)", client.unfollowCalls)
	}
}

func TestFriendSyncerContinuesRemovalsWhenAddsAreRateLimited(t *testing.T) {
	limitErr := &xblsocial.ResponseError{StatusCode: 429, RetryAfter: 5 * time.Second}
	client := &syncFriendClient{
		people: []Person{
			{XUID: "1", IsFollowingCaller: true},
			{XUID: "2", IsFollowingCaller: true},
			{XUID: "3", IsFollowedByCaller: true},
		},
		follow: func(context.Context, string) error {
			return limitErr
		},
	}
	syncer := FriendSyncer{
		Client: client,
		Config: FriendSyncConfig{AutoFollow: true, AutoUnfollow: true},
	}
	result, err := syncer.syncWithOptions(context.Background(), friendSyncOptions{autoFollow: true, autoUnfollow: true})
	if err != nil {
		t.Fatalf("syncWithOptions() error = %v, want nil", err)
	}
	if client.followCalls != 1 {
		t.Fatalf("follow calls = %d, want 1 (stop adds after rate limit)", client.followCalls)
	}
	if client.unfollowCalls != 1 {
		t.Fatalf("unfollow calls = %d, want 1 (removals use a separate limit)", client.unfollowCalls)
	}
	if result.followRetryAfter != 5*time.Second {
		t.Fatalf("follow retry-after = %s, want 5s", result.followRetryAfter)
	}
	if result.unfollowRetryAfter != 0 {
		t.Fatalf("unfollow retry-after = %s, want 0", result.unfollowRetryAfter)
	}
}

func TestFriendSyncerDropsRestrictedFollowers(t *testing.T) {
	restrictedErr := &xblsocial.ResponseError{Code: 1049}
	var notified []string
	client := &syncFriendClient{
		people: []Person{
			{XUID: "1", Gamertag: "Restricted", IsFollowingCaller: true},
			{XUID: "2", Gamertag: "Fine", IsFollowingCaller: true},
		},
		follow: func(_ context.Context, xuid string) error {
			if xuid == "1" {
				return restrictedErr
			}
			return nil
		},
	}
	syncer := FriendSyncer{
		Client: client,
		Config: FriendSyncConfig{AutoFollow: true},
		Notifier: notifierFunc(func(_ context.Context, message string) error {
			notified = append(notified, message)
			return nil
		}),
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatalf("Sync() error = %v, want nil", err)
	}
	if got := strings.Join(client.removedFollowers, ","); got != "1" {
		t.Fatalf("removed followers = %q, want 1", got)
	}
	if client.followCalls != 2 {
		t.Fatalf("follow calls = %d, want 2 (continue past restricted account)", client.followCalls)
	}
	if len(notified) != 1 || !strings.Contains(notified[0], "Restricted") {
		t.Fatalf("notifications = %v, want restriction notification", notified)
	}
}

func TestFriendSyncerSkipsGuestXUIDsUnconditionally(t *testing.T) {
	guest := strconv.FormatUint(1<<52, 10)
	client := &syncFriendClient{
		people: []Person{{XUID: guest, IsFollowingCaller: true}},
	}
	syncer := FriendSyncer{
		Client: client,
		Config: FriendSyncConfig{AutoFollow: true},
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.followCalls != 0 {
		t.Fatalf("follow calls = %d, want 0 (guest XUIDs always skipped)", client.followCalls)
	}
}

func TestFriendSyncerPrunesHistoryForExFriends(t *testing.T) {
	history := newMemoryHistory()
	history.set("me", "1", time.Now())
	history.set("me", "gone", time.Now())
	history.set("other", "gone", time.Now())
	client := &syncFriendClient{
		people: []Person{{XUID: "1", IsFollowingCaller: true, IsFollowedByCaller: true}},
	}
	syncer := FriendSyncer{
		Client:  client,
		Config:  FriendSyncConfig{Cleanup: FriendCleanupConfig{InactiveDays: 15}},
		History: history,
		Account: "me",
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := history.get("me", "gone"); ok {
		t.Fatal("expected history entry for ex-friend to be pruned")
	}
	if _, ok := history.get("me", "1"); !ok {
		t.Fatal("expected history entry for current friend to remain")
	}
	if _, ok := history.get("other", "gone"); !ok {
		t.Fatal("pruning one account must not touch another account's history")
	}
}

func TestFriendSyncRunStateTracksSeparateFollowAndUnfollowBackoff(t *testing.T) {
	now := time.Unix(100, 0)
	state := friendSyncRunState{}
	state.record(now, friendSyncResult{followRetryAfter: 2 * time.Minute})

	blocked := state.options(now.Add(time.Minute), true)
	if blocked.autoFollow {
		t.Fatal("expected auto-follow suppressed during retry-after")
	}
	if !blocked.autoUnfollow {
		t.Fatal("expected auto-unfollow to keep running (separate limit)")
	}

	allowed := state.options(now.Add(3*time.Minute), true)
	if !allowed.autoFollow || !allowed.autoUnfollow {
		t.Fatalf("expected mutations after retry-after, got %#v", allowed)
	}
}

func TestFriendSyncRunStateSuppressesAutoFollowWhenFriendListIsFull(t *testing.T) {
	now := time.Unix(100, 0)
	state := friendSyncRunState{}
	state.record(now, friendSyncResult{friendListFull: true})

	opts := state.options(now.Add(time.Minute), true)
	if opts.autoFollow {
		t.Fatal("expected auto-follow suppressed after friend-list-full")
	}
	if !opts.autoUnfollow {
		t.Fatal("expected auto-unfollow to keep running after friend-list-full")
	}
}

type syncFriendClient struct {
	people           []Person
	accept           func(context.Context) ([]Person, error)
	follow           func(context.Context, string) error
	unfollow         func(context.Context, string) error
	removeFriend     func(context.Context, string) error
	removeFollower   func(context.Context, string) error
	followCalls      int
	unfollowCalls    int
	removedFriends   []string
	removedFollowers []string
}

func (c *syncFriendClient) Friends(context.Context) ([]Person, error) {
	return c.people, nil
}

func (c *syncFriendClient) Follow(ctx context.Context, xuid string) error {
	c.followCalls++
	if c.follow != nil {
		return c.follow(ctx, xuid)
	}
	return nil
}

func (c *syncFriendClient) Unfollow(ctx context.Context, xuid string) error {
	c.unfollowCalls++
	if c.unfollow != nil {
		return c.unfollow(ctx, xuid)
	}
	return nil
}

func (c *syncFriendClient) RemoveFriend(ctx context.Context, xuid string) error {
	if c.removeFriend != nil {
		if err := c.removeFriend(ctx, xuid); err != nil {
			return err
		}
	}
	c.removedFriends = append(c.removedFriends, xuid)
	return nil
}

func (c *syncFriendClient) RemoveFollower(ctx context.Context, xuid string) error {
	if c.removeFollower != nil {
		if err := c.removeFollower(ctx, xuid); err != nil {
			return err
		}
	}
	c.removedFollowers = append(c.removedFollowers, xuid)
	return nil
}

func (c *syncFriendClient) AcceptPendingFriendRequests(ctx context.Context, _ func(string) bool) (FriendRequestResult, error) {
	if c.accept == nil {
		return FriendRequestResult{}, nil
	}
	accepted, err := c.accept(ctx)
	return FriendRequestResult{Accepted: accepted}, err
}

type deadlineFriendClient struct {
	called    map[string]bool
	deadlined map[string]bool
}

func (c *deadlineFriendClient) record(operation string, ctx context.Context) {
	if c.called == nil {
		c.called = map[string]bool{}
		c.deadlined = map[string]bool{}
	}
	c.called[operation] = true
	deadline, ok := ctx.Deadline()
	c.deadlined[operation] = ok && time.Until(deadline) > 0 && time.Until(deadline) <= 20*time.Second
}

func (c *deadlineFriendClient) Friends(ctx context.Context) ([]Person, error) {
	c.record("friends", ctx)
	return []Person{
		{XUID: "follow", Gamertag: "Follow", IsFollowingCaller: true},
		{XUID: "restricted", Gamertag: "Restricted", IsFollowingCaller: true},
		{XUID: "unfollow", Gamertag: "Unfollow", IsFollowedByCaller: true},
		{XUID: "pending", Gamertag: "Pending", IsFollowingCaller: true, IsFollowedByCaller: true},
		{XUID: "old", Gamertag: "Old", IsFollowingCaller: true, IsFollowedByCaller: true},
	}, nil
}

func (c *deadlineFriendClient) Follow(ctx context.Context, xuid string) error {
	c.record("follow", ctx)
	if xuid == "restricted" {
		return xblsocial.ErrFriendRestricted
	}
	return nil
}

func (c *deadlineFriendClient) Unfollow(ctx context.Context, _ string) error {
	c.record("unfollow", ctx)
	return nil
}

func (c *deadlineFriendClient) RemoveFriend(ctx context.Context, _ string) error {
	c.record("remove_friend", ctx)
	return nil
}

func (c *deadlineFriendClient) RemoveFollower(ctx context.Context, _ string) error {
	c.record("remove_follower", ctx)
	return nil
}

func (c *deadlineFriendClient) AcceptPendingFriendRequests(ctx context.Context, _ func(string) bool) (FriendRequestResult, error) {
	c.record("accept", ctx)
	return FriendRequestResult{Accepted: []Person{{XUID: "pending", Gamertag: "Pending"}}}, nil
}

type deadlineInviter struct {
	record func(string, context.Context)
}

func (i deadlineInviter) Invite(ctx context.Context, _, _ string) error {
	i.record("invite", ctx)
	return nil
}

type notifierFunc func(context.Context, string) error

func (f notifierFunc) Notify(ctx context.Context, message string) error {
	return f(ctx, message)
}

// memoryHistory is an in-memory HistoryStore keyed by account.
type memoryHistory struct {
	mu       sync.Mutex
	accounts map[string]map[string]time.Time
}

func newMemoryHistory() *memoryHistory {
	return &memoryHistory{accounts: map[string]map[string]time.Time{}}
}

func (h *memoryHistory) set(account, xuid string, when time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.accounts[account] == nil {
		h.accounts[account] = map[string]time.Time{}
	}
	h.accounts[account][xuid] = when
}

func (h *memoryHistory) get(account, xuid string) (time.Time, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	when, ok := h.accounts[account][xuid]
	return when, ok
}

func (h *memoryHistory) LastSeen(_ context.Context, account string) (map[string]time.Time, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return maps.Clone(h.accounts[account]), nil
}

func (h *memoryHistory) Track(_ context.Context, account string, when time.Time, xuids ...string) error {
	for _, xuid := range xuids {
		if _, ok := h.get(account, xuid); !ok {
			h.set(account, xuid, when)
		}
	}
	return nil
}

func (h *memoryHistory) Seen(_ context.Context, xuid string, when time.Time) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entries := range h.accounts {
		if _, ok := entries[xuid]; ok {
			entries[xuid] = when
		}
	}
	return nil
}

func (h *memoryHistory) Forget(_ context.Context, account string, xuids ...string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, xuid := range xuids {
		delete(h.accounts[account], xuid)
	}
	return nil
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

// fakeXbox models the Xbox people service behind FriendClient: who follows the
// account, whom it follows, and incoming friend requests. Ending a friendship
// leaves the other person's follow in place, as Xbox does.
type fakeXbox struct {
	mu        sync.Mutex
	followers map[string]bool // they follow the account
	following map[string]bool // the account follows them
	pending   map[string]bool // incoming friend requests
	// limit makes bulk accepts fail with code 1028 once following reaches it.
	limit int
	// bulkAdd overrides how a bulk accept is answered.
	bulkAdd func(xuids []string) *http.Response
	// removeFollower optionally fails a follower removal.
	removeFollower func(xuid string) *http.Response
	bulkPosts      int
	follows        []string
}

func newFakeXbox() *fakeXbox {
	return &fakeXbox{followers: map[string]bool{}, following: map[string]bool{}, pending: map[string]bool{}}
}

// befriend makes each XUID a mutual friend.
func (f *fakeXbox) befriend(xuids ...string) {
	for _, xuid := range xuids {
		f.followers[xuid], f.following[xuid] = true, true
	}
}

var fakeXboxXUID = regexp.MustCompile(`xuid\((\d+)\)`)

func (f *fakeXbox) peopleJSON(set map[string]bool) string {
	people := []map[string]any{}
	for _, xuid := range slices.Sorted(maps.Keys(set)) {
		people = append(people, map[string]any{
			"xuid": xuid, "gamertag": "GT" + xuid,
			"isFollowingCaller":  f.followers[xuid],
			"isFollowedByCaller": f.following[xuid],
		})
	}
	data, _ := json.Marshal(map[string]any{"people": people})
	return string(data)
}

func (f *fakeXbox) client() FriendClient {
	return FriendClient{Social: newTestSocialClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp := f.serve(req)
		resp.Request = req
		return resp, nil
	})})}
}

func (f *fakeXbox) serve(req *http.Request) *http.Response {
	f.mu.Lock()
	defer f.mu.Unlock()
	path := req.URL.Path
	xuid := ""
	if m := fakeXboxXUID.FindStringSubmatch(path); m != nil {
		xuid = m[1]
	}
	switch {
	case req.Method == http.MethodGet && strings.HasSuffix(path, "/people/followers"):
		return response(http.StatusOK, f.peopleJSON(f.followers))
	case req.Method == http.MethodGet && strings.HasSuffix(path, "/people/social"):
		return response(http.StatusOK, f.peopleJSON(f.following))
	case req.Method == http.MethodGet && strings.Contains(path, "friendRequests(received)"):
		return response(http.StatusOK, f.peopleJSON(f.pending))
	case req.Method == http.MethodPost && strings.HasPrefix(path, "/bulk/"):
		f.bulkPosts++
		var body struct {
			XUIDs []string `json:"xuids"`
		}
		_ = json.NewDecoder(req.Body).Decode(&body)
		if f.bulkAdd != nil {
			return f.bulkAdd(body.XUIDs)
		}
		if f.limit > 0 && len(f.following)+len(body.XUIDs) > f.limit {
			return response(http.StatusBadRequest, `{"code":1028,"description":"The attempted People request was rejected because it would exceed the People list limit."}`)
		}
		for _, x := range body.XUIDs {
			delete(f.pending, x)
			f.befriend(x)
		}
		return updated(body.XUIDs)
	case req.Method == http.MethodPut:
		f.following[xuid] = true
		f.follows = append(f.follows, xuid)
		return response(http.StatusNoContent, "")
	case req.Method == http.MethodDelete && strings.Contains(path, "/people/follower/"):
		if f.removeFollower != nil {
			if resp := f.removeFollower(xuid); resp != nil {
				return resp
			}
		}
		delete(f.followers, xuid)
		return response(http.StatusNoContent, "")
	case req.Method == http.MethodDelete && strings.Contains(path, "/friends/v2/"):
		delete(f.following, xuid)
		delete(f.pending, xuid)
		return response(http.StatusOK, "")
	case req.Method == http.MethodDelete:
		delete(f.following, xuid)
		return response(http.StatusNoContent, "")
	}
	return response(http.StatusNotFound, "")
}

// Removed friends who still follow the account must not be followed back into the freed slot.
func TestFriendSyncDoesNotFollowBackRemovedFriends(t *testing.T) {
	x := newFakeXbox()
	x.befriend("42")
	history := newMemoryHistory()
	history.set("100", "42", time.Now().Add(-16*24*time.Hour))
	s := &FriendSyncer{Client: x.client(), History: history, Account: "100",
		Config: FriendSyncConfig{AutoFollow: true, AutoUnfollow: true, Cleanup: FriendCleanupConfig{InactiveDays: 15}}}
	for _, cleanup := range []bool{true, false, true} {
		s.runSync(context.Background(), cleanup)
	}
	if len(x.follows) != 0 || x.followers["42"] || x.following["42"] {
		t.Fatalf("follows=%v follower=%v following=%v, want the friendship gone both ways", x.follows, x.followers["42"], x.following["42"])
	}
}

// A throttled follower removal is finished later and never turns into a follow-back.
func TestFriendSyncFinishesThrottledRemoval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		x := newFakeXbox()
		x.befriend("42")
		throttled := false
		x.removeFollower = func(string) *http.Response {
			if throttled {
				return nil
			}
			throttled = true
			resp := response(http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "30")
			return resp
		}
		history := newMemoryHistory()
		history.set("100", "42", time.Now().Add(-16*24*time.Hour))
		s := &FriendSyncer{Client: x.client(), History: history, Account: "100",
			Config: FriendSyncConfig{AutoFollow: true, AutoUnfollow: true, Cleanup: FriendCleanupConfig{InactiveDays: 15}}}
		s.runSync(context.Background(), true)
		s.runSync(context.Background(), false)
		if !x.followers["42"] || len(x.follows) != 0 {
			t.Fatalf("during backoff follower=%v follows=%v, want follower kept and no follow-back", x.followers["42"], x.follows)
		}
		time.Sleep(30 * time.Second)
		s.runSync(context.Background(), false)
		if x.followers["42"] || len(x.follows) != 0 {
			t.Fatalf("after backoff follower=%v follows=%v, want follower removed", x.followers["42"], x.follows)
		}
	})
}

// With maxFriends set, a full list makes room for waiting requests instead of failing every pass.
func TestFriendSyncMakesRoomWhenAcceptFindsListFull(t *testing.T) {
	x := newFakeXbox()
	x.limit = 3
	x.befriend("1", "2", "3")
	x.pending["9"] = true
	history := newMemoryHistory()
	history.set("100", "1", time.Now().Add(-3*24*time.Hour))
	history.set("100", "2", time.Now().Add(-1*time.Hour))
	history.set("100", "3", time.Now().Add(-2*24*time.Hour))
	s := &FriendSyncer{Client: x.client(), History: history, Account: "100",
		Config: FriendSyncConfig{AutoFollow: true, AutoUnfollow: true, Cleanup: FriendCleanupConfig{MaxFriends: 3}}}

	if _, again := s.runSync(context.Background(), false); !again {
		t.Fatal("expected another pass once room was made")
	}
	s.runSync(context.Background(), false)
	if !x.following["9"] || x.following["1"] || !x.following["2"] || !x.following["3"] {
		t.Fatalf("following = %v, want 9 added in place of 1, the least recently seen", x.following)
	}
	if !s.state.autoFollowUntil.IsZero() {
		t.Fatal("a list made room for must not back off accepts")
	}
}

// Without maxFriends, a full list backs off instead of retrying the same accept every pass.
func TestFriendSyncBacksOffWhenAcceptFindsListFull(t *testing.T) {
	x := newFakeXbox()
	x.limit = 1
	x.befriend("1")
	x.pending["9"] = true
	s := &FriendSyncer{Client: x.client(), Config: FriendSyncConfig{AutoFollow: true}}
	for range 3 {
		s.runSync(context.Background(), false)
	}
	if x.bulkPosts != 1 || s.state.autoFollowUntil.IsZero() {
		t.Fatalf("bulk posts=%d backoff=%v, want one post then backoff", x.bulkPosts, s.state.autoFollowUntil)
	}
}

// maxFriends leaves room for waiting requests by removing the least recently seen friends.
func TestFriendSyncKeepsFriendsWithinMaxFriends(t *testing.T) {
	x := newFakeXbox()
	x.befriend("1", "2", "3", "4")
	history := newMemoryHistory()
	for i, xuid := range []string{"3", "1", "4", "2"} {
		history.set("100", xuid, time.Now().Add(-time.Duration(4-i)*time.Hour))
	}
	s := &FriendSyncer{Client: x.client(), History: history, Account: "100",
		Config: FriendSyncConfig{AutoUnfollow: true, Cleanup: FriendCleanupConfig{MaxFriends: 2}}}
	s.runSync(context.Background(), false)
	if got := strings.Join(slices.Sorted(maps.Keys(x.following)), ","); got != "2,4" {
		t.Fatalf("following = %s, want 2,4 (the most recently seen)", got)
	}
	if _, ok := history.get("100", "3"); ok {
		t.Fatal("removed friends should leave history")
	}
}

// A request Xbox refuses is retried later, not on every pass, and does not block the rest.
func TestFriendSyncRetriesRefusedRequestsLater(t *testing.T) {
	x := newFakeXbox()
	x.pending["1"], x.pending["2"], x.pending["3"] = true, true, true
	x.bulkAdd = func(xuids []string) *http.Response {
		if slices.Contains(xuids, "3") {
			return response(http.StatusBadRequest, "")
		}
		for _, xuid := range xuids {
			delete(x.pending, xuid)
			x.befriend(xuid)
		}
		return updated(xuids)
	}
	s := &FriendSyncer{Client: x.client(), Config: FriendSyncConfig{AutoFollow: true}}
	s.runSync(context.Background(), false)
	posts := x.bulkPosts
	s.runSync(context.Background(), false)
	if !x.following["1"] || !x.following["2"] || x.bulkPosts != posts {
		t.Fatalf("following=%v posts %d -> %d, want 1 and 2 accepted and 3 not retried yet", x.following, posts, x.bulkPosts)
	}
}

// Xbox can report a request as accepted while it stays pending: that is not a new friend.
func TestFriendSyncAnnouncesAcceptedFriendsOnce(t *testing.T) {
	var log bytes.Buffer
	var invites []string
	x := newFakeXbox()
	x.pending["7"] = true
	x.bulkAdd = func([]string) *http.Response { return updated([]string{"7"}) }
	s := &FriendSyncer{Client: x.client(), Config: FriendSyncConfig{AutoFollow: true, InitialInvite: true},
		Inviter: fakeSyncInviter{invite: func(xuid string) { invites = append(invites, xuid) }},
		Log:     slog.New(slog.NewTextHandler(&log, nil))}
	for range 3 {
		s.runSync(context.Background(), false)
	}
	if len(invites) != 0 || strings.Contains(log.String(), "added friend") || x.bulkPosts != 1 {
		t.Fatalf("invites=%v posts=%d log=%s, want nothing announced for a request still pending", invites, x.bulkPosts, log.String())
	}

	x.bulkAdd = nil
	x.pending["8"] = true
	for range 3 {
		s.runSync(context.Background(), false)
	}
	if fmt.Sprint(invites) != "[8]" || strings.Count(log.String(), `msg="added friend"`) != 1 {
		t.Fatalf("invites=%v log=%s, want one announcement and invite for 8", invites, log.String())
	}
}

// Cleanup and auto-unfollow must never cut the links between the bot's own accounts.
func TestFriendSyncNeverRemovesOwnAccounts(t *testing.T) {
	x := newFakeXbox()
	x.befriend("200", "9")
	x.following["201"] = true
	history := newMemoryHistory()
	history.set("100", "200", time.Now().Add(-30*24*time.Hour))
	history.set("100", "9", time.Now())
	s := &FriendSyncer{Client: x.client(), History: history, Account: "100", OwnAccounts: []string{"100", "200", "201"},
		Config: FriendSyncConfig{AutoUnfollow: true, Cleanup: FriendCleanupConfig{InactiveDays: 15, MaxFriends: 1}}}
	s.runSync(context.Background(), true)
	if !x.following["200"] || !x.following["201"] || x.following["9"] {
		t.Fatalf("following = %v, want own accounts kept and 9 removed for capacity", x.following)
	}
}

// Restricted-follower removals share the removal rate limit and stop when it is hit.
func TestFriendSyncRestrictedRemovalHonoursRetryAfter(t *testing.T) {
	var attempts int
	client := &syncFriendClient{
		people: []Person{{XUID: "1", IsFollowingCaller: true}, {XUID: "2", IsFollowingCaller: true}},
		follow: func(context.Context, string) error { return &xblsocial.ResponseError{Code: 1049} },
		removeFollower: func(context.Context, string) error {
			attempts++
			return &xblsocial.ResponseError{StatusCode: http.StatusTooManyRequests, RetryAfter: time.Minute}
		},
	}
	s := &FriendSyncer{Client: client, Config: FriendSyncConfig{AutoFollow: true}}
	result, err := s.syncWithOptions(context.Background(), friendSyncOptions{autoFollow: true, autoUnfollow: true})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || result.unfollowRetryAfter != time.Minute {
		t.Fatalf("attempts=%d retry=%s, want 1 attempt and a 1m backoff", attempts, result.unfollowRetryAfter)
	}
}
