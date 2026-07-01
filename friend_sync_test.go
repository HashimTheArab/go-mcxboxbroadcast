package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

func TestFriendSyncerAcceptsPendingIncomingRequests(t *testing.T) {
	var accepted bool
	var invited []string
	client := syncFriendClient{
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

func TestFriendSyncerDebugLogsPendingFriendAccepts(t *testing.T) {
	var log bytes.Buffer
	client := syncFriendClient{
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
	result, err := syncer.syncWithOptions(context.Background(), friendSyncOptions{expire: true, autoFollow: true, autoUnfollow: true})
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
	if client.removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1 (removals not aborted)", client.removeCalls)
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
	if client.removeCalls != 1 {
		t.Fatalf("remove calls = %d, want 1 (removals use a separate limit)", client.removeCalls)
	}
	if result.followRetryAfter != 5*time.Second {
		t.Fatalf("follow retry-after = %s, want 5s", result.followRetryAfter)
	}
	if result.unfollowRetryAfter != 0 {
		t.Fatalf("unfollow retry-after = %s, want 0", result.unfollowRetryAfter)
	}
}

func TestFriendSyncerForceUnfollowsRestrictedAccounts(t *testing.T) {
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
	if got := strings.Join(client.forceUnfollowed, ","); got != "1" {
		t.Fatalf("force unfollowed = %q, want 1", got)
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
	history := &syncHistoryStore{
		lastSeen: map[string]time.Time{
			"1":    time.Now(),
			"gone": time.Now(),
		},
	}
	client := &syncFriendClient{
		people: []Person{{XUID: "1", IsFollowingCaller: true, IsFollowedByCaller: true}},
	}
	syncer := FriendSyncer{
		Client:       client,
		Config:       FriendSyncConfig{ExpiryEnabled: true, ExpiryDays: 15},
		History:      history,
		PruneHistory: true,
	}
	if err := syncer.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := history.lastSeen["gone"]; ok {
		t.Fatal("expected history entry for ex-friend to be pruned")
	}
	if _, ok := history.lastSeen["1"]; !ok {
		t.Fatal("expected history entry for current friend to remain")
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
	people          []Person
	accept          func(context.Context) ([]Person, error)
	follow          func(context.Context, string) error
	unfollow        func(context.Context, string) error
	followCalls     int
	removeCalls     int
	forceUnfollowed []string
}

func (c *syncFriendClient) ForceUnfollow(_ context.Context, xuid string) error {
	c.forceUnfollowed = append(c.forceUnfollowed, xuid)
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
	c.removeCalls++
	if c.unfollow != nil {
		return c.unfollow(ctx, xuid)
	}
	return nil
}

func (c *syncFriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	if c.accept != nil {
		return c.accept(ctx)
	}
	return nil, nil
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
