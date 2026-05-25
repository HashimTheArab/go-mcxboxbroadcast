package broadcaster

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
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

func TestFriendSyncerStopsAutoFollowPassWhenFriendListIsFull(t *testing.T) {
	var log bytes.Buffer
	fullErr := classifiedSyncErr{kind: "friend_list_full"}
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
	err := syncer.Sync(context.Background())
	if !errors.Is(err, fullErr) {
		t.Fatalf("Sync() error = %v, want friend-list-full error", err)
	}
	if client.followCalls != 1 {
		t.Fatalf("follow calls = %d, want 1", client.followCalls)
	}
	if !strings.Contains(log.String(), "friend list full") {
		t.Fatalf("expected friend-list-full log, got %q", log.String())
	}
}

func TestFriendSyncRunStateSuppressesMutationsDuringRetryAfter(t *testing.T) {
	now := time.Unix(100, 0)
	state := friendSyncRunState{}
	state.recordError(now, &RetryAfterError{Delay: 2 * time.Minute})

	blocked := state.options(now.Add(time.Minute), true)
	if blocked.autoFollow || blocked.autoUnfollow {
		t.Fatalf("expected mutations suppressed during retry-after, got %#v", blocked)
	}

	allowed := state.options(now.Add(3*time.Minute), true)
	if !allowed.autoFollow || !allowed.autoUnfollow {
		t.Fatalf("expected mutations after retry-after, got %#v", allowed)
	}
}

func TestFriendSyncRunStateSkipsReadsDuringRetryAfter(t *testing.T) {
	now := time.Unix(100, 0)
	state := friendSyncRunState{}
	state.recordError(now, &RetryAfterError{Delay: 2 * time.Minute})

	if !state.backingOff(now.Add(time.Minute)) {
		t.Fatal("expected sync skipped during retry-after")
	}

	if state.backingOff(now.Add(3 * time.Minute)) {
		t.Fatal("expected sync after retry-after")
	}
}

func TestFriendSyncRunStateSuppressesAutoFollowWhenFriendListIsFull(t *testing.T) {
	now := time.Unix(100, 0)
	state := friendSyncRunState{}
	state.recordError(now, classifiedSyncErr{kind: FriendErrorKindFullList})

	opts := state.options(now.Add(time.Minute), true)
	if opts.autoFollow {
		t.Fatal("expected auto-follow suppressed after friend-list-full")
	}
	if !opts.autoUnfollow {
		t.Fatal("expected auto-unfollow to keep running after friend-list-full")
	}
}

type syncFriendClient struct {
	people      []Person
	accept      func(context.Context) ([]Person, error)
	follow      func(context.Context, string) error
	unfollow    func(context.Context, string) error
	followCalls int
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
}

func (f fakeSyncInviter) Invite(xuid string, _ int32) error {
	f.invite(xuid)
	return nil
}

type classifiedSyncErr struct {
	kind string
}

func (e classifiedSyncErr) Error() string {
	return e.kind
}

func (e classifiedSyncErr) FriendErrorKind() string {
	return e.kind
}
