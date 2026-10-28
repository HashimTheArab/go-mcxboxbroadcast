package broadcaster

import (
	"context"
	"errors"
	"fmt"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// FriendClient adapts go-xsapi/v2's Xbox social client to the FriendSyncer
// API. Minecraft only joins sessions through friends, so it manages friends
// alone, like MCXboxBroadcast.
type FriendClient struct {
	Social *xblsocial.Client
}

type Person struct {
	XUID        string `json:"xuid"`
	Gamertag    string `json:"gamertag"`
	DisplayName string `json:"displayName"`
}

// MaxFriends is the Xbox friend list limit.
const MaxFriends = 1000

// friendListConfig matches the undecorated friend list request MCXboxBroadcast makes.
var friendListConfig = xblsocial.PeopleListConfig{Undecorated: true}

// addFriendsBatchSize stays below Xbox Live's undocumented bulk-operation
// limit. Code 1050 responses are also split dynamically in case the service
// applies a lower limit to a particular account or request.
const addFriendsBatchSize = 50

// AcceptFriendRequestsError reports a successful bulk accept response that
// still failed to update one or more pending friend requests.
type AcceptFriendRequestsError struct {
	Failed []Person
}

func (e *AcceptFriendRequestsError) Error() string {
	return fmt.Sprintf("accept pending friend requests: failed to update %d users", len(e.Failed))
}

// Friends returns the authenticated account's friends.
func (c FriendClient) Friends(ctx context.Context) ([]Person, error) {
	users, err := c.Social.People(ctx, xblsocial.PeopleListFriends, friendListConfig)
	if err != nil {
		return nil, err
	}
	return peopleFromSocialUsers(users), nil
}

// AcceptPendingFriendRequests accepts incoming Xbox friend requests with
// bounded add calls and returns the people that Xbox reported as updated.
func (c FriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	pending, err := c.Social.People(ctx, xblsocial.PeopleListIncomingFriendRequests, xblsocial.PeopleListConfig{Undecorated: true})
	if err != nil {
		return nil, err
	}
	xuids := make([]string, 0, len(pending))
	byXUID := make(map[string]Person, len(pending))
	for _, user := range pending {
		if user.XUID == "" || isGuestXUID(user.XUID) {
			continue
		}
		xuids = append(xuids, user.XUID)
		byXUID[user.XUID] = personFromSocialUser(user)
	}
	if len(xuids) == 0 {
		return nil, nil
	}

	updatedXUIDs := make([]string, 0, len(xuids))
	var bulkErr error
	for start := 0; start < len(xuids); start += addFriendsBatchSize {
		end := min(start+addFriendsBatchSize, len(xuids))
		updated, err := addFriends(ctx, c.Social, xuids[start:end])
		updatedXUIDs = append(updatedXUIDs, updated...)
		if err != nil {
			bulkErr = err
			break
		}
	}
	updated := make(map[string]struct{}, len(updatedXUIDs))
	accepted := make([]Person, 0, len(updatedXUIDs))
	for _, xuid := range updatedXUIDs {
		if person, ok := byXUID[xuid]; ok {
			updated[xuid] = struct{}{}
			accepted = append(accepted, person)
		}
	}
	if bulkErr != nil {
		return accepted, bulkErr
	}
	var failed []Person
	for _, xuid := range xuids {
		if _, ok := updated[xuid]; !ok {
			failed = append(failed, byXUID[xuid])
		}
	}
	if len(failed) > 0 {
		return accepted, &AcceptFriendRequestsError{Failed: failed}
	}
	return accepted, nil
}

// addFriends retries Xbox's bulk-limit response with progressively smaller
// batches. Successful work from the first half is retained if the second half
// fails for an unrelated reason such as rate limiting.
func addFriends(ctx context.Context, client *xblsocial.Client, xuids []string) ([]string, error) {
	result, err := client.AddFriends(ctx, xuids)
	if err == nil || len(xuids) == 1 || !errors.Is(err, xblsocial.ErrBulkOperationLimit) {
		return result.Updated, err
	}

	middle := len(xuids) / 2
	left, err := addFriends(ctx, client, xuids[:middle])
	if err != nil {
		return left, err
	}
	right, err := addFriends(ctx, client, xuids[middle:])
	return append(left, right...), err
}

// AddFriend accepts the user's pending request, or sends one.
func (c FriendClient) AddFriend(ctx context.Context, xuid string) error {
	return c.Social.AddFriend(ctx, xuid)
}

// RemoveFriend ends the friendship or declines the user's pending request.
func (c FriendClient) RemoveFriend(ctx context.Context, xuid string) error {
	return c.Social.RemoveFriend(ctx, xuid)
}

func peopleFromSocialUsers(users []xblsocial.User) []Person {
	people := make([]Person, 0, len(users))
	for _, user := range users {
		if user.XUID != "" {
			people = append(people, personFromSocialUser(user))
		}
	}
	return people
}

func personFromSocialUser(user xblsocial.User) Person {
	return Person{
		XUID:        user.XUID,
		Gamertag:    user.GamerTag,
		DisplayName: user.DisplayName,
	}
}
