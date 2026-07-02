package broadcaster

import (
	"context"
	"fmt"
	"net/http"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

// FriendClient adapts go-xsapi/v2's Xbox social client to the FriendSyncer
// API, using the same endpoints, contract versions, and request shapes as
// MCXboxBroadcast.
type FriendClient struct {
	Client *http.Client
	Social *xblsocial.Client
}

type Person struct {
	XUID                 string `json:"xuid"`
	Gamertag             string `json:"gamertag"`
	DisplayName          string `json:"displayName"`
	ModernGamertag       string `json:"modernGamertag"`
	IsFollowingCaller    bool   `json:"isFollowingCaller"`
	IsFollowedByCaller   bool   `json:"isFollowedByCaller"`
	UniqueModernGamertag string `json:"uniqueModernGamertag"`
}

// friendListConfig fetches people lists undecorated with contract version 5,
// keeping the periodic response small at large friend counts.
var friendListConfig = xblsocial.PeopleListConfig{Undecorated: true, ContractVersion: 5}

// AcceptFriendRequestsError reports a successful bulk accept response that
// still failed to update one or more pending friend requests.
type AcceptFriendRequestsError struct {
	Failed []string
}

func (e *AcceptFriendRequestsError) Error() string {
	return fmt.Sprintf("accept pending friend requests: failed to update %d users", len(e.Failed))
}

func (e *AcceptFriendRequestsError) FailedXUIDs() []string {
	return append([]string(nil), e.Failed...)
}

// Friends returns a merged view of people following the authenticated account
// and people the authenticated account follows.
func (c FriendClient) Friends(ctx context.Context) ([]Person, error) {
	socialClient := c.social()
	followers, err := socialClient.People(ctx, xblsocial.PeopleListFollowers, friendListConfig)
	if err != nil {
		return nil, err
	}
	following, err := socialClient.People(ctx, xblsocial.PeopleListFollowing, friendListConfig)
	if err != nil {
		return nil, err
	}
	return mergePeople(peopleFromSocialUsers(followers), peopleFromSocialUsers(following)), nil
}

// Summary returns the caller's social summary, including the total number of
// people the account follows.
func (c FriendClient) Summary(ctx context.Context) (xblsocial.Summary, error) {
	return c.social().Summary(ctx)
}

// AcceptPendingFriendRequests accepts incoming Xbox friend requests with a
// single bulk add call and returns the people that Xbox reported as updated.
func (c FriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	socialClient := c.social()
	pending, err := socialClient.People(ctx, xblsocial.PeopleListIncomingFriendRequests, xblsocial.PeopleListConfig{Undecorated: true})
	if err != nil {
		return nil, err
	}
	xuids := make([]string, 0, len(pending))
	byXUID := make(map[string]Person, len(pending))
	for _, user := range pending {
		if user.XUID == "" {
			continue
		}
		xuids = append(xuids, user.XUID)
		byXUID[user.XUID] = personFromSocialUser(user)
	}
	if len(xuids) == 0 {
		return nil, nil
	}

	updatedXUIDs, err := socialClient.BulkAddFriends(ctx, xuids)
	if err != nil {
		return nil, err
	}
	updated := make(map[string]struct{}, len(updatedXUIDs))
	accepted := make([]Person, 0, len(updatedXUIDs))
	for _, xuid := range updatedXUIDs {
		if person, ok := byXUID[xuid]; ok {
			updated[xuid] = struct{}{}
			accepted = append(accepted, person)
		}
	}
	var failed []string
	for _, xuid := range xuids {
		if _, ok := updated[xuid]; !ok {
			failed = append(failed, xuid)
		}
	}
	if len(failed) > 0 {
		return accepted, &AcceptFriendRequestsError{Failed: failed}
	}
	return accepted, nil
}

// Follow follows the XUID, which makes the user a friend when they also follow
// the authenticated account.
func (c FriendClient) Follow(ctx context.Context, xuid string) error {
	return c.social().Follow(ctx, xuid)
}

// Unfollow removes the authenticated account's follow relationship for xuid.
func (c FriendClient) Unfollow(ctx context.Context, xuid string) error {
	return c.social().Unfollow(ctx, xuid)
}

// ForceUnfollow removes the follow relationship the user identified by xuid
// has towards the authenticated account. It is used to drop followers whose
// privacy or enforcement restrictions prevent a friendship.
func (c FriendClient) ForceUnfollow(ctx context.Context, xuid string) error {
	return c.social().RemoveFollower(ctx, xuid)
}

func (c FriendClient) social() *xblsocial.Client {
	if c.Social != nil {
		return c.Social
	}
	return xblsocial.New(c.client(), nil, xsts.UserInfo{}, nil)
}

func peopleFromSocialUsers(users []xblsocial.User) []Person {
	people := make([]Person, 0, len(users))
	for _, user := range users {
		people = append(people, personFromSocialUser(user))
	}
	return people
}

func personFromSocialUser(user xblsocial.User) Person {
	return Person{
		XUID:                 user.XUID,
		Gamertag:             user.GamerTag,
		DisplayName:          user.DisplayName,
		ModernGamertag:       user.ModernGamerTag,
		IsFollowingCaller:    user.Followed,
		IsFollowedByCaller:   user.Following,
		UniqueModernGamertag: user.UniqueModernGamerTag,
	}
}

func mergePeople(groups ...[]Person) []Person {
	merged := make(map[string]Person)
	order := make([]string, 0)
	for _, group := range groups {
		for _, person := range group {
			if person.XUID == "" {
				continue
			}
			existing, ok := merged[person.XUID]
			if !ok {
				merged[person.XUID] = person
				order = append(order, person.XUID)
				continue
			}
			merged[person.XUID] = mergePerson(existing, person)
		}
	}
	out := make([]Person, 0, len(order))
	for _, xuid := range order {
		out = append(out, merged[xuid])
	}
	return out
}

func mergePerson(existing, next Person) Person {
	existing.IsFollowedByCaller = existing.IsFollowedByCaller || next.IsFollowedByCaller
	existing.IsFollowingCaller = existing.IsFollowingCaller || next.IsFollowingCaller
	if existing.Gamertag == "" {
		existing.Gamertag = next.Gamertag
	}
	if existing.DisplayName == "" {
		existing.DisplayName = next.DisplayName
	}
	if existing.ModernGamertag == "" {
		existing.ModernGamertag = next.ModernGamertag
	}
	if existing.UniqueModernGamertag == "" {
		existing.UniqueModernGamertag = next.UniqueModernGamertag
	}
	return existing
}

func (c FriendClient) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}
