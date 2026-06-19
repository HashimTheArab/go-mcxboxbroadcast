package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

// FriendClient adapts go-xsapi/v2's Xbox social client to the FriendSyncer API.
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
	// go-xsapi/social.Friends only returns accepted friends; sync needs both
	// sides of the relationship so pending inbound followers can be accepted.
	socialClient := c.social()
	followers, err := socialClient.Followers(ctx)
	if err != nil {
		return nil, err
	}
	following, err := socialClient.Following(ctx)
	if err != nil {
		return nil, err
	}
	return mergePeople(peopleFromSocialUsers(followers), peopleFromSocialUsers(following)), nil
}

// AcceptPendingFriendRequests accepts incoming Xbox friend requests and returns
// the people that Xbox reported as updated.
func (c FriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	socialClient := c.social()
	pending, err := socialClient.IncomingFriendRequests(ctx)
	if err != nil {
		return nil, err
	}
	if len(pending) == 0 {
		return nil, nil
	}

	accepted := make([]Person, 0, len(pending))
	var failed []string
	for _, user := range pending {
		if user.XUID == "" {
			continue
		}
		if err := socialClient.AddFriend(ctx, user.XUID); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || retryDelay(err) > 0 {
				return accepted, err
			}
			failed = append(failed, user.XUID)
			continue
		}
		accepted = append(accepted, personFromSocialUser(user))
	}
	if len(failed) > 0 {
		return accepted, &AcceptFriendRequestsError{Failed: failed}
	}
	return accepted, nil
}

func (c FriendClient) social() *xblsocial.Client {
	if c.Social != nil {
		return c.Social
	}
	return xblsocial.New(c.client(), nil, xsts.UserInfo{}, nil)
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

func peopleFromSocialUsers(users []xblsocial.User) []Person {
	people := make([]Person, 0, len(users))
	for _, user := range users {
		people = append(people, personFromSocialUser(user))
	}
	return people
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

func (c FriendClient) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}
