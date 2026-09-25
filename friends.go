package broadcaster

import (
	"context"
	"errors"
	"net/http"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// FriendClient adapts go-xsapi/v2's Xbox social client to the FriendSyncer
// API, using the same endpoints, contract versions, and request shapes as
// MCXboxBroadcast.
type FriendClient struct {
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

// addFriendsBatchSize stays below Xbox Live's undocumented bulk-operation
// limit. Rejected batches are also split dynamically; see acceptFriends.
const addFriendsBatchSize = 50

// FriendRequestResult reports what happened to incoming friend requests.
type FriendRequestResult struct {
	// Accepted lists requests Xbox reported as accepted.
	Accepted []Person
	// Rejected lists requests Xbox refused, each with the reason.
	Rejected []RejectedFriendRequest
	// Waiting counts requests still pending afterwards, including skipped ones.
	Waiting int
}

// RejectedFriendRequest is an incoming friend request Xbox refused to accept.
// Err matches [xblsocial.ErrFriendRestricted] when the person's privacy or
// enforcement settings block the friendship, and [xblsocial.ErrFriendListFull]
// when either the account's or the requester's list is full.
type RejectedFriendRequest struct {
	Person Person
	Err    error
}

// ErrFriendRequestNotAccepted is reported for a request a bulk accept listed
// as failed without giving a reason.
var ErrFriendRequestNotAccepted = errors.New("xbox did not accept the friend request")

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

// AcceptPendingFriendRequests accepts incoming Xbox friend requests in bounded
// batches, leaving requests for which skip reports true pending. It stops at
// the first rate limit or server error; refusals are narrowed down to the
// people they apply to and reported in Rejected.
func (c FriendClient) AcceptPendingFriendRequests(ctx context.Context, skip func(xuid string) bool) (FriendRequestResult, error) {
	var result FriendRequestResult
	pending, err := c.social().People(ctx, xblsocial.PeopleListIncomingFriendRequests, xblsocial.PeopleListConfig{Undecorated: true})
	if err != nil {
		return result, err
	}
	byXUID := make(map[string]Person, len(pending))
	xuids := make([]string, 0, len(pending))
	for _, user := range pending {
		if _, dup := byXUID[user.XUID]; user.XUID == "" || dup {
			continue
		}
		byXUID[user.XUID] = personFromSocialUser(user)
		if skip == nil || !skip(user.XUID) {
			xuids = append(xuids, user.XUID)
		}
	}
	for start := 0; start < len(xuids) && err == nil; start += addFriendsBatchSize {
		err = c.acceptFriends(ctx, xuids[start:min(start+addFriendsBatchSize, len(xuids))], byXUID, &result)
	}
	result.Waiting = len(byXUID) - len(result.Accepted)
	return result, err
}

// acceptFriends accepts xuids, splitting a refused batch down to the people it
// applies to. It returns an error only when later batches should not be tried.
func (c FriendClient) acceptFriends(ctx context.Context, xuids []string, byXUID map[string]Person, result *FriendRequestResult) error {
	bulk, err := c.social().AddFriends(ctx, xuids)
	if err == nil {
		updated := make(map[string]struct{}, len(bulk.Updated))
		for _, xuid := range bulk.Updated {
			person, ok := byXUID[xuid]
			if _, dup := updated[xuid]; !ok || dup {
				continue
			}
			updated[xuid] = struct{}{}
			result.Accepted = append(result.Accepted, person)
		}
		for _, xuid := range xuids {
			if _, ok := updated[xuid]; !ok {
				result.Rejected = append(result.Rejected, RejectedFriendRequest{Person: byXUID[xuid], Err: ErrFriendRequestNotAccepted})
			}
		}
		return nil
	}
	// A full-list refusal may be one requester's list, so it is split like any other.
	switch {
	case !isRequestRefusal(err):
		return err
	case len(xuids) == 1:
		result.Rejected = append(result.Rejected, RejectedFriendRequest{Person: byXUID[xuids[0]], Err: err})
		return nil
	}
	middle := len(xuids) / 2
	if err := c.acceptFriends(ctx, xuids[:middle], byXUID, result); err != nil {
		return err
	}
	return c.acceptFriends(ctx, xuids[middle:], byXUID, result)
}

// isRequestRefusal reports whether err is a client error that may apply to
// only some people in a bulk request, rather than a rate limit or auth failure.
func isRequestRefusal(err error) bool {
	var responseErr *xblsocial.ResponseError
	if !errors.As(err, &responseErr) {
		return false
	}
	code := responseErr.StatusCode
	return code >= 400 && code < 500 && code != http.StatusTooManyRequests && code != http.StatusUnauthorized
}

// Follow follows the XUID, which makes the user a friend when they also follow
// the authenticated account.
func (c FriendClient) Follow(ctx context.Context, xuid string) error {
	return c.social().Follow(ctx, xuid)
}

// Unfollow drops the authenticated account's follow of xuid. Xbox keeps the
// user's follow of the account, so use it only for people who do not follow back.
func (c FriendClient) Unfollow(ctx context.Context, xuid string) error {
	return c.social().RemoveMutualFollow(ctx, xuid)
}

// RemoveFriend ends the friendship with xuid, or declines their pending
// request, so a new request is needed to be friends again. It does not remove
// a one-way follow; a 404 means there was no friendship or request to end.
func (c FriendClient) RemoveFriend(ctx context.Context, xuid string) error {
	return c.social().RemoveFriend(ctx, xuid)
}

// RemoveFollower drops xuid's follow of the authenticated account.
func (c FriendClient) RemoveFollower(ctx context.Context, xuid string) error {
	return ignoreNotFound(c.social().RemoveFollower(ctx, xuid))
}

// ignoreNotFound treats a missing relationship as already removed.
func ignoreNotFound(err error) error {
	if isNotFound(err) {
		return nil
	}
	return err
}

// isNotFound reports whether err is a social 404.
func isNotFound(err error) bool {
	var responseErr *xblsocial.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == http.StatusNotFound
}

func (c FriendClient) social() *xblsocial.Client {
	return c.Social
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
