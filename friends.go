package broadcaster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
)

// FriendClient implements the Xbox social calls used by FriendSyncer with the
// same endpoints, contract versions, and request shapes as MCXboxBroadcast.
type FriendClient struct {
	Client *http.Client
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

// SocialSummary is the caller's follower/following totals from the Xbox
// social summary endpoint.
type SocialSummary struct {
	TargetFollowingCount int `json:"targetFollowingCount"`
	TargetFollowerCount  int `json:"targetFollowerCount"`
}

const (
	peopleHubFollowersURL = "https://peoplehub.xboxlive.com/users/me/people/followers"
	peopleHubSocialURL    = "https://peoplehub.xboxlive.com/users/me/people/social"
	pendingRequestsURL    = "https://peoplehub.xboxlive.com/users/me/people/friendrequests(received)"
	bulkAddFriendsURL     = "https://social.xboxlive.com/bulk/users/me/people/friends/v2?method=add"
	socialSummaryURL      = "https://social.xboxlive.com/users/me/summary"
	peopleURLFormat       = "https://social.xboxlive.com/users/me/people/xuid(%s)"
	followerURLFormat     = "https://social.xboxlive.com/users/me/people/follower/xuid(%s)"
)

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
// and people the authenticated account follows. Both lists are fetched
// undecorated with contract version 5, matching MCXboxBroadcast.
func (c FriendClient) Friends(ctx context.Context) ([]Person, error) {
	followers, err := c.people(ctx, peopleHubFollowersURL, "5")
	if err != nil {
		return nil, err
	}
	following, err := c.people(ctx, peopleHubSocialURL, "5")
	if err != nil {
		return nil, err
	}
	return mergePeople(followers, following), nil
}

// Summary returns the caller's social summary, including the total number of
// people the account follows.
func (c FriendClient) Summary(ctx context.Context) (SocialSummary, error) {
	var summary SocialSummary
	resp, err := c.do(ctx, http.MethodGet, socialSummaryURL, "3", nil)
	if err != nil {
		return SocialSummary{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return SocialSummary{}, socialResponseError(resp)
	}
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return SocialSummary{}, err
	}
	return summary, nil
}

// AcceptPendingFriendRequests accepts incoming Xbox friend requests with a
// single bulk add call and returns the people that Xbox reported as updated.
func (c FriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	pending, err := c.people(ctx, pendingRequestsURL, "7")
	if err != nil {
		return nil, err
	}
	xuids := make([]string, 0, len(pending))
	byXUID := make(map[string]Person, len(pending))
	for _, person := range pending {
		if person.XUID == "" {
			continue
		}
		xuids = append(xuids, person.XUID)
		byXUID[person.XUID] = person
	}
	if len(xuids) == 0 {
		return nil, nil
	}

	body, err := json.Marshal(map[string][]string{"xuids": xuids})
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, http.MethodPost, bulkAddFriendsURL, "1", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, socialResponseError(resp)
	}
	var result struct {
		UpdatedPeople []string `json:"updatedPeople"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	updated := make(map[string]struct{}, len(result.UpdatedPeople))
	accepted := make([]Person, 0, len(result.UpdatedPeople))
	for _, xuid := range result.UpdatedPeople {
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
	return c.relationship(ctx, http.MethodPut, fmt.Sprintf(peopleURLFormat, xuid), http.StatusOK, http.StatusNoContent)
}

// Unfollow removes the authenticated account's follow relationship for xuid.
func (c FriendClient) Unfollow(ctx context.Context, xuid string) error {
	return c.relationship(ctx, http.MethodDelete, fmt.Sprintf(peopleURLFormat, xuid), http.StatusOK, http.StatusNoContent)
}

// ForceUnfollow removes the follow relationship the user identified by xuid
// has towards the authenticated account. It is used to drop followers whose
// privacy or enforcement restrictions prevent a friendship.
func (c FriendClient) ForceUnfollow(ctx context.Context, xuid string) error {
	return c.relationship(ctx, http.MethodDelete, fmt.Sprintf(followerURLFormat, xuid), http.StatusOK, http.StatusNoContent)
}

// relationship sends a relationship mutation and converts non-success
// responses into social response errors.
func (c FriendClient) relationship(ctx context.Context, method, url string, successCodes ...int) error {
	resp, err := c.do(ctx, method, url, "3", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	for _, code := range successCodes {
		if resp.StatusCode == code {
			return nil
		}
	}
	return socialResponseError(resp)
}

// people fetches a PeopleHub or social people list.
func (c FriendClient) people(ctx context.Context, url, contractVersion string) ([]Person, error) {
	resp, err := c.do(ctx, http.MethodGet, url, contractVersion, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, socialResponseError(resp)
	}
	var result struct {
		People []Person `json:"people"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.People, nil
}

func (c FriendClient) do(ctx context.Context, method, url, contractVersion string, body io.Reader) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Xbl-Contract-Version", contractVersion)
	req.Header.Set("Accept-Language", "en-GB")
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.client().Do(req)
}

// socialResponseError converts a non-success social/PeopleHub response into an
// *xblsocial.ResponseError so callers can match rate limits and restriction
// codes with errors.Is/As.
func socialResponseError(resp *http.Response) error {
	responseErr := &xblsocial.ResponseError{
		StatusCode: resp.StatusCode,
		RetryAfter: retryAfterHeader(resp.Header.Get("Retry-After")),
	}
	if resp.Request != nil {
		responseErr.Method = resp.Request.Method
		if resp.Request.URL != nil {
			responseErr.URL = resp.Request.URL.String()
		}
	}
	var body struct {
		Code        int    `json:"code"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err == nil {
		responseErr.Code = body.Code
		responseErr.Description = body.Description
	}
	return responseErr
}

func retryAfterHeader(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return 0
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
