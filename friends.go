package broadcaster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/df-mc/go-xsapi"
)

const (
	followersURL              = "https://peoplehub.xboxlive.com/users/me/people/followers"
	socialURL                 = "https://peoplehub.xboxlive.com/users/me/people/social"
	pendingFriendRequestsURL  = "https://peoplehub.xboxlive.com/users/me/people/friendrequests(received)"
	acceptFriendRequestsURL   = "https://social.xboxlive.com/bulk/users/me/people/friends/v2?method=add"
	peopleURL                 = "https://social.xboxlive.com/users/me/people/xuid(%s)"
	followerURL               = "https://social.xboxlive.com/users/me/people/follower/xuid(%s)"
	FriendErrorKindUnknown    = "unknown"
	FriendErrorKindFullList   = "friend_list_full"
	FriendErrorKindRestricted = "restricted"
)

// FriendClient wraps the Xbox social endpoints used by MCXboxBroadcast for
// follower/friend synchronization.
type FriendClient struct {
	TokenSource xsapi.TokenSource
	Client      *http.Client
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

type peopleResponse struct {
	People []Person `json:"people"`
}

type acceptFriendRequestsResponse struct {
	FailedToUpdate []string `json:"failedToUpdate"`
	UpdatedPeople  []string `json:"updatedPeople"`
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

// RetryAfterError reports an Xbox social rate limit and the server requested
// delay before retrying.
type RetryAfterError struct {
	Method     string
	URL        string
	StatusCode int
	Delay      time.Duration
}

func (e *RetryAfterError) Error() string {
	if e.Delay <= 0 {
		return fmt.Sprintf("%s %s: rate limited", e.Method, e.URL)
	}
	return fmt.Sprintf("%s %s: rate limited, retry after %s", e.Method, e.URL, e.Delay)
}

func (e *RetryAfterError) RetryDelay() time.Duration {
	return e.Delay
}

// FriendSocialError carries Xbox social modify error details, including known
// friend-list hard-cap and restricted/privacy classifications.
type FriendSocialError struct {
	Method      string
	URL         string
	StatusCode  int
	Code        int
	Description string
	Source      string
	Kind        string
}

func (e *FriendSocialError) Error() string {
	if e.Code == 0 {
		return fmt.Sprintf("%s %s: %d", e.Method, e.URL, e.StatusCode)
	}
	if e.Description == "" {
		return fmt.Sprintf("%s %s: xbox social code %d", e.Method, e.URL, e.Code)
	}
	return fmt.Sprintf("%s %s: xbox social code %d: %s", e.Method, e.URL, e.Code, e.Description)
}

func (e *FriendSocialError) XboxSocialCode() int {
	return e.Code
}

func (e *FriendSocialError) FriendErrorKind() string {
	return e.Kind
}

func IsFriendListFull(err error) bool {
	var social interface {
		FriendErrorKind() string
	}
	return errors.As(err, &social) && social.FriendErrorKind() == FriendErrorKindFullList
}

func IsFriendRestricted(err error) bool {
	var social interface {
		FriendErrorKind() string
	}
	return errors.As(err, &social) && social.FriendErrorKind() == FriendErrorKindRestricted
}

// Followers returns people following the authenticated account.
func (c FriendClient) Followers(ctx context.Context) ([]Person, error) {
	return c.people(ctx, followersURL)
}

// Social returns people the authenticated account follows.
func (c FriendClient) Social(ctx context.Context) ([]Person, error) {
	return c.people(ctx, socialURL)
}

// Friends returns a merged view of followers and followed people.
func (c FriendClient) Friends(ctx context.Context) ([]Person, error) {
	followers, err := c.Followers(ctx)
	if err != nil {
		return nil, err
	}
	social, err := c.Social(ctx)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]Person, len(followers)+len(social))
	for _, p := range append(followers, social...) {
		if existing, ok := merged[p.XUID]; ok {
			p.IsFollowedByCaller = p.IsFollowedByCaller || existing.IsFollowedByCaller
			p.IsFollowingCaller = p.IsFollowingCaller || existing.IsFollowingCaller
			if p.Gamertag == "" {
				p.Gamertag = existing.Gamertag
			}
		}
		merged[p.XUID] = p
	}
	out := make([]Person, 0, len(merged))
	for _, p := range merged {
		out = append(out, p)
	}
	return out, nil
}

// Follow follows the XUID, which makes the user a friend when they also follow
// the authenticated account.
func (c FriendClient) Follow(ctx context.Context, xuid string) error {
	return c.empty(ctx, http.MethodPut, fmt.Sprintf(peopleURL, xuid))
}

// Unfollow removes the authenticated account's follow relationship for xuid.
func (c FriendClient) Unfollow(ctx context.Context, xuid string) error {
	return c.empty(ctx, http.MethodDelete, fmt.Sprintf(peopleURL, xuid))
}

// AcceptPendingFriendRequests accepts incoming Xbox friend requests and returns
// the people that Xbox reported as updated.
func (c FriendClient) AcceptPendingFriendRequests(ctx context.Context) ([]Person, error) {
	req, err := c.request(ctx, http.MethodGet, pendingFriendRequestsURL)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Xbl-Contract-Version", "7")
	req.Header.Set("Accept-Language", "en-US")
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.responseError(req, resp)
	}
	var pending peopleResponse
	if err := json.NewDecoder(resp.Body).Decode(&pending); err != nil {
		return nil, err
	}
	if len(pending.People) == 0 {
		return nil, nil
	}

	xuids := make([]string, 0, len(pending.People))
	byXUID := make(map[string]Person, len(pending.People))
	for _, p := range pending.People {
		if p.XUID == "" {
			continue
		}
		xuids = append(xuids, p.XUID)
		byXUID[p.XUID] = p
	}
	if len(xuids) == 0 {
		return nil, nil
	}

	body, err := json.Marshal(struct {
		XUIDs []string `json:"xuids"`
	}{XUIDs: xuids})
	if err != nil {
		return nil, err
	}
	req, err = c.requestWithBody(ctx, http.MethodPost, acceptFriendRequestsURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = c.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, c.responseError(req, resp)
	}
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(responseBody)) == 0 {
		return pending.People, nil
	}
	var accepted acceptFriendRequestsResponse
	if err := json.Unmarshal(responseBody, &accepted); err != nil {
		return nil, err
	}
	out := make([]Person, 0, len(accepted.UpdatedPeople))
	for _, xuid := range accepted.UpdatedPeople {
		p, ok := byXUID[xuid]
		if !ok {
			p = Person{XUID: xuid}
		}
		out = append(out, p)
	}
	if len(accepted.FailedToUpdate) > 0 {
		return out, &AcceptFriendRequestsError{Failed: append([]string(nil), accepted.FailedToUpdate...)}
	}
	return out, nil
}

func (c FriendClient) people(ctx context.Context, url string) ([]Person, error) {
	req, err := c.request(ctx, http.MethodGet, url)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Xbl-Contract-Version", "5")
	req.Header.Set("Accept-Language", "en-US")
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, c.responseError(req, resp)
	}
	var data peopleResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	return data.People, nil
}

func (c FriendClient) empty(ctx context.Context, method, url string) error {
	req, err := c.request(ctx, method, url)
	if err != nil {
		return err
	}
	resp, err := c.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return c.responseError(req, resp)
	}
	return nil
}

func (c FriendClient) request(ctx context.Context, method, url string) (*http.Request, error) {
	return c.requestWithBody(ctx, method, url, nil)
}

func (c FriendClient) requestWithBody(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.TokenSource == nil {
		return nil, fmt.Errorf("token source is nil")
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	tok, err := c.TokenSource.Token()
	if err != nil {
		return nil, fmt.Errorf("request token: %w", err)
	}
	tok.SetAuthHeader(req)
	return req, nil
}

func (c FriendClient) responseError(req *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusTooManyRequests {
		return &RetryAfterError{
			Method:     req.Method,
			URL:        req.URL.String(),
			StatusCode: resp.StatusCode,
			Delay:      parseRetryAfter(resp.Header.Get("Retry-After")),
		}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s %s: %s: read error body: %w", req.Method, req.URL, resp.Status, err)
	}
	social := decodeFriendSocialError(body)
	social.Method = req.Method
	social.URL = req.URL.String()
	social.StatusCode = resp.StatusCode
	if social.Kind == "" {
		social.Kind = FriendErrorKindUnknown
	}
	return social
}

func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	delay := time.Until(when)
	if delay < 0 {
		return 0
	}
	return delay
}

func decodeFriendSocialError(body []byte) *FriendSocialError {
	var data struct {
		Code        int    `json:"code"`
		Description string `json:"description"`
		Source      string `json:"source"`
	}
	_ = json.Unmarshal(body, &data)
	return &FriendSocialError{
		Code:        data.Code,
		Description: data.Description,
		Source:      data.Source,
		Kind:        classifyFriendSocialCode(data.Code),
	}
}

func classifyFriendSocialCode(code int) string {
	switch code {
	case 1028:
		return FriendErrorKindFullList
	case 1011, 1049:
		return FriendErrorKindRestricted
	default:
		return FriendErrorKindUnknown
	}
}

func (c FriendClient) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}
