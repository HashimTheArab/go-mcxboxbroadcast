package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

const (
	peopleHubDecorations      = "bio,detail,multiplayerSummary,preferredColor,presenceDetail"
	peopleHubGroupURL         = "https://peoplehub.xboxlive.com/users/me/people/%s/decoration/" + peopleHubDecorations
	FriendErrorKindUnknown    = "unknown"
	FriendErrorKindFullList   = "friend_list_full"
	FriendErrorKindRestricted = "restricted"
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

type peopleHubResponse struct {
	People []xblsocial.User `json:"people"`
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

// Friends returns a merged view of people following the authenticated account
// and people the authenticated account follows.
func (c FriendClient) Friends(ctx context.Context) ([]Person, error) {
	followers, err := c.peopleHubGroup(ctx, "followers")
	if err != nil {
		return nil, err
	}
	social, err := c.peopleHubGroup(ctx, "social")
	if err != nil {
		return nil, err
	}
	return mergePeople(followers, social), nil
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
	return xblsocial.New(classifyingFriendHTTPClient(c.client()), nil, xsts.UserInfo{}, nil)
}

func (c FriendClient) peopleHubGroup(ctx context.Context, group string) ([]Person, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf(peopleHubGroupURL, group), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Xbl-Contract-Version", "7")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US")

	resp, err := c.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, friendResponseError(req, resp)
	}
	var data peopleHubResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	return peopleFromSocialUsers(data.People), nil
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

func classifyingFriendHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	clone := *client
	base := clone.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	clone.Transport = friendErrorTransport{base: base}
	return &clone
}

type friendErrorTransport struct {
	base http.RoundTripper
}

func (t friendErrorTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	err = friendResponseError(req, resp)
	_ = resp.Body.Close()
	return nil, err
}

func friendResponseError(req *http.Request, resp *http.Response) error {
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
