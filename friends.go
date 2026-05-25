package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/df-mc/go-xsapi"
)

const (
	followersURL = "https://peoplehub.xboxlive.com/users/me/people/followers"
	socialURL    = "https://peoplehub.xboxlive.com/users/me/people/social"
	peopleURL    = "https://social.xboxlive.com/users/me/people/xuid(%s)"
	followerURL  = "https://social.xboxlive.com/users/me/people/follower/xuid(%s)"
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
		return nil, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
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
		return fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	return nil
}

func (c FriendClient) request(ctx context.Context, method, url string) (*http.Request, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.TokenSource == nil {
		return nil, fmt.Errorf("token source is nil")
	}
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
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

func (c FriendClient) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}
