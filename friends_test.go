package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
	"golang.org/x/oauth2"
)

const socialDecorations = "bio,detail,multiplayerSummary,preferredColor,presenceDetail"

func peopleHubURL(group string) string {
	return "https://peoplehub.xboxlive.com/users/me/people/" + group + "/decoration/" + socialDecorations
}

func followURL(xuid string) string {
	return fmt.Sprintf("https://social.xboxlive.com/users/me/people/xuid(%s)", xuid)
}

func addFriendURL(xuid string) string {
	return fmt.Sprintf("https://social.xboxlive.com/users/me/people/friends/v2/xuid(%s)", xuid)
}

func unfollowURL(xuid string) string {
	return addFriendURL(xuid) + "?deleteRelationships=follows"
}

func TestFriendClientFriendsMergesFollowersAndSocial(t *testing.T) {
	var requests []string
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.URL.String())
			switch req.URL.String() {
			case peopleHubURL("followers"):
				if req.Header.Get("X-Xbl-Contract-Version") != "7" {
					t.Fatalf("contract version = %q, want 7", req.Header.Get("X-Xbl-Contract-Version"))
				}
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"Follower","displayName":"Display","modernGamertag":"Modern","uniqueModernGamertag":"Modern#1234","isFollowingCaller":true}]}`), nil
			case peopleHubURL("social"):
				return response(http.StatusOK, `{"people":[{"xuid":"1","isFollowedByCaller":true},{"xuid":"2","gamertag":"Followed","isFollowedByCaller":true}]}`), nil
			default:
				t.Fatalf("unexpected URL %s", req.URL)
			}
			return nil, nil
		})},
	}
	people, err := client.Friends(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRequests := strings.Join([]string{peopleHubURL("followers"), peopleHubURL("social")}, ",")
	if got := strings.Join(requests, ","); got != wantRequests {
		t.Fatalf("requests = %s, want %s", got, wantRequests)
	}
	if len(people) != 2 {
		t.Fatalf("expected 2 people, got %d", len(people))
	}
	var mapped Person
	for _, p := range people {
		if p.XUID == "1" {
			mapped = p
		}
	}
	if !mapped.IsFollowedByCaller || !mapped.IsFollowingCaller {
		t.Fatalf("expected mapped follow flags, got %#v", mapped)
	}
	if mapped.Gamertag != "Follower" || mapped.DisplayName != "Display" || mapped.ModernGamertag != "Modern" || mapped.UniqueModernGamertag != "Modern#1234" {
		t.Fatalf("unexpected mapped profile fields: %#v", mapped)
	}
}

func TestFriendClientFollowUsesContextAndAuth(t *testing.T) {
	called := false
	client := FriendClient{
		Client: testAuthenticatedClient("XBL3.0 x=user;token", roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			if req.Header.Get("Authorization") == "" {
				t.Fatal("missing authorization header")
			}
			if req.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", req.Method)
			}
			if req.URL.String() != followURL("123") {
				t.Fatalf("unexpected URL %s", req.URL)
			}
			return response(http.StatusNoContent, ""), nil
		})),
	}
	if err := client.Follow(context.Background(), "123"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("client was not called")
	}
}

func TestFriendClientFollowReturnsRetryAfterError(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			resp := response(http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "7")
			return resp, nil
		})},
	}
	err := client.Follow(context.Background(), "123")
	if err == nil {
		t.Fatal("expected retry-after error")
	}
	var responseErr *xblsocial.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("expected social response error, got %T: %v", err, err)
	}
	if responseErr.RetryAfter != 7*time.Second {
		t.Fatalf("retry delay = %s, want 7s", responseErr.RetryAfter)
	}
}

func TestFriendClientUnfollowReturnsRetryAfterError(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodDelete {
				t.Fatalf("unexpected method %s", req.Method)
			}
			if req.URL.String() != unfollowURL("123") {
				t.Fatalf("unexpected URL %s", req.URL)
			}
			resp := response(http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "3")
			return resp, nil
		})},
	}
	err := client.Unfollow(context.Background(), "123")
	if err == nil {
		t.Fatal("expected retry-after error")
	}
	var responseErr *xblsocial.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("expected social response error, got %T: %v", err, err)
	}
	if responseErr.RetryAfter != 3*time.Second {
		t.Fatalf("retry delay = %s, want 3s", responseErr.RetryAfter)
	}
}

func TestFriendClientFollowReturnsSocialResponseErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		code int
		want error
	}{
		{
			name: "friend list full",
			body: `{"code":1028,"description":"The attempted People request was rejected because it would exceed the People list limit."}`,
			code: 1028,
			want: xblsocial.ErrFriendListFull,
		},
		{
			name: "restricted account",
			body: `{"code":1049,"description":"Target user privacy settings do not allow friend requests to be received."}`,
			code: 1049,
			want: xblsocial.ErrFriendRestricted,
		},
		{
			name: "blocked or forbidden",
			body: `{"code":1011,"description":"The requested friend operation was forbidden."}`,
			code: 1011,
			want: xblsocial.ErrFriendRestricted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := FriendClient{
				Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return response(http.StatusBadRequest, tt.body), nil
				})},
			}
			err := client.Follow(context.Background(), "123")
			if err == nil {
				t.Fatal("expected social error")
			}
			var responseErr *xblsocial.ResponseError
			if !errors.As(err, &responseErr) {
				t.Fatalf("expected social response error, got %T: %v", err, err)
			}
			if responseErr.Code != tt.code {
				t.Fatalf("social code = %d, want %d", responseErr.Code, tt.code)
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("errors.Is(%v) = false for %T: %v", tt.want, err, err)
			}
		})
	}
}

func TestFriendClientAcceptPendingFriendRequests(t *testing.T) {
	var requests []string
	client := FriendClient{
		Client: testAuthenticatedClient("XBL3.0 x=user;token", roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.Method+" "+req.URL.String())
			if req.Header.Get("Authorization") == "" {
				t.Fatal("missing authorization header")
			}
			switch {
			case req.Method == http.MethodGet && req.URL.String() == peopleHubURL("friendRequests(received)"):
				if req.Header.Get("X-Xbl-Contract-Version") != "7" {
					t.Fatalf("contract version = %q, want 7", req.Header.Get("X-Xbl-Contract-Version"))
				}
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case req.Method == http.MethodPut && req.URL.String() == addFriendURL("1"):
				return response(http.StatusOK, ""), nil
			case req.Method == http.MethodPut && req.URL.String() == addFriendURL("2"):
				return response(http.StatusOK, ""), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})),
	}
	accepter, ok := any(client).(interface {
		AcceptPendingFriendRequests(context.Context) ([]Person, error)
	})
	if !ok {
		t.Fatal("FriendClient does not accept pending friend requests")
	}
	accepted, err := accepter.AcceptPendingFriendRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRequests := strings.Join([]string{
		http.MethodGet + " " + peopleHubURL("friendRequests(received)"),
		http.MethodPut + " " + addFriendURL("1"),
		http.MethodPut + " " + addFriendURL("2"),
	}, ",")
	if got := strings.Join(requests, ","); got != wantRequests {
		t.Fatalf("requests = %s, want %s", got, wantRequests)
	}
	if len(accepted) != 2 || accepted[0].XUID != "1" || accepted[1].XUID != "2" {
		t.Fatalf("accepted people = %#v", accepted)
	}
}

func TestFriendClientAcceptPendingFriendRequestsReturnsAcceptedPeople(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case http.MethodPut:
				return response(http.StatusOK, ""), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}

	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 2 || accepted[0].XUID != "1" || accepted[1].XUID != "2" {
		t.Fatalf("accepted people = %#v", accepted)
	}
}

func TestFriendClientAcceptPendingFriendRequestsReturnsRetryAfterError(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"}]}`), nil
			case http.MethodPut:
				resp := response(http.StatusTooManyRequests, "")
				resp.Header.Set("Retry-After", "11")
				return resp, nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}
	accepter, ok := any(client).(interface {
		AcceptPendingFriendRequests(context.Context) ([]Person, error)
	})
	if !ok {
		t.Fatal("FriendClient does not accept pending friend requests")
	}
	_, err := accepter.AcceptPendingFriendRequests(context.Background())
	if err == nil {
		t.Fatal("expected retry-after error")
	}
	var responseErr *xblsocial.ResponseError
	if !errors.As(err, &responseErr) {
		t.Fatalf("expected social response error, got %T: %v", err, err)
	}
	if responseErr.RetryAfter != 11*time.Second {
		t.Fatalf("retry delay = %s, want 11s", responseErr.RetryAfter)
	}
}

func TestFriendClientAcceptPendingFriendRequestsReportsFailedUpdates(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch {
			case req.Method == http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case req.Method == http.MethodPut && req.URL.String() == addFriendURL("1"):
				return response(http.StatusOK, ""), nil
			case req.Method == http.MethodPut && req.URL.String() == addFriendURL("2"):
				return response(http.StatusBadRequest, `{"code":1049,"description":"restricted"}`), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})},
	}

	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err == nil {
		t.Fatal("expected failed updates error")
	}
	if len(accepted) != 1 || accepted[0].XUID != "1" {
		t.Fatalf("accepted people = %#v, want xuid 1", accepted)
	}
	var acceptErr interface {
		FailedXUIDs() []string
	}
	if !errors.As(err, &acceptErr) {
		t.Fatalf("expected accept failure error, got %T: %v", err, err)
	}
	if got := strings.Join(acceptErr.FailedXUIDs(), ","); got != "2" {
		t.Fatalf("failed xuids = %s, want 2", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testAuthenticatedClient(auth string, next http.RoundTripper) *http.Client {
	return &http.Client{Transport: authenticatedTestTransport{auth: auth, next: next}}
}

type authenticatedTestTransport struct {
	auth string
	next http.RoundTripper
}

func (t authenticatedTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("Authorization", t.auth)
	return t.next.RoundTrip(req)
}

func response(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

type staticOAuthSource struct{}

func (staticOAuthSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "live"}, nil
}
