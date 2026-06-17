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

	"golang.org/x/oauth2"
)

func TestFriendClientFriendsMergesFollowersAndSocial(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body := `{"people":[]}`
			switch req.URL.String() {
			case followersURL:
				body = `{"people":[{"xuid":"1","gamertag":"Follower","isFollowingCaller":true}]}`
			case socialURL:
				body = `{"people":[{"xuid":"1","gamertag":"Follower","isFollowedByCaller":true},{"xuid":"2","gamertag":"Social","isFollowedByCaller":true}]}`
			default:
				t.Fatalf("unexpected URL %s", req.URL)
			}
			return response(http.StatusOK, body), nil
		})},
	}
	people, err := client.Friends(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(people) != 2 {
		t.Fatalf("expected 2 people, got %d", len(people))
	}
	var merged Person
	for _, p := range people {
		if p.XUID == "1" {
			merged = p
		}
	}
	if !merged.IsFollowedByCaller || !merged.IsFollowingCaller {
		t.Fatalf("expected merged follow flags, got %#v", merged)
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
			if req.URL.String() != fmt.Sprintf(peopleURL, "123") {
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
	var retry interface {
		RetryDelay() time.Duration
	}
	if !errors.As(err, &retry) {
		t.Fatalf("expected retry-after error, got %T: %v", err, err)
	}
	if retry.RetryDelay() != 7*time.Second {
		t.Fatalf("retry delay = %s, want 7s", retry.RetryDelay())
	}
}

func TestFriendClientUnfollowReturnsRetryAfterError(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			resp := response(http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "3")
			return resp, nil
		})},
	}
	err := client.Unfollow(context.Background(), "123")
	if err == nil {
		t.Fatal("expected retry-after error")
	}
	var retry interface {
		RetryDelay() time.Duration
	}
	if !errors.As(err, &retry) {
		t.Fatalf("expected retry-after error, got %T: %v", err, err)
	}
	if retry.RetryDelay() != 3*time.Second {
		t.Fatalf("retry delay = %s, want 3s", retry.RetryDelay())
	}
}

func TestFriendClientFollowClassifiesSocialModifyErrors(t *testing.T) {
	tests := []struct {
		name string
		body string
		code int
		kind string
	}{
		{
			name: "friend list full",
			body: `{"code":1028,"description":"The attempted People request was rejected because it would exceed the People list limit."}`,
			code: 1028,
			kind: "friend_list_full",
		},
		{
			name: "restricted account",
			body: `{"code":1049,"description":"Target user privacy settings do not allow friend requests to be received."}`,
			code: 1049,
			kind: "restricted",
		},
		{
			name: "blocked or forbidden",
			body: `{"code":1011,"description":"The requested friend operation was forbidden."}`,
			code: 1011,
			kind: "restricted",
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
			var social interface {
				XboxSocialCode() int
				FriendErrorKind() string
			}
			if !errors.As(err, &social) {
				t.Fatalf("expected social error, got %T: %v", err, err)
			}
			if social.XboxSocialCode() != tt.code {
				t.Fatalf("social code = %d, want %d", social.XboxSocialCode(), tt.code)
			}
			if social.FriendErrorKind() != tt.kind {
				t.Fatalf("kind = %q, want %q", social.FriendErrorKind(), tt.kind)
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
			case req.Method == http.MethodGet && req.URL.String() == pendingFriendRequestsURL:
				if req.Header.Get("X-Xbl-Contract-Version") != "7" {
					t.Fatalf("contract version = %q, want 7", req.Header.Get("X-Xbl-Contract-Version"))
				}
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case req.Method == http.MethodPost && req.URL.String() == acceptFriendRequestsURL:
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				if string(body) != `{"xuids":["1","2"]}` {
					t.Fatalf("accept body = %s", body)
				}
				return response(http.StatusOK, `{"updatedPeople":["2","1"]}`), nil
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
	if got, want := strings.Join(requests, ","), http.MethodGet+" "+pendingFriendRequestsURL+","+http.MethodPost+" "+acceptFriendRequestsURL; got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
	if len(accepted) != 2 || accepted[0].XUID != "2" || accepted[1].XUID != "1" {
		t.Fatalf("accepted people = %#v", accepted)
	}
}

func TestFriendClientAcceptPendingFriendRequestsAllowsNoContentSuccess(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case http.MethodPost:
				return response(http.StatusNoContent, ""), nil
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
			case http.MethodPost:
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
	var retry interface {
		RetryDelay() time.Duration
	}
	if !errors.As(err, &retry) {
		t.Fatalf("expected retry-after error, got %T: %v", err, err)
	}
	if retry.RetryDelay() != 11*time.Second {
		t.Fatalf("retry delay = %s, want 11s", retry.RetryDelay())
	}
}

func TestFriendClientAcceptPendingFriendRequestsReportsFailedUpdates(t *testing.T) {
	client := FriendClient{
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case http.MethodPost:
				return response(http.StatusOK, `{"updatedPeople":["1"],"failedToUpdate":["2"]}`), nil
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
