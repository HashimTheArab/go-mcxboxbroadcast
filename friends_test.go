package broadcaster

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"golang.org/x/oauth2"
)

func TestFriendClientFriendsMergesFollowersAndSocial(t *testing.T) {
	client := FriendClient{
		TokenSource: staticTokenSource{},
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
		TokenSource: staticTokenSource{},
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			if req.Header.Get("Authorization") == "" {
				t.Fatal("missing authorization header")
			}
			if req.Method != http.MethodPut {
				t.Fatalf("unexpected method %s", req.Method)
			}
			return response(http.StatusNoContent, ""), nil
		})},
	}
	if err := client.Follow(context.Background(), "123"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("client was not called")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
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
