package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	xblsocial "github.com/df-mc/go-xsapi/v2/social"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

// newTestSocialClient creates a social client using only the test transport.
func newTestSocialClient(client *http.Client) *xblsocial.Client {
	return xblsocial.New(client, nil, xsts.UserInfo{}, nil)
}

func TestBroadcasterFriendClientUsesXSAPIOwnedSocialClient(t *testing.T) {
	xbl := newTestXSAPIClient(t, &http.Client{}, "primary")
	b := &Broadcaster{conf: Config{XBLClient: xbl, HTTPClient: &http.Client{}}}

	client := b.friendClient()
	if client.Social == nil || client.Social != xbl.Social() {
		t.Fatal("friend client did not use xsapi-owned social subclient")
	}
}

// Endpoint fixtures pinning the exact request URLs the social layer must hit
// for MCXboxBroadcast parity.
const (
	peopleHubFriendsURL = "https://peoplehub.xboxlive.com/users/me/people/friends"
	pendingRequestsURL  = "https://peoplehub.xboxlive.com/users/me/people/friendRequests(received)"
	addFriendsURL       = "https://social.xboxlive.com/bulk/users/me/people/friends/v2?method=add"
)

func TestFriendClientAcceptPendingFriendRequestsUsesAddFriends(t *testing.T) {
	var requests []string
	client := FriendClient{Social: newTestSocialClient(
		testAuthenticatedClient("XBL3.0 x=user;token", roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.Method+" "+req.URL.String())
			if req.Header.Get("Authorization") == "" {
				t.Fatal("missing authorization header")
			}
			switch {
			case req.Method == http.MethodGet && req.URL.String() == pendingRequestsURL:
				if req.Header.Get("X-Xbl-Contract-Version") != "7" {
					t.Fatalf("contract version = %q, want 7", req.Header.Get("X-Xbl-Contract-Version"))
				}
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case req.Method == http.MethodPost && req.URL.String() == addFriendsURL:
				var body struct {
					XUIDs []string `json:"xuids"`
				}
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if got, want := strings.Join(body.XUIDs, ","), "1,2"; got != want {
					t.Fatalf("add-friends XUIDs = %s, want %s", got, want)
				}
				return response(http.StatusOK, `{"updatedPeople":["1","2"]}`), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})))}

	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRequests := strings.Join([]string{
		http.MethodGet + " " + pendingRequestsURL,
		http.MethodPost + " " + addFriendsURL,
	}, ",")
	if got := strings.Join(requests, ","); got != wantRequests {
		t.Fatalf("requests = %s, want %s", got, wantRequests)
	}
	if len(accepted) != 2 || accepted[0].XUID != "1" || accepted[1].XUID != "2" {
		t.Fatalf("accepted people = %#v", accepted)
	}
}

func TestFriendClientAcceptPendingFriendRequestsBatchesAdds(t *testing.T) {
	pending := make([]map[string]string, 51)
	for i := range pending {
		pending[i] = map[string]string{"xuid": fmt.Sprint(i + 1)}
	}
	pendingBody, err := json.Marshal(map[string]any{"people": pending})
	if err != nil {
		t.Fatal(err)
	}

	var batchSizes []int
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, string(pendingBody)), nil
			case http.MethodPost:
				var body struct {
					XUIDs []string `json:"xuids"`
				}
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				batchSizes = append(batchSizes, len(body.XUIDs))
				responseBody, err := json.Marshal(map[string]any{"updatedPeople": body.XUIDs})
				if err != nil {
					t.Fatal(err)
				}
				return response(http.StatusOK, string(responseBody)), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})})}

	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != len(pending) {
		t.Fatalf("accepted %d people, want %d", len(accepted), len(pending))
	}
	if got := fmt.Sprint(batchSizes); got != "[50 1]" {
		t.Fatalf("bulk batch sizes = %s, want [50 1]", got)
	}
}

func TestFriendClientAcceptPendingFriendRequestsSplitsLimitErrors(t *testing.T) {
	var batchSizes []int
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1"},{"xuid":"2"},{"xuid":"3"},{"xuid":"4"}]}`), nil
			case http.MethodPost:
				var body struct {
					XUIDs []string `json:"xuids"`
				}
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				batchSizes = append(batchSizes, len(body.XUIDs))
				if len(body.XUIDs) > 2 {
					return response(http.StatusBadRequest, `{"code":1050,"description":"Bulk Operation exceeded our limits, retry with a lower count."}`), nil
				}
				responseBody, err := json.Marshal(map[string]any{"updatedPeople": body.XUIDs})
				if err != nil {
					t.Fatal(err)
				}
				return response(http.StatusOK, string(responseBody)), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})})}

	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(accepted) != 4 {
		t.Fatalf("accepted people = %#v, want all four", accepted)
	}
	if got := fmt.Sprint(batchSizes); got != "[4 2 2]" {
		t.Fatalf("bulk batch sizes = %s, want [4 2 2]", got)
	}
}

func TestFriendClientAcceptPendingFriendRequestsReturnsRetryAfterError(t *testing.T) {
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
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
		})})}

	_, err := client.AcceptPendingFriendRequests(context.Background())
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
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One"},{"xuid":"2","gamertag":"Two"}]}`), nil
			case http.MethodPost:
				return response(http.StatusOK, `{"updatedPeople":["1"]}`), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})})}

	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err == nil {
		t.Fatal("expected failed updates error")
	}
	if len(accepted) != 1 || accepted[0].XUID != "1" {
		t.Fatalf("accepted people = %#v, want xuid 1", accepted)
	}
	var acceptErr *AcceptFriendRequestsError
	if !errors.As(err, &acceptErr) {
		t.Fatalf("expected accept failure error, got %T: %v", err, err)
	}
	if len(acceptErr.Failed) != 1 || acceptErr.Failed[0].XUID != "2" || acceptErr.Failed[0].Gamertag != "Two" {
		t.Fatalf("failed people = %#v, want Two", acceptErr.Failed)
	}
}

// Friends reads only the friend list, undecorated with the default contract, like MCXboxBroadcast.
func TestFriendClientFriendsReadsFriendList(t *testing.T) {
	var requests []string
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.URL.String())
			if req.Header.Get("X-Xbl-Contract-Version") != "7" {
				t.Fatalf("contract version = %q, want 7", req.Header.Get("X-Xbl-Contract-Version"))
			}
			return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"One","displayName":"Display","isFriend":true},{"xuid":""}]}`), nil
		})})}

	people, err := client.Friends(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(requests, ","); got != peopleHubFriendsURL {
		t.Fatalf("requests = %s, want %s", got, peopleHubFriendsURL)
	}
	if len(people) != 1 || people[0] != (Person{XUID: "1", Gamertag: "One"}) {
		t.Fatalf("people = %#v", people)
	}
}

func TestFriendClientAddAndRemoveUseFriendsEndpoint(t *testing.T) {
	var requests []string
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.Method+" "+req.URL.String())
			return response(http.StatusOK, ""), nil
		})})}
	if err := client.AddFriend(context.Background(), "5"); err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveFriend(context.Background(), "5"); err != nil {
		t.Fatal(err)
	}
	want := "PUT https://social.xboxlive.com/users/me/people/friends/v2/xuid(5)," +
		"DELETE https://social.xboxlive.com/users/me/people/friends/v2/xuid(5)?deleteRelationships=friends"
	if got := strings.Join(requests, ","); got != want {
		t.Fatalf("requests = %s, want %s", got, want)
	}
}

func TestFriendClientAcceptPendingFriendRequestsSkipsGuests(t *testing.T) {
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodGet {
				t.Fatalf("guest request accepted: %s %s", req.Method, req.URL)
			}
			return response(http.StatusOK, `{"people":[{"xuid":"4503599627370497"}]}`), nil
		})})}
	accepted, err := client.AcceptPendingFriendRequests(context.Background())
	if err != nil || len(accepted) != 0 {
		t.Fatalf("accepted = %#v, err = %v", accepted, err)
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
