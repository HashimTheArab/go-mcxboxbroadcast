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

	client := b.friendClientFor(xbl)
	if client.Social == nil || client.Social != xbl.Social() {
		t.Fatal("friend client did not use xsapi-owned social subclient")
	}
}

// Endpoint fixtures pinning the exact request URLs the social layer must hit
// for MCXboxBroadcast parity.
const (
	peopleHubFollowersURL = "https://peoplehub.xboxlive.com/users/me/people/followers"
	peopleHubSocialURL    = "https://peoplehub.xboxlive.com/users/me/people/social"
	pendingRequestsURL    = "https://peoplehub.xboxlive.com/users/me/people/friendRequests(received)"
	addFriendsURL         = "https://social.xboxlive.com/bulk/users/me/people/friends/v2?method=add"
)

func followURL(xuid string) string {
	return fmt.Sprintf("https://social.xboxlive.com/users/me/people/xuid(%s)", xuid)
}

func unfollowURL(xuid string) string {
	return fmt.Sprintf("https://social.xboxlive.com/users/me/people/xuid(%s)", xuid)
}

func TestFriendClientFriendsMergesFollowersAndSocial(t *testing.T) {
	var requests []string
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requests = append(requests, req.URL.String())
			switch req.URL.String() {
			case peopleHubFollowersURL:

				if req.Header.Get("X-Xbl-Contract-Version") != "5" {
					t.Fatalf("contract version = %q, want 5", req.Header.Get("X-Xbl-Contract-Version"))
				}
				return response(http.StatusOK, `{"people":[{"xuid":"1","gamertag":"Follower","displayName":"Display","modernGamertag":"Modern","uniqueModernGamertag":"Modern#1234","isFollowingCaller":true}]}`), nil
			case peopleHubSocialURL:
				return response(http.StatusOK, `{"people":[{"xuid":"1","isFollowedByCaller":true},{"xuid":"2","gamertag":"Followed","isFollowedByCaller":true}]}`), nil
			default:
				t.Fatalf("unexpected URL %s", req.URL)
			}
			return nil, nil
		})})}

	people, err := client.Friends(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRequests := strings.Join([]string{peopleHubFollowersURL, peopleHubSocialURL}, ",")
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
	client := FriendClient{Social: newTestSocialClient(
		testAuthenticatedClient("XBL3.0 x=user;token", roundTripFunc(func(req *http.Request) (*http.Response, error) {
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
		})))}

	if err := client.Follow(context.Background(), "123"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("client was not called")
	}
}

func TestFriendClientFollowReturnsRetryAfterError(t *testing.T) {
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			resp := response(http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "7")
			return resp, nil
		})})}

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
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method != http.MethodDelete {
				t.Fatalf("unexpected method %s", req.Method)
			}
			if req.URL.String() != unfollowURL("123") {
				t.Fatalf("unexpected URL %s", req.URL)
			}
			resp := response(http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "3")
			return resp, nil
		})})}

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
			client := FriendClient{Social: newTestSocialClient(
				&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					return response(http.StatusBadRequest, tt.body), nil
				})})}

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

	result, err := client.AcceptPendingFriendRequests(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	accepted := result.Accepted
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

	result, err := client.AcceptPendingFriendRequests(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if accepted := result.Accepted; len(accepted) != len(pending) {
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

	result, err := client.AcceptPendingFriendRequests(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Accepted) != 4 {
		t.Fatalf("accepted people = %#v, want all four", result.Accepted)
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

	_, err := client.AcceptPendingFriendRequests(context.Background(), nil)
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
				return response(http.StatusOK, `{"updatedPeople":["1"],"failedToUpdate":["2"]}`), nil
			default:
				t.Fatalf("unexpected request %s %s", req.Method, req.URL)
			}
			return nil, nil
		})})}

	result, err := client.AcceptPendingFriendRequests(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Accepted) != 1 || result.Accepted[0].XUID != "1" {
		t.Fatalf("accepted people = %#v, want xuid 1", result.Accepted)
	}
	if len(result.Rejected) != 1 || result.Rejected[0].Person.XUID != "2" || !errors.Is(result.Rejected[0].Err, ErrFriendRequestNotAccepted) {
		t.Fatalf("rejected = %#v, want xuid 2 not accepted", result.Rejected)
	}
	if result.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", result.Waiting)
	}
}

// bulkAddServer answers pending-list reads and bulk adds from a callback, recording each batch.
func bulkAddServer(t *testing.T, pending []string, add func(xuids []string) *http.Response) (FriendClient, *[][]string) {
	t.Helper()
	people := make([]map[string]string, 0, len(pending))
	for _, xuid := range pending {
		people = append(people, map[string]string{"xuid": xuid, "gamertag": "GT" + xuid})
	}
	pendingBody, err := json.Marshal(map[string]any{"people": people})
	if err != nil {
		t.Fatal(err)
	}
	var batches [][]string
	client := FriendClient{Social: newTestSocialClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var resp *http.Response
		switch req.Method {
		case http.MethodGet:
			resp = response(http.StatusOK, string(pendingBody))
		case http.MethodPost:
			var body struct {
				XUIDs []string `json:"xuids"`
			}
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			batches = append(batches, body.XUIDs)
			resp = add(body.XUIDs)
		default:
			t.Fatalf("unexpected request %s %s", req.Method, req.URL)
		}
		resp.Request = req
		return resp, nil
	})})}
	return client, &batches
}

func updated(xuids []string) *http.Response {
	body, _ := json.Marshal(map[string]any{"updatedPeople": xuids})
	return response(http.StatusOK, string(body))
}

// One refused request must not block the rest of its batch or later batches.
func TestFriendClientAcceptIsolatesRefusedRequests(t *testing.T) {
	xuids := make([]string, addFriendsBatchSize+2)
	for i := range xuids {
		xuids[i] = fmt.Sprint(i + 1)
	}
	client, batches := bulkAddServer(t, xuids, func(batch []string) *http.Response {
		for _, xuid := range batch {
			switch xuid {
			case "3":
				return response(http.StatusBadRequest, "Bad Request")
			case "4":
				return response(http.StatusForbidden, `{"code":1049,"description":"privacy"}`)
			}
		}
		return updated(batch)
	})

	result, err := client.AcceptPendingFriendRequests(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Accepted) != len(xuids)-2 {
		t.Fatalf("accepted %d people, want %d", len(result.Accepted), len(xuids)-2)
	}
	rejected := map[string]error{}
	for _, r := range result.Rejected {
		rejected[r.Person.XUID] = r.Err
	}
	if len(rejected) != 2 || !strings.Contains(fmt.Sprint(rejected["3"]), "Bad Request") || !errors.Is(rejected["4"], xblsocial.ErrFriendRestricted) {
		t.Fatalf("rejected = %v, want 3 with its body and 4 restricted", rejected)
	}
	if last := (*batches)[len(*batches)-1]; strings.Join(last, ",") != fmt.Sprintf("%d,%d", addFriendsBatchSize+1, addFriendsBatchSize+2) {
		t.Fatalf("last batch = %v, want the second page of requests", last)
	}
}

// A full friend list applies to every request, so it stops without splitting.
func TestFriendClientAcceptStopsWhenFriendListFull(t *testing.T) {
	client, batches := bulkAddServer(t, []string{"1", "2", "3"}, func([]string) *http.Response {
		return response(http.StatusBadRequest, `{"code":1028,"description":"full"}`)
	})
	result, err := client.AcceptPendingFriendRequests(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ListFull || result.Waiting != 3 || len(result.Rejected) != 0 {
		t.Fatalf("result = %+v, want list full with 3 waiting", result)
	}
	if len(*batches) != 1 {
		t.Fatalf("bulk requests = %d, want 1", len(*batches))
	}
}

func TestFriendClientAcceptSkipsRequests(t *testing.T) {
	client, batches := bulkAddServer(t, []string{"1", "2"}, updated)
	result, err := client.AcceptPendingFriendRequests(context.Background(), func(xuid string) bool { return xuid == "1" })
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(*batches) != "[[2]]" || result.Waiting != 1 {
		t.Fatalf("batches = %v waiting = %d, want [[2]] and 1", *batches, result.Waiting)
	}
}

func TestFriendClientRemoveFriendEndsFriendship(t *testing.T) {
	var requests []string
	client := FriendClient{Social: newTestSocialClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		requests = append(requests, req.Method+" "+req.URL.String())
		resp := response(http.StatusNotFound, "")
		resp.Request = req
		return resp, nil
	})})}
	// A 404 means no friendship was ended, so it must not pass as a freed slot.
	if err := client.RemoveFriend(context.Background(), "123"); !isNotFound(err) {
		t.Fatalf("RemoveFriend() error = %v, want the 404", err)
	}
	if want := "DELETE https://social.xboxlive.com/users/me/people/friends/v2/xuid(123)?deleteRelationships=friends"; strings.Join(requests, ",") != want {
		t.Fatalf("requests = %v, want %s", requests, want)
	}
}

func TestFriendClientRemoveFollowerDeletesFollowerRelationship(t *testing.T) {
	called := false
	client := FriendClient{Social: newTestSocialClient(
		&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			if req.Method != http.MethodDelete {
				t.Fatalf("unexpected method %s", req.Method)
			}
			if want := "https://social.xboxlive.com/users/me/people/follower/xuid(123)"; req.URL.String() != want {
				t.Fatalf("unexpected URL %s, want %s", req.URL, want)
			}
			return response(http.StatusNoContent, ""), nil
		})})}

	if err := client.RemoveFollower(context.Background(), "123"); err != nil {
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
