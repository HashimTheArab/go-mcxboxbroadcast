package broadcaster

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi/v2/presence"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

func TestPresenceClientUpdatePostsActiveStateAndReturnsHeartbeat(t *testing.T) {
	var called bool
	client := PresenceClient{
		Presence: presence.New(testAuthenticatedClient("XBL3.0 x=user;token", roundTripFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			if req.Method != http.MethodPost {
				t.Fatalf("unexpected method %s", req.Method)
			}
			if req.URL.String() != "https://userpresence.xboxlive.com/users/xuid(1)/devices/current/titles/current" {
				t.Fatalf("unexpected URL %s", req.URL)
			}
			if req.Header.Get("Authorization") != "XBL3.0 x=user;token" {
				t.Fatalf("unexpected authorization header %q", req.Header.Get("Authorization"))
			}
			if req.Header.Get("X-Xbl-Contract-Version") != "3" {
				t.Fatalf("unexpected contract version %q", req.Header.Get("X-Xbl-Contract-Version"))
			}
			if req.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("unexpected content type %q", req.Header.Get("Content-Type"))
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				State string `json:"state"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("decode body %q: %v", string(body), err)
			}
			if payload.State != "active" {
				t.Fatalf("unexpected body state %q", payload.State)
			}
			resp := response(http.StatusOK, "")
			resp.Header.Set("X-Heartbeat-After", "42")
			return resp, nil
		})), xsts.UserInfo{XUID: "1"}),
	}

	heartbeat, err := client.Update(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("client was not called")
	}
	if heartbeat != 42*time.Second {
		t.Fatalf("unexpected heartbeat %s", heartbeat)
	}
}

func TestPresenceClientUpdateDefaultsHeartbeatWhenHeaderInvalid(t *testing.T) {
	client := PresenceClient{
		Presence: presence.New(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			resp := response(http.StatusOK, "")
			resp.Header.Set("X-Heartbeat-After", "bad")
			return resp, nil
		})}, xsts.UserInfo{XUID: "1"}),
	}

	heartbeat, err := client.Update(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if heartbeat != defaultPresenceHeartbeat {
		t.Fatalf("unexpected heartbeat %s", heartbeat)
	}
}

func TestPresenceClientUpdateBoundsRequestContext(t *testing.T) {
	client := PresenceClient{
		Presence: presence.New(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			deadline, ok := req.Context().Deadline()
			if !ok {
				t.Fatal("presence request has no deadline")
			}
			remaining := time.Until(deadline)
			if remaining <= 0 || remaining > 20*time.Second {
				t.Fatalf("presence request deadline remaining = %s", remaining)
			}
			return response(http.StatusOK, ""), nil
		})}, xsts.UserInfo{XUID: "1"}),
	}
	if _, err := client.Update(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPresenceClientUpdateRejectsNilClient(t *testing.T) {
	client := PresenceClient{}

	_, err := client.Update(context.Background())
	if err == nil || !strings.Contains(err.Error(), "presence client is nil") {
		t.Fatalf("expected nil client error, got %v", err)
	}
}

func TestBroadcasterPresenceClientsIncludeEnabledSubAccounts(t *testing.T) {
	httpClient := &http.Client{}
	primary := newTestXSAPIClient(t, httpClient, "primary")
	enabled := newTestXSAPIClient(t, httpClient, "enabled")
	b := &Broadcaster{conf: Config{
		XUID:       "primary",
		HTTPClient: httpClient,
		XBLClient:  primary,
		SubAccounts: []SubAccountConfig{
			{ID: "enabled", Enabled: true, XBLClient: enabled, XUID: "enabled"},
			{ID: "failed-lazy-client", Enabled: true, XUID: "failed-lazy-client", XBLTokenSource: staticTokenSource{}},
			{ID: "disabled", Enabled: false, XUID: "disabled"},
			{ID: "missing-token", Enabled: true},
		},
	}}

	clients := b.presenceClients()
	if len(clients) != 2 {
		t.Fatalf("expected primary and enabled sub-account presence clients, got %d", len(clients))
	}
	if clients[1].XUID != "enabled" {
		t.Fatalf("expected credentialed sub-account presence, got xuid %q", clients[1].XUID)
	}
	if clients[0].Presence != primary.Presence() || clients[1].Presence != enabled.Presence() {
		t.Fatal("presence clients did not use xsapi-owned presence subclients")
	}
}
