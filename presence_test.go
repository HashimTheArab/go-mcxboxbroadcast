package broadcaster

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi"
)

func TestPresenceClientUpdatePostsActiveStateAndReturnsHeartbeat(t *testing.T) {
	var called bool
	client := PresenceClient{
		TokenSource: staticTokenSource{},
		Client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
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
			if string(body) != `{"state":"active"}` {
				t.Fatalf("unexpected body %q", string(body))
			}
			resp := response(http.StatusOK, "")
			resp.Header.Set("X-Heartbeat-After", "42")
			return resp, nil
		})},
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
		TokenSource: staticTokenSource{},
		Client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			resp := response(http.StatusOK, "")
			resp.Header.Set("X-Heartbeat-After", "bad")
			return resp, nil
		})},
	}

	heartbeat, err := client.Update(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if heartbeat != defaultPresenceHeartbeat {
		t.Fatalf("unexpected heartbeat %s", heartbeat)
	}
}

func TestPresenceClientUpdateRejectsEmptyXUID(t *testing.T) {
	client := PresenceClient{TokenSource: emptyXUIDTokenSource{}}

	_, err := client.Update(context.Background())
	if err == nil || !strings.Contains(err.Error(), "xuid is empty") {
		t.Fatalf("expected empty xuid error, got %v", err)
	}
}

func TestBroadcasterPresenceClientsIncludeEnabledSubAccounts(t *testing.T) {
	httpClient := &http.Client{}
	b := &Broadcaster{conf: Config{
		TokenSource: staticTokenSource{},
		HTTPClient:  httpClient,
		SubAccounts: []SubAccountConfig{
			{ID: "enabled", Enabled: true, TokenSource: staticTokenSource{}},
			{ID: "disabled", Enabled: false, TokenSource: staticTokenSource{}},
			{ID: "missing-token", Enabled: true},
		},
	}}

	clients := b.presenceClients()
	if len(clients) != 2 {
		t.Fatalf("expected primary and one enabled sub-account presence client, got %d", len(clients))
	}
	for _, client := range clients {
		if client.Client != httpClient {
			t.Fatal("presence client did not use configured HTTP client")
		}
	}
}

type emptyXUIDTokenSource struct{}

func (emptyXUIDTokenSource) Token() (xsapi.Token, error) {
	return emptyXUIDToken{}, nil
}

type emptyXUIDToken struct {
	staticToken
}

func (emptyXUIDToken) DisplayClaims() xsapi.DisplayClaims {
	return xsapi.DisplayClaims{}
}
