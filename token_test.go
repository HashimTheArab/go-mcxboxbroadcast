package broadcaster

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"golang.org/x/oauth2"
)

type staticTokenSource struct{}

func (staticTokenSource) Token() (xsapi.Token, error) {
	return staticToken{}, nil
}

type staticToken struct{}

func (staticToken) SetAuthHeader(req *http.Request) {
	req.Header.Set("Authorization", "XBL3.0 x=user;token")
}

func (staticToken) String() string { return "XBL3.0 x=user;token" }

func (staticToken) DisplayClaims() xsapi.DisplayClaims {
	return xsapi.DisplayClaims{GamerTag: "Tester", XUID: "1", UserHash: "user"}
}

type staticMinecraftTokenSource struct{}

func (staticMinecraftTokenSource) Token() (*service.Token, error) {
	return &service.Token{AuthorizationHeader: "Bearer minecraft", ValidUntil: time.Now().Add(time.Hour)}, nil
}

func TestNewLiveTokenSourceUsesContextHTTPClientForRefresh(t *testing.T) {
	var called bool
	var client *http.Client
	client = &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		called = true
		if req.URL.String() != "https://login.live.com/oauth20_token.srf" {
			t.Fatalf("unexpected refresh URL %q", req.URL.String())
		}
		if got, _ := req.Context().Value(oauth2.HTTPClient).(*http.Client); got != client {
			t.Fatal("refresh request did not retain context HTTP client")
		}
		return tokenTestResponse(http.StatusOK, `{"access_token":"new-access","token_type":"bearer","refresh_token":"new-refresh","expires_in":3600}`), nil
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	src := NewLiveTokenSource(ctx, &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		Expiry:       time.Now().Add(-time.Hour),
	}, io.Discard)

	tok, err := src.Token()
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected refresh request through context HTTP client")
	}
	if tok.AccessToken != "new-access" {
		t.Fatalf("unexpected access token %q", tok.AccessToken)
	}
	if tok.RefreshToken != "new-refresh" {
		t.Fatalf("unexpected refresh token %q", tok.RefreshToken)
	}
}

type tokenRoundTripFunc func(*http.Request) (*http.Response, error)

func (f tokenRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func tokenTestResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func testImageFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "screenshot.jpg")
	if err := os.WriteFile(path, []byte("fake image bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
