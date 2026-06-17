package broadcaster

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi/v2/xal/xasd"
	"github.com/df-mc/go-xsapi/v2/xal/xasu"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"golang.org/x/oauth2"
)

type staticTokenSource struct {
	xuid string
}

func (s staticTokenSource) XSTSToken(context.Context, string) (*xsts.Token, error) {
	xuid := s.xuid
	if xuid == "" {
		xuid = "1"
	}
	return &xsts.Token{
		Token:    "token",
		NotAfter: time.Now().Add(time.Hour),
		DisplayClaims: xsts.DisplayClaims{UserInfo: []xsts.UserInfo{{
			UserInfo: xasu.UserInfo{UserHash: "user"},
			XUID:     xuid,
			GamerTag: "Tester",
		}}},
	}, nil
}

func (staticTokenSource) DeviceToken(context.Context) (*xasd.Token, error) {
	return &xasd.Token{
		Token:    "device",
		NotAfter: time.Now().Add(time.Hour),
		DisplayClaims: xasd.DisplayClaims{DeviceInfo: xasd.DeviceInfo{
			DeviceID: "device",
		}},
	}, nil
}

func (staticTokenSource) ProofKey() *ecdsa.PrivateKey {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return key
}

type staticMinecraftTokenSource struct{}

func (staticMinecraftTokenSource) ServiceToken(context.Context) (*service.Token, error) {
	return &service.Token{AuthorizationHeader: "Bearer minecraft", ValidUntil: time.Now().Add(time.Hour)}, nil
}

type contextCapturingTokenSource struct {
	ctx context.Context
}

func (s *contextCapturingTokenSource) XSTSToken(ctx context.Context, _ string) (*xsts.Token, error) {
	s.ctx = ctx
	return nil, errors.New("stop before network")
}

func (*contextCapturingTokenSource) DeviceToken(context.Context) (*xasd.Token, error) {
	return nil, errors.New("unexpected device token request")
}

func (*contextCapturingTokenSource) ProofKey() *ecdsa.PrivateKey { return nil }

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

func TestUploadGalleryUsesBroadcasterContextForLazyMinecraftTokenSource(t *testing.T) {
	type contextKey struct{}
	const want = "broadcaster"

	src := &contextCapturingTokenSource{}
	broadcasterCtx := context.WithValue(context.Background(), contextKey{}, want)
	galleryCtx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "gallery"))
	cancel()

	b := &Broadcaster{
		ctx: broadcasterCtx,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		conf: Config{
			XBLTokenSource: src,
			Gallery: &GalleryConfig{
				Enabled:   true,
				ImagePath: testGalleryImageFile(t),
			},
		},
	}
	b.uploadGallery(galleryCtx)

	if src.ctx == nil {
		t.Fatal("xbox token source was not called")
	}
	if src.ctx.Err() != nil {
		t.Fatalf("lazy token source used expired context: %v", src.ctx.Err())
	}
	if got := src.ctx.Value(contextKey{}); got != want {
		t.Fatalf("lazy token source context value = %v, want %q", got, want)
	}
}

func TestNewXBLTokenSourceCachesDeviceAndXSTSTokens(t *testing.T) {
	var deviceRequests int
	var sisuRequests int
	validUntil := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case "https://device.auth.xboxlive.com/device/authenticate":
			deviceRequests++
			return tokenTestResponse(http.StatusOK, `{"IssueInstant":"`+validUntil+`","NotAfter":"`+validUntil+`","Token":"device","DisplayClaims":{"xdi":{"did":"device"}}}`), nil
		case "https://sisu.xboxlive.com/authorize":
			sisuRequests++
			return tokenTestResponse(http.StatusOK, `{"TitleToken":{"IssueInstant":"`+validUntil+`","NotAfter":"`+validUntil+`","Token":"title","DisplayClaims":{"xti":{"tid":"1739947436"}}},"UserToken":{"IssueInstant":"`+validUntil+`","NotAfter":"`+validUntil+`","Token":"user","DisplayClaims":{"xui":[{"uhs":"user"}]}},"AuthorizationToken":{"IssueInstant":"`+validUntil+`","NotAfter":"`+validUntil+`","Token":"xsts","DisplayClaims":{"xui":[{"gtg":"Tester","xid":"1","uhs":"user"}]}}}`), nil
		default:
			t.Fatalf("unexpected token request %s %s", req.Method, req.URL)
		}
		return nil, nil
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	src := NewXBLTokenSource(ctx, oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "live",
		Expiry:      time.Now().Add(time.Hour),
	}))

	for i := 0; i < 2; i++ {
		tok, err := src.XSTSToken(context.Background(), "http://xboxlive.com")
		if err != nil {
			t.Fatal(err)
		}
		if got := tok.UserInfo().XUID; got != "1" {
			t.Fatalf("xuid = %q, want 1", got)
		}
	}
	if deviceRequests > 1 {
		t.Fatalf("device auth requests = %d, want at most 1", deviceRequests)
	}
	if sisuRequests != 1 {
		t.Fatalf("sisu auth requests = %d, want 1", sisuRequests)
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
