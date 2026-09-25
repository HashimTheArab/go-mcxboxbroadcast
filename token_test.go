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
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-xsapi/v2/xal/xasd"
	"github.com/df-mc/go-xsapi/v2/xal/xasu"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/sandertv/gophertunnel/minecraft/auth"
	"github.com/sandertv/gophertunnel/minecraft/auth/authclient"
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

type failingMinecraftTokenSource struct {
	err error
}

func (s failingMinecraftTokenSource) ServiceToken(context.Context) (*service.Token, error) {
	return nil, s.err
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

type blockingTokenSource struct {
	started chan<- struct{}
	unblock <-chan struct{}
}

func (s blockingTokenSource) XSTSToken(ctx context.Context, _ string) (*xsts.Token, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.unblock
	return nil, ctx.Err()
}

func (blockingTokenSource) DeviceToken(context.Context) (*xasd.Token, error) {
	return nil, errors.New("unexpected device token request")
}

func (blockingTokenSource) ProofKey() *ecdsa.PrivateKey { return nil }

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

func TestLiveTokenSourcePersistsRotatedRefreshTokens(t *testing.T) {
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return tokenTestResponse(http.StatusOK, `{"access_token":"new-access","token_type":"bearer","refresh_token":"rotated-refresh","expires_in":3600}`), nil
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	var persisted []*oauth2.Token
	src := NewLiveTokenSourceWithPersist(ctx, &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		Expiry:       time.Now().Add(-time.Hour),
	}, io.Discard, func(tok *oauth2.Token) {
		persisted = append(persisted, tok)
	})

	if _, err := src.Token(); err != nil {
		t.Fatal(err)
	}
	if len(persisted) != 1 || persisted[0].RefreshToken != "rotated-refresh" {
		t.Fatalf("persisted tokens = %#v, want one with rotated-refresh", persisted)
	}
}

func TestLiveTokenSourceDoesNotFallBackToDeviceCodeOnTransientRefreshError(t *testing.T) {
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	src := newLiveTokenSource(ctx, &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		Expiry:       time.Now().Add(-time.Hour),
	}, io.Discard, nil)
	src.retry = fastLiveRetry

	if _, err := src.Token(); err == nil {
		t.Fatal("expected transient refresh error to surface")
	}
}

func TestLiveTokenSourceDoesNotFallBackToDeviceCodeOnTransientServerError(t *testing.T) {
	// A non-200 with an OAuth error other than invalid_grant (or none at all)
	// means the refresh token may still be usable; no re-login should start.
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		return tokenTestResponse(http.StatusServiceUnavailable, `{"error":"server_error","error_description":"try again"}`), nil
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	src := newLiveTokenSource(ctx, &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		Expiry:       time.Now().Add(-time.Hour),
	}, io.Discard, nil)
	src.retry = fastLiveRetry

	_, err := src.Token()
	var refreshErr *liveRefreshError
	if !errors.As(err, &refreshErr) || refreshErr.Code != "server_error" {
		t.Fatalf("Token() error = %v, want surfaced refresh error", err)
	}
}

// A refresh response that omits refresh_token must keep the previous one, not force a device-code login later.
func TestLiveTokenSourceKeepsRefreshTokenWhenResponseOmitsIt(t *testing.T) {
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != "https://login.live.com/oauth20_token.srf" {
			t.Errorf("unexpected request %s", req.URL)
			return nil, errors.New("unexpected request")
		}
		return tokenTestResponse(http.StatusOK, `{"access_token":"new-access","token_type":"bearer","expires_in":3600}`), nil
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	var persisted *oauth2.Token
	src := newLiveTokenSource(ctx, &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		Expiry:       time.Now().Add(-time.Hour),
	}, io.Discard, func(tok *oauth2.Token) { persisted = tok })

	tok, err := src.Token()
	if err != nil {
		t.Fatal(err)
	}
	if tok.RefreshToken != "old-refresh" || persisted == nil || persisted.RefreshToken != "old-refresh" {
		t.Fatalf("refresh token = %q, persisted %v; want old-refresh kept", tok.RefreshToken, persisted)
	}
}

// A refresh token rejected after startup must not hold callers for the whole device-code login.
func TestLiveTokenSourceRuntimeReauthRunsInBackground(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var approved atomic.Bool
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, deviceLoginTestClient(t, &approved))
		var out strings.Builder
		var persisted atomic.Pointer[oauth2.Token]
		src := newLiveTokenSource(ctx, &oauth2.Token{
			AccessToken:  "old-access",
			RefreshToken: "revoked",
			Expiry:       time.Now().Add(-time.Hour),
		}, &out, func(tok *oauth2.Token) { persisted.Store(tok) })
		src.issued = true

		for range 2 {
			if _, err := src.Token(); !errors.Is(err, errLiveReauthPending) {
				t.Fatalf("Token() error = %v, want pending re-authentication", err)
			}
		}
		approved.Store(true)
		time.Sleep(5 * time.Second)
		synctest.Wait()

		tok, err := src.Token()
		if err != nil {
			t.Fatal(err)
		}
		if tok.AccessToken != "fresh" || persisted.Load() == nil || persisted.Load().AccessToken != "fresh" {
			t.Fatalf("token = %q, persisted %v; want fresh token stored", tok.AccessToken, persisted.Load())
		}
		if !strings.Contains(out.String(), "Microsoft sign-in expired") {
			t.Fatalf("sign-in output = %q, want expiry notice", out.String())
		}
		drainTokenTestTimers()
	})
}

// An abandoned runtime sign-in must end at its deadline and report the failure.
func TestLiveTokenSourceRuntimeReauthTimesOut(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, deviceLoginTestClient(t, new(atomic.Bool)))
		var out strings.Builder
		src := newLiveTokenSource(ctx, &oauth2.Token{
			AccessToken:  "old-access",
			RefreshToken: "revoked",
			Expiry:       time.Now().Add(-time.Hour),
		}, &out, nil)
		src.issued = true
		src.reauthTimeout = time.Minute

		if _, err := src.Token(); !errors.Is(err, errLiveReauthPending) {
			t.Fatalf("Token() error = %v, want pending re-authentication", err)
		}
		time.Sleep(2 * time.Minute)
		synctest.Wait()

		if _, err := src.Token(); err == nil || errors.Is(err, errLiveReauthPending) {
			t.Fatalf("Token() error = %v, want the failed sign-in", err)
		}
		if !strings.Contains(out.String(), "Microsoft sign-in failed") {
			t.Fatalf("sign-in output = %q, want failure notice", out.String())
		}
		// The next call starts a fresh sign-in.
		if _, err := src.Token(); !errors.Is(err, errLiveReauthPending) {
			t.Fatalf("Token() error = %v, want a new pending re-authentication", err)
		}
		drainTokenTestTimers()
	})
}

// drainTokenTestTimers lets pending logins and HTTP client timers expire so
// the synctest bubble can exit.
func drainTokenTestTimers() {
	time.Sleep(time.Hour)
	synctest.Wait()
}

// deviceLoginTestClient rejects refresh tokens and completes device-code
// polling once approved is set.
func deviceLoginTestClient(t *testing.T, approved *atomic.Bool) *http.Client {
	return &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case "https://login.live.com/oauth20_connect.srf":
			return tokenTestJSONResponse(http.StatusOK, `{"device_code":"device","user_code":"code","verification_uri":"https://www.microsoft.com/link","expires_in":900,"interval":1}`), nil
		case "https://login.live.com/oauth20_token.srf":
			if err := req.ParseForm(); err != nil {
				return nil, err
			}
			if req.Form.Get("grant_type") == "refresh_token" {
				return tokenTestResponse(http.StatusBadRequest, `{"error":"invalid_grant","error_description":"revoked"}`), nil
			}
			if !approved.Load() {
				return tokenTestJSONResponse(http.StatusBadRequest, `{"error":"authorization_pending"}`), nil
			}
			return tokenTestJSONResponse(http.StatusOK, `{"access_token":"fresh","token_type":"bearer","refresh_token":"fresh-refresh","expires_in":3600}`), nil
		default:
			t.Errorf("unexpected request %s %s", req.Method, req.URL)
			return nil, errors.New("unexpected request")
		}
	})}
}

func TestLiveTokenSourceFallsBackToDeviceCodeWhenRefreshRejected(t *testing.T) {
	// The device-code flow starts with a POST to the device-code endpoint; the
	// fallback is detected by observing that request after a refresh rejection.
	var urls []string
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		urls = append(urls, req.URL.String())
		if req.URL.String() == "https://login.live.com/oauth20_token.srf" {
			return tokenTestResponse(http.StatusBadRequest, `{"error":"invalid_grant","error_description":"expired"}`), nil
		}
		return nil, errors.New("stop device-code flow")
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)
	src := NewLiveTokenSource(ctx, &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "old-refresh",
		Expiry:       time.Now().Add(-time.Hour),
	}, io.Discard)

	_, err := src.Token()
	if err == nil {
		t.Fatal("expected error from aborted device-code flow")
	}
	if len(urls) < 2 {
		t.Fatalf("requests = %v, want refresh followed by device-code fallback", urls)
	}
	var refreshErr *liveRefreshError
	if errors.As(err, &refreshErr) {
		t.Fatalf("refresh rejection should have been superseded by device-code fallback, got %v", err)
	}
}

func TestRequestLiveTokenWriterSuggestsPasswordResetForInvalidGrant(t *testing.T) {
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case "https://login.live.com/oauth20_connect.srf":
			return tokenTestJSONResponse(http.StatusOK, `{"device_code":"device","user_code":"code","verification_uri":"https://www.microsoft.com/link","expires_in":900,"interval":1}`), nil
		case "https://login.live.com/oauth20_token.srf":
			return tokenTestJSONResponse(http.StatusBadRequest, `{"error":"invalid_grant","error_description":"user interaction is required"}`), nil
		default:
			t.Fatalf("unexpected device auth request %s %s", req.Method, req.URL)
			return nil, nil
		}
	})}
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, client)

	_, err := requestLiveTokenWriter(ctx, auth.AndroidConfig, io.Discard)
	if err == nil {
		t.Fatal("expected device authentication error")
	}
	if !strings.Contains(err.Error(), "set or reset the Microsoft account password") {
		t.Fatalf("error = %q, want password-reset guidance", err)
	}
	var retrieveErr *oauth2.RetrieveError
	if !errors.As(err, &retrieveErr) || retrieveErr.ErrorCode != "invalid_grant" {
		t.Fatalf("error = %v, want wrapped invalid_grant retrieve error", err)
	}
}

func TestMinecraftTokenDiagnosticsFormatsPlayerBannedError(t *testing.T) {
	baseErr := errors.New(`minecraft/service: PlayerBanned: "Player 2535433454914320 is banned." ()`)
	src := withMinecraftTokenDiagnostics(failingMinecraftTokenSource{err: baseErr})

	_, err := src.ServiceToken(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, baseErr) {
		t.Fatalf("expected wrapped base error, got %v", err)
	}
	want := "failed to get MC token header: error: PlayerBanned, error message: Player 2535433454914320 is banned."
	if err.Error() != want {
		t.Fatalf("error = %q, want %q", err.Error(), want)
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
		case "https://title.mgt.xboxlive.com/titles/default/endpoints?type=1":
			return tokenTestResponse(http.StatusOK, `{"EndPoints":[{"Protocol":"https","Host":"*.xboxlive.com","HostType":"wildcard","RelyingParty":"http://xboxlive.com","TokenType":"JWT"}]}`), nil
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
	ctx := auth.WithContextClient(context.Background(), client)
	src := NewXBLTokenSource(ctx, oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: "live",
		Expiry:      time.Now().Add(time.Hour),
	}))

	for i := 0; i < 2; i++ {
		tok, err := src.XSTSToken(ctx, "http://xboxlive.com")
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

func TestNewXSAPIClientUsesLazyRTA(t *testing.T) {
	client := &http.Client{Transport: tokenRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("unexpected HTTP request during xsapi client construction: %s %s", req.Method, req.URL)
		return nil, nil
	})}

	xbl, err := NewXSAPIClient(context.Background(), staticTokenSource{}, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if xbl.RTA() != nil {
		t.Fatal("xsapi client opened RTA during construction")
	}
}

// fastLiveRetry keeps the refresh retry budget but not its real backoff.
var fastLiveRetry = authclient.RetryOptions{Attempts: 5, MinDelay: time.Millisecond, MaxDelay: time.Millisecond}

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

func tokenTestJSONResponse(code int, body string) *http.Response {
	resp := tokenTestResponse(code, body)
	resp.Header.Set("Content-Type", "application/json")
	return resp
}
