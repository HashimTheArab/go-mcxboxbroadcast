package broadcaster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/df-mc/go-playfab/v2"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/sandertv/gophertunnel/minecraft/auth"
	"github.com/sandertv/gophertunnel/minecraft/auth/authclient"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/microsoft"
)

const liveAuthScope = "service::user.auth.xboxlive.com::MBI_SSL"

var minecraftServiceErrorPattern = regexp.MustCompile(`minecraft/service:\s*([^:]+):\s*"([^"]*)"`)

// NewLiveTokenSource returns a Microsoft Live token source that uses ctx for
// outbound authentication requests. If ctx carries oauth2.HTTPClient, that
// client is used for device auth and refresh requests.
func NewLiveTokenSource(ctx context.Context, tok *oauth2.Token, out io.Writer) oauth2.TokenSource {
	return NewLiveTokenSourceWithPersist(ctx, tok, out, nil)
}

// NewLiveTokenSourceWithPersist is like NewLiveTokenSource but calls persist
// with every newly issued token so rotated refresh tokens survive restarts.
// A rejected refresh token falls back to device-code login: blocking before
// the first token is handed out, in the background afterwards, with Token
// failing fast until the operator completes it.
func NewLiveTokenSourceWithPersist(ctx context.Context, tok *oauth2.Token, out io.Writer, persist func(*oauth2.Token)) oauth2.TokenSource {
	return oauth2.ReuseTokenSource(tok, newLiveTokenSource(ctx, tok, out, persist))
}

func newLiveTokenSource(ctx context.Context, tok *oauth2.Token, out io.Writer, persist func(*oauth2.Token)) *liveTokenSource {
	if ctx == nil {
		ctx = context.Background()
	}
	if out == nil {
		out = io.Discard
	}
	return &liveTokenSource{
		ctx:           ctx,
		tok:           tok,
		out:           out,
		config:        auth.AndroidConfig,
		persist:       persist,
		retry:         authclient.RetryOptions{Attempts: 5},
		reauthTimeout: 15 * time.Minute,
		issued:        tok.Valid(),
	}
}

// NewXBLTokenSource adapts a Microsoft Live token source into an xsapi token
// source for Xbox Live session-directory calls.
func NewXBLTokenSource(ctx context.Context, live oauth2.TokenSource) xsapi.TokenSource {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = withDefaultXBLTokenCache(ctx)
	return auth.ContextSession(ctx, live)
}

// NewXSAPIClient creates the Xbox Live API client set used for MPSD, social,
// presence, and PlayFab-backed Minecraft service authentication.
func NewXSAPIClient(ctx context.Context, src xsapi.TokenSource, client *http.Client, log *slog.Logger) (*xsapi.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if src == nil {
		return nil, fmt.Errorf("xbox live token source is nil")
	}
	ctx = auth.WithContextClient(ctx, client)
	return xsapi.ClientConfig{HTTPClient: client, Logger: log, RTAMode: xsapi.RTALazy}.New(ctx, src)
}

func NewMinecraftTokenSource(ctx context.Context, xbl *xsapi.Client, client *http.Client) (service.TokenSource, error) {
	return newMinecraftTokenSource(ctx, xbl, client, nil)
}

func newMinecraftTokenSource(ctx context.Context, xbl *xsapi.Client, client *http.Client, log *slog.Logger) (service.TokenSource, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if xbl == nil {
		return nil, fmt.Errorf("xbox live client is nil")
	}
	ctx = auth.WithContextClient(ctx, client)
	debugLog(log, "discovering minecraft services")
	discovery, err := service.Discover(ctx, service.ApplicationTypeMinecraftPE, protocol.CurrentVersion)
	if err != nil {
		return nil, fmt.Errorf("discover minecraft services: %w", err)
	}
	debugLog(log, "discovered minecraft services")
	debugLog(log, "loading minecraft auth environment")
	env := new(service.AuthorizationEnvironment)
	if err := discovery.Environment(env); err != nil {
		return nil, fmt.Errorf("load auth environment: %w", err)
	}
	debugLog(log, "loaded minecraft auth environment", "playfab_title_id", env.PlayFabTitleID)
	if client != nil {
		env.HTTPClient = client
	}
	debugLog(log, "logging in to playfab with xbox")
	playfabClient, err := playfab.LoginWithXbox(ctx, env.PlayFabTitleID, xbl, playfab.ClientConfig{
		HTTPClient:    client,
		CreateAccount: true,
	})
	if err != nil {
		return nil, fmt.Errorf("login to playfab with xbox: %w", err)
	}
	debugLog(log, "logged in to playfab with xbox")
	return withMinecraftTokenDiagnostics(env.TokenSource(playfabClient, service.TokenConfig{})), nil
}

func withMinecraftTokenDiagnostics(src service.TokenSource) service.TokenSource {
	if src == nil {
		return nil
	}
	if _, ok := src.(diagnosticMinecraftTokenSource); ok {
		return src
	}
	return diagnosticMinecraftTokenSource{TokenSource: src}
}

type diagnosticMinecraftTokenSource struct {
	service.TokenSource
}

func (s diagnosticMinecraftTokenSource) ServiceToken(ctx context.Context) (*service.Token, error) {
	tok, err := s.TokenSource.ServiceToken(ctx)
	if err != nil {
		return nil, minecraftServiceTokenError(err)
	}
	return tok, nil
}

func minecraftServiceTokenError(err error) error {
	matches := minecraftServiceErrorPattern.FindStringSubmatch(err.Error())
	if len(matches) != 3 {
		return err
	}
	detail := &minecraftTokenHeaderError{
		Code:    matches[1],
		Message: matches[2],
		err:     err,
	}
	return detail
}

type minecraftTokenHeaderError struct {
	Code    string
	Message string
	err     error
}

func (e *minecraftTokenHeaderError) Error() string {
	return fmt.Sprintf("failed to get MC token header: error: %s, error message: %s", e.Code, e.Message)
}

func (e *minecraftTokenHeaderError) Unwrap() error {
	return e.err
}

func withDefaultXBLTokenCache(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return auth.WithXBLTokenCache(ctx, auth.AndroidConfig.NewTokenCache())
}

// errLiveReauthPending is returned while a background device-code login runs.
var errLiveReauthPending = errors.New("microsoft sign-in required: complete the device-code sign-in shown in the console or notification")

// liveTokenSource is only called through oauth2.ReuseTokenSource, which
// serialises Token calls.
type liveTokenSource struct {
	ctx     context.Context
	tok     *oauth2.Token
	out     io.Writer
	config  auth.Config
	persist func(*oauth2.Token)
	retry   authclient.RetryOptions

	reauthTimeout time.Duration
	issued        bool          // a token was handed out, so logins must not block callers
	reauth        *pendingLogin // in-flight background device-code login
}

type pendingLogin struct {
	done chan struct{}
	tok  *oauth2.Token
	err  error
}

func (s *liveTokenSource) Token() (*oauth2.Token, error) {
	if s.reauth != nil {
		return s.finishReauth()
	}
	var cause error
	if s.tok != nil && s.tok.RefreshToken != "" {
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		defer cancel()

		tok, err := refreshLiveToken(ctx, s.config.ClientID, s.tok.RefreshToken, s.retry)
		if err == nil {
			return s.store(tok), nil
		}
		// Only fall back to device-code login when the server rejected the
		// refresh token itself; transport failures and transient server
		// errors should surface instead of prompting a needless re-login.
		var refreshErr *liveRefreshError
		if !errors.As(err, &refreshErr) || !refreshErr.requiresReauth() {
			return nil, err
		}
		cause = err
	}
	if s.issued {
		s.startReauth()
		if cause != nil {
			return nil, fmt.Errorf("%w: %w", errLiveReauthPending, cause)
		}
		return nil, errLiveReauthPending
	}
	tok, err := requestLiveTokenWriter(s.ctx, s.config, s.out)
	if err != nil {
		return nil, err
	}
	return s.store(tok), nil
}

// store records the newly issued token in memory and through the persist hook.
func (s *liveTokenSource) store(tok *oauth2.Token) *oauth2.Token {
	s.tok = tok
	s.issued = true
	if s.persist != nil {
		s.persist(tok)
	}
	return tok
}

// startReauth runs a bounded device-code login without holding the caller.
func (s *liveTokenSource) startReauth() {
	login := &pendingLogin{done: make(chan struct{})}
	s.reauth = login
	_, _ = fmt.Fprintln(s.out, "Microsoft sign-in expired. Xbox requests fail until the device-code sign-in completes.")
	go func() {
		defer close(login.done)
		ctx, cancel := context.WithTimeout(s.ctx, s.reauthTimeout)
		defer cancel()
		login.tok, login.err = requestLiveTokenWriter(ctx, s.config, s.out)
		if login.err != nil {
			_, _ = fmt.Fprintf(s.out, "Microsoft sign-in failed: %v. A new sign-in starts with the next Xbox request.\n", login.err)
			return
		}
		if s.persist != nil {
			s.persist(login.tok)
		}
	}()
}

// finishReauth returns the background login's outcome once it has finished.
func (s *liveTokenSource) finishReauth() (*oauth2.Token, error) {
	select {
	case <-s.reauth.done:
	default:
		return nil, errLiveReauthPending
	}
	login := s.reauth
	s.reauth = nil
	if login.err != nil {
		return nil, fmt.Errorf("microsoft sign-in: %w", login.err)
	}
	s.tok = login.tok
	return login.tok, nil
}

func requestLiveTokenWriter(ctx context.Context, conf auth.Config, out io.Writer) (*oauth2.Token, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if out == nil {
		out = io.Discard
	}
	tok, err := conf.RequestLiveTokenContext(ctx, out)
	var retrieveErr *oauth2.RetrieveError
	if errors.As(err, &retrieveErr) && retrieveErr.ErrorCode == "invalid_grant" {
		return nil, fmt.Errorf("%w; set or reset the Microsoft account password, then retry the device login", err)
	}
	return tok, err
}

// refreshLiveToken redeems refreshToken, keeping it when the response omits a
// rotated one.
func refreshLiveToken(ctx context.Context, clientID, refreshToken string, retry authclient.RetryOptions) (*oauth2.Token, error) {
	resp, err := postLiveForm(ctx, microsoft.LiveConnectEndpoint.TokenURL, retry, url.Values{
		"client_id":     {clientID},
		"scope":         {liveAuthScope},
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body := new(liveTokenResponse)
	if err := json.NewDecoder(resp.Body).Decode(body); err != nil {
		return nil, fmt.Errorf("POST %s: json decode: %w", microsoft.LiveConnectEndpoint.TokenURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &liveRefreshError{Code: body.Error, Description: body.ErrorDescription}
	}
	tok := body.token()
	if tok.RefreshToken == "" {
		tok.RefreshToken = refreshToken
	}
	return tok, nil
}

// liveRefreshError is a Microsoft Live token endpoint rejection of a refresh
// token, such as invalid_grant for an expired or revoked token.
type liveRefreshError struct {
	Code        string
	Description string
}

func (e *liveRefreshError) Error() string {
	return fmt.Sprintf("POST %s: refresh error: %v: %v", microsoft.LiveConnectEndpoint.TokenURL, e.Code, e.Description)
}

// requiresReauth reports whether the rejection means the refresh token is no
// longer usable and a fresh login is required. Other OAuth error codes (such
// as server_error or temporarily_unavailable) may succeed on retry.
func (e *liveRefreshError) requiresReauth() bool {
	return e.Code == "invalid_grant"
}

func postLiveForm(ctx context.Context, endpoint string, retry authclient.RetryOptions, form url.Values) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request for POST %s: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := authclient.SendRequestWithRetries(ctx, liveAuthHTTPClient(ctx), req, retry)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", endpoint, err)
	}
	return resp, nil
}

type liveTokenResponse struct {
	AccessToken      string `json:"access_token"`
	TokenType        string `json:"token_type"`
	RefreshToken     string `json:"refresh_token"`
	ExpiresIn        int64  `json:"expires_in"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (r *liveTokenResponse) token() *oauth2.Token {
	tok := &oauth2.Token{
		AccessToken:  r.AccessToken,
		TokenType:    r.TokenType,
		RefreshToken: r.RefreshToken,
	}
	if r.ExpiresIn > 0 {
		tok.Expiry = time.Now().Add(time.Duration(r.ExpiresIn) * time.Second)
	}
	return tok
}

func liveAuthHTTPClient(ctx context.Context) *http.Client {
	if ctx != nil {
		if client, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok && client != nil {
			return client
		}
	}
	return defaultLiveAuthHTTPClient
}

var defaultLiveAuthHTTPClient = newDefaultLiveAuthHTTPClient()

func newDefaultLiveAuthHTTPClient() *http.Client {
	baseTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok || baseTransport == nil {
		baseTransport = &http.Transport{}
	}
	transport := baseTransport.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.Renegotiation = tls.RenegotiateOnceAsClient
	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}
