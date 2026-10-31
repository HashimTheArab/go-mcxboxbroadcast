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
	"os"
	"path/filepath"
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

// NewLiveTokenSource returns a Microsoft Live token source that calls persist, if non-nil, with each new token.
// A rejected refresh token falls back to device-code login on out; after the first token, Token fails fast until it completes.
func NewLiveTokenSource(ctx context.Context, tok *oauth2.Token, out io.Writer, persist func(*oauth2.Token)) oauth2.TokenSource {
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
	return auth.ContextSession(auth.WithXBLTokenCache(ctx, auth.AndroidConfig.NewTokenCache()), live)
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

// NewMinecraftTokenSource logs in to PlayFab with xbl and returns the Minecraft services token
// source for signaling and gallery requests. A nil log uses slog.Default.
func NewMinecraftTokenSource(ctx context.Context, xbl *xsapi.Client, client *http.Client, log *slog.Logger) (service.TokenSource, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if log == nil {
		log = slog.Default()
	}
	if xbl == nil {
		return nil, fmt.Errorf("xbox live client is nil")
	}
	ctx = auth.WithContextClient(ctx, client)
	log.Debug("discovering minecraft services")
	discovery, err := service.Discover(ctx, service.ApplicationTypeMinecraftPE, protocol.CurrentVersion)
	if err != nil {
		return nil, fmt.Errorf("discover minecraft services: %w", err)
	}
	env := new(service.AuthorizationEnvironment)
	if err := discovery.Environment(env); err != nil {
		return nil, fmt.Errorf("load auth environment: %w", err)
	}
	log.Debug("loaded minecraft auth environment", "playfab_title_id", env.PlayFabTitleID)
	if client != nil {
		env.HTTPClient = client
	}
	log.Debug("logging in to playfab with xbox")
	playfabClient, err := playfab.LoginWithXbox(ctx, env.PlayFabTitleID, xbl, playfab.ClientConfig{
		HTTPClient:    client,
		CreateAccount: true,
	})
	if err != nil {
		return nil, fmt.Errorf("login to playfab with xbox: %w", err)
	}
	log.Debug("logged in to playfab with xbox")
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

// requestLiveTokenWriter runs device-code logins until one succeeds or ctx ends.
// A code that expires or that Microsoft rejects is explained on out and
// replaced, so the operator sees why before the next code.
func requestLiveTokenWriter(ctx context.Context, conf auth.Config, out io.Writer) (*oauth2.Token, error) {
	for {
		tok, err := conf.RequestLiveTokenContext(ctx, out)
		if err == nil {
			return tok, nil
		}
		notice, retry := liveLoginRetry(ctx, err)
		if !retry {
			return nil, err
		}
		_, _ = fmt.Fprintln(out, notice)
	}
}

// msaSubErrorPasswordRequired is the Microsoft account sub-error returned for
// an account without a password; setting one let the same sign-in succeed.
const msaSubErrorPasswordRequired = "0x80043558"

// liveLoginRetry reports whether a failed device-code login should restart
// with a new code, and the notice explaining the failure.
func liveLoginRetry(ctx context.Context, err error) (notice string, retry bool) {
	if ctx.Err() != nil {
		return "", false
	}
	const expired = "The sign-in code expired before anyone used it. Use the new code below."
	// Polling ends with the bare context error once the code's own deadline passes.
	if errors.Is(err, context.DeadlineExceeded) && !errors.As(err, new(*url.Error)) {
		return expired, true
	}
	var retrieveErr *oauth2.RetrieveError
	if !errors.As(err, &retrieveErr) {
		return "", false
	}
	switch retrieveErr.ErrorCode {
	case "expired_token":
		return expired, true
	case "access_denied":
		return "The Microsoft sign-in was declined. Use the new code below.", true
	case "invalid_grant":
		return liveLoginRejection(retrieveErr), true
	}
	return "", false
}

// liveLoginRejection explains an invalid_grant from device-code polling. The
// Microsoft account error codes only arrive in response headers.
func liveLoginRejection(e *oauth2.RetrieveError) string {
	var code, subCode string
	if e.Response != nil {
		// Account type | error | sub-error | cloud instance | data boundary.
		if parts := strings.Split(e.Response.Header.Get("X-Ms-Clientdata"), "|"); len(parts) >= 3 {
			code, subCode = strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])
		}
		if code == "" {
			code = e.Response.Header.Get("X-Wlid-Error")
		}
	}
	if strings.EqualFold(subCode, msaSubErrorPasswordRequired) {
		return "Microsoft rejected the sign-in because this account needs a password. Set one at https://account.microsoft.com/security, then use the new code below."
	}
	var body struct {
		CorrelationID string `json:"correlation_id"`
	}
	_ = json.Unmarshal(e.Body, &body)
	var details []string
	for _, d := range [][2]string{{"error", code}, {"sub-error", subCode}, {"correlation ID", body.CorrelationID}} {
		if d[1] != "" {
			details = append(details, d[0]+" "+d[1])
		}
	}
	description := e.ErrorDescription
	if description == "" {
		description = e.ErrorCode
	}
	if len(details) > 0 {
		description += " (" + strings.Join(details, ", ") + ")"
	}
	return "Microsoft rejected the sign-in: " + description + " Sign in at https://account.microsoft.com with this account and finish any steps it asks for, then use the new code below."
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
	if client, ok := ctx.Value(oauth2.HTTPClient).(*http.Client); ok && client != nil {
		return client
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

func LoadLiveToken(path string) (*oauth2.Token, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var tok oauth2.Token
	if err := json.NewDecoder(f).Decode(&tok); err != nil {
		return nil, err
	}
	if tok.AccessToken == "" && tok.RefreshToken == "" {
		return nil, errors.New("cached token is empty")
	}
	return &tok, nil
}

// SaveLiveToken atomically replaces the token cache at path, readable only by its owner.
func SaveLiveToken(path string, tok *oauth2.Token) error {
	if tok == nil {
		return errors.New("token is nil")
	}
	data, err := json.MarshalIndent(tok, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(data, '\n'))
}

// writeFileAtomic replaces path with data through a synced 0600 temp file in
// the same directory, so a crash leaves either the old or the new content.
func writeFileAtomic(path string, data []byte) (err error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	// Persist the rename itself; not every platform can sync a directory.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
