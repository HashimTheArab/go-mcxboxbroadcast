package broadcaster

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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

// NewLiveTokenSource returns a Microsoft Live token source that uses ctx for
// outbound authentication requests. If ctx carries oauth2.HTTPClient, that
// client is used for device auth and refresh requests.
func NewLiveTokenSource(ctx context.Context, tok *oauth2.Token, out io.Writer) oauth2.TokenSource {
	if ctx == nil {
		ctx = context.Background()
	}
	if out == nil {
		out = io.Discard
	}
	return oauth2.ReuseTokenSource(tok, &liveTokenSource{
		ctx:    ctx,
		tok:    tok,
		out:    out,
		config: auth.AndroidConfig,
	})
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
	if client != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, client)
	}
	return xsapi.ClientConfig{HTTPClient: client, Logger: log, RTAMode: xsapi.RTALazy}.New(ctx, src)
}

func NewMinecraftTokenSource(ctx context.Context, xbl *xsapi.Client, client *http.Client) (service.TokenSource, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if xbl == nil {
		return nil, fmt.Errorf("xbox live client is nil")
	}
	if client != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, client)
	}
	discovery, err := service.Discover(ctx, service.ApplicationTypeMinecraftPE, protocol.CurrentVersion)
	if err != nil {
		return nil, fmt.Errorf("discover minecraft services: %w", err)
	}
	env := new(service.AuthorizationEnvironment)
	if err := discovery.Environment(env); err != nil {
		return nil, fmt.Errorf("load auth environment: %w", err)
	}
	if client != nil {
		env.HTTPClient = client
	}
	playfabClient, err := playfab.LoginWithXbox(ctx, env.PlayFabTitleID, xbl, playfab.ClientConfig{
		HTTPClient:    client,
		CreateAccount: true,
	})
	if err != nil {
		return nil, fmt.Errorf("login to playfab with xbox: %w", err)
	}
	return env.TokenSource(playfabClient, service.TokenConfig{}), nil
}

func withDefaultXBLTokenCache(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return auth.WithXBLTokenCache(ctx, auth.AndroidConfig.NewTokenCache())
}

type liveTokenSource struct {
	ctx    context.Context
	tok    *oauth2.Token
	out    io.Writer
	config auth.Config
}

func (s *liveTokenSource) Token() (*oauth2.Token, error) {
	if s.tok != nil && s.tok.RefreshToken != "" {
		ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
		defer cancel()

		tok, err := refreshLiveToken(ctx, s.config.ClientID, s.tok.RefreshToken)
		if err != nil {
			return nil, err
		}
		s.tok = tok
		return tok, nil
	}
	tok, err := requestLiveTokenWriter(s.ctx, s.config, s.out)
	if err != nil {
		return nil, err
	}
	s.tok = tok
	return tok, nil
}

func requestLiveTokenWriter(ctx context.Context, conf auth.Config, out io.Writer) (*oauth2.Token, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if out == nil {
		out = io.Discard
	}
	return conf.RequestLiveTokenContext(ctx, out)
}

func refreshLiveToken(ctx context.Context, clientID, refreshToken string) (*oauth2.Token, error) {
	resp, err := postLiveForm(ctx, microsoft.LiveConnectEndpoint.TokenURL, url.Values{
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
		return nil, fmt.Errorf("POST %s: refresh error: %v: %v", microsoft.LiveConnectEndpoint.TokenURL, body.Error, body.ErrorDescription)
	}
	return body.token(), nil
}

func postLiveForm(ctx context.Context, endpoint string, form url.Values) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request for POST %s: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := authclient.SendRequestWithRetries(ctx, liveAuthHTTPClient(ctx), req, authclient.RetryOptions{Attempts: 5})
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
