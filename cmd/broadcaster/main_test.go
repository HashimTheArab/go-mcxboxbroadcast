package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	broadcaster "github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/df-mc/go-xsapi/v2"
	"golang.org/x/oauth2"
)

func TestSubAccountCachePathUsesExplicitPath(t *testing.T) {
	path, err := subAccountCachePath("/base", broadcaster.SubAccountFile{
		ID:        "alt",
		CachePath: "cache/alt.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join("/base", "cache", "alt.json") {
		t.Fatalf("unexpected path %q", path)
	}
}

func TestSubAccountCachePathDerivesFromID(t *testing.T) {
	path, err := subAccountCachePath("/base", broadcaster.SubAccountFile{ID: "alt"})
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join("/base", "cache", "sub_accounts", "alt", "live_token.json")
	if path != want {
		t.Fatalf("unexpected path %q", path)
	}
}

func TestSubAccountCachePathRequiresIDWhenPathOmitted(t *testing.T) {
	if _, err := subAccountCachePath("/base", broadcaster.SubAccountFile{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunBroadcasterCommandRejectsDuplicateSubAccountCacheBeforeAuth(t *testing.T) {
	var authenticated bool
	err := runBroadcasterCommand(context.Background(), commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Accounts.PrimaryCachePath = "cache/live_token.json"
			cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{
				ID:        "alt",
				Enabled:   true,
				CachePath: "cache/live_token.json",
			}}
			return cfg, nil
		},
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			authenticated = true
			return staticOAuthTokenSource{}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate account cache path") {
		t.Fatalf("expected duplicate cache path error, got %v", err)
	}
	if authenticated {
		t.Fatal("authenticated before rejecting duplicate sub-account cache path")
	}
}

func TestRunBroadcasterCommandStartsAndClosesBroadcaster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var output strings.Builder
	started := false
	closed := false
	var gotSubAccounts int
	var closedClients int
	err := runBroadcasterCommand(ctx, commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: &output,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Session.SessionInfo.Port = 19132
			cfg.Accounts.PrimaryCachePath = "cache/live_token.json"
			cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{
				ID:      "alt",
				Enabled: true,
			}}
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) {
			return nil, errors.ErrUnsupported
		},
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token), time.Duration) (oauth2.TokenSource, error) {
			return staticOAuthTokenSource{}, nil
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
			return nil
		},
		NewXSAPIClient: testNewXSAPIClient,
		CloseXSAPIClients: func(_ *slog.Logger, clients []*xsapi.Client) {
			closedClients = len(clients)
		},
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotSubAccounts = len(conf.SubAccounts)
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					started = true
					cancel()
					return nil
				},
				wait: func() error {
					<-ctx.Done()
					return nil
				},
				close: func() error {
					closed = true
					return nil
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !started || !closed {
		t.Fatalf("started=%v closed=%v", started, closed)
	}
	if gotSubAccounts != 1 {
		t.Fatalf("expected one sub-account, got %d", gotSubAccounts)
	}
	if closedClients != 2 {
		t.Fatalf("expected primary and sub-account clients to be closed, got %d", closedClients)
	}
	if got := output.String(); !strings.Contains(got, "starting go-mcxboxbroadcast") || strings.Contains(got, "msg=broadcasting") {
		t.Fatalf("unexpected lifecycle logs: %q", got)
	}
}

func TestRunBroadcasterCommandClosesXSAPIClientsWhenStartFails(t *testing.T) {
	startErr := errors.New("start failed")
	var closedClients int
	err := runBroadcasterCommand(context.Background(), commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Session.SessionInfo.Port = 19132
			cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{
				ID:      "alt",
				Enabled: true,
			}}
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) {
			return nil, errors.ErrUnsupported
		},
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token), time.Duration) (oauth2.TokenSource, error) {
			return staticOAuthTokenSource{}, nil
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
			return nil
		},
		NewXSAPIClient: testNewXSAPIClient,
		CloseXSAPIClients: func(_ *slog.Logger, clients []*xsapi.Client) {
			closedClients = len(clients)
		},
		NewBroadcaster: func(broadcaster.Config) (commandBroadcaster, error) {
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					return startErr
				},
			}, nil
		},
	})
	if !errors.Is(err, startErr) {
		t.Fatalf("expected start error, got %v", err)
	}
	if closedClients != 2 {
		t.Fatalf("expected primary and sub-account clients to be closed, got %d", closedClients)
	}
}

func TestRunBroadcasterCommandClosesAfterInternalFailure(t *testing.T) {
	runErr := errors.New("session recovery exhausted")
	closeErr := errors.New("listener close failed")
	for _, tc := range []struct {
		name     string
		closeErr error
	}{
		{name: "runtime failure"},
		{name: "runtime and close failure", closeErr: closeErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var closed bool
			var closedClients int
			result := make(chan error, 1)
			go func() {
				result <- runBroadcasterCommand(ctx, commandOptions{
					ConfigPath: "/base/config.yml",
				}, commandDeps{
					Stdout: io.Discard,
					LoadConfig: func(string) (broadcaster.ConfigFile, error) {
						cfg := broadcaster.DefaultConfigFile()
						cfg.Session.SessionInfo.IP = "127.0.0.1"
						cfg.Session.SessionInfo.Port = 19132
						cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{ID: "alt", Enabled: true}}
						return cfg, nil
					},
					LoadLiveToken: func(string) (*oauth2.Token, error) {
						return nil, errors.ErrUnsupported
					},
					NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
						return staticOAuthTokenSource{}
					},
					SaveLiveToken: func(string, *oauth2.Token) error {
						return nil
					},
					LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token), time.Duration) (oauth2.TokenSource, error) {
						return staticOAuthTokenSource{}, nil
					},
					NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
						return nil
					},
					NewXSAPIClient: testNewXSAPIClient,
					CloseXSAPIClients: func(_ *slog.Logger, clients []*xsapi.Client) {
						closedClients = len(clients)
					},
					NewBroadcaster: func(broadcaster.Config) (commandBroadcaster, error) {
						return fakeCommandBroadcaster{
							start: func(context.Context) error { return nil },
							wait:  func() error { return runErr },
							close: func() error {
								closed = true
								return tc.closeErr
							},
						}, nil
					},
				})
			}()
			select {
			case err := <-result:
				if ctx.Err() != nil {
					t.Fatal("external context was canceled before command returned")
				}
				if !errors.Is(err, runErr) {
					t.Fatalf("expected runtime error, got %v", err)
				}
				if tc.closeErr != nil && !errors.Is(err, tc.closeErr) {
					t.Fatalf("expected close error alongside runtime error, got %v", err)
				}
			case <-time.After(5 * time.Second):
				cancel()
				<-result
				t.Fatal("command did not return after internal broadcaster failure")
			}
			if !closed {
				t.Fatal("broadcaster was not closed after internal failure")
			}
			if closedClients != 2 {
				t.Fatalf("expected primary and sub-account clients to be closed, got %d", closedClients)
			}
		})
	}
}

func TestRunBroadcasterCommandAppliesConfiguredHTTPProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var gotClient *http.Client
	var gotAuthClient *http.Client
	err := runBroadcasterCommand(ctx, commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.HTTP.Proxy = "http://127.0.0.1:8080"
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Session.SessionInfo.Port = 19132
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) {
			return nil, errors.ErrUnsupported
		},
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		NewXBLTokenSource: func(ctx context.Context, _ oauth2.TokenSource) xsapi.TokenSource {
			gotAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
			return nil
		},
		NewXSAPIClient: testNewXSAPIClient,
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotClient = conf.HTTPClient
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					cancel()
					return nil
				},
				close: func() error {
					return nil
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotClient == nil {
		t.Fatal("expected configured HTTP client")
	}
	if gotAuthClient != gotClient {
		t.Fatal("expected Xbox auth context to use configured HTTP client")
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, err := gotClient.Transport.(*http.Transport).Proxy(req)
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL.String() != "http://127.0.0.1:8080" {
		t.Fatalf("unexpected proxy URL %q", proxyURL.String())
	}
}

func TestRunBroadcasterCommandAppliesConfiguredHTTPProxyToPrimaryLiveTokenSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var gotLiveAuthClient *http.Client
	var gotRuntimeClient *http.Client
	err := runBroadcasterCommand(ctx, commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.HTTP.Proxy = "http://127.0.0.1:8080"
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Session.SessionInfo.Port = 19132
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) {
			return nil, errors.ErrUnsupported
		},
		NewLiveTokenSource: func(ctx context.Context, _ *oauth2.Token, _ io.Writer, _ func(*oauth2.Token)) oauth2.TokenSource {
			gotLiveAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
			return nil
		},
		NewXSAPIClient: testNewXSAPIClient,
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotRuntimeClient = conf.HTTPClient
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					cancel()
					return nil
				},
				close: func() error {
					return nil
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotRuntimeClient == nil {
		t.Fatal("expected configured runtime HTTP client")
	}
	if gotLiveAuthClient != gotRuntimeClient {
		t.Fatal("expected primary Live auth source to use configured HTTP client")
	}
}

func TestRunBroadcasterCommandAppliesConfiguredHTTPProxyToSubAccountTokenLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var gotSubAccountAuthClient *http.Client
	var gotRuntimeClient *http.Client
	err := runBroadcasterCommand(ctx, commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.HTTP.Proxy = "http://127.0.0.1:8080"
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Session.SessionInfo.Port = 19132
			cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{
				ID:      "alt",
				Enabled: true,
			}}
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) {
			return nil, errors.ErrUnsupported
		},
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		LoadAccountToken: func(ctx context.Context, _ string, _ io.Writer, _ func(*oauth2.Token), _ time.Duration) (oauth2.TokenSource, error) {
			gotSubAccountAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
			return staticOAuthTokenSource{}, nil
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
			return nil
		},
		NewXSAPIClient: testNewXSAPIClient,
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotRuntimeClient = conf.HTTPClient
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					cancel()
					return nil
				},
				close: func() error {
					return nil
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotRuntimeClient == nil {
		t.Fatal("expected configured runtime HTTP client")
	}
	if gotSubAccountAuthClient != gotRuntimeClient {
		t.Fatal("expected sub-account token load to use configured HTTP client")
	}
}

func TestRunBroadcasterCommandPreservesHTTPClientWithoutConfiguredProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	baseClient := &http.Client{}
	var gotLiveAuthClient *http.Client
	var gotRuntimeClient *http.Client
	err := runBroadcasterCommand(ctx, commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout:     io.Discard,
		HTTPClient: baseClient,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Session.SessionInfo.Port = 19132
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) {
			return nil, errors.ErrUnsupported
		},
		NewLiveTokenSource: func(ctx context.Context, _ *oauth2.Token, _ io.Writer, _ func(*oauth2.Token)) oauth2.TokenSource {
			gotLiveAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
			return nil
		},
		NewXSAPIClient: testNewXSAPIClient,
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotRuntimeClient = conf.HTTPClient
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					cancel()
					return nil
				},
				close: func() error {
					return nil
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotRuntimeClient != baseClient {
		t.Fatal("expected HTTP client to be preserved when no proxy is configured")
	}
	if gotLiveAuthClient != nil {
		t.Fatal("expected Live auth to keep default HTTP behavior when no proxy is configured")
	}
}

// A sub-account that cannot sign in is skipped and reported; the primary still starts.
func TestRunBroadcasterCommandSkipsSubAccountWhoseLoginFails(t *testing.T) {
	var started bool
	var gotSubAccounts int
	var notified []string
	err := runBroadcasterCommand(context.Background(), commandOptions{ConfigPath: "/base/config.yml"}, commandDeps{
		Stdout: io.Discard,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			notified = append(notified, string(body))
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: http.Header{}}, nil
		})},
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Session.SessionInfo.IP = "127.0.0.1"
			cfg.Notifications = broadcaster.NotificationConfig{Enabled: true, WebhookURL: "https://hooks.test/webhook"}
			cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{ID: "alt", Enabled: true}}
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) { return nil, errors.ErrUnsupported },
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error { return nil },
		LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token), time.Duration) (oauth2.TokenSource, error) {
			return nil, errors.New("poll device token: expired_token")
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource { return nil },
		NewXSAPIClient:    testNewXSAPIClient,
		CloseXSAPIClients: func(*slog.Logger, []*xsapi.Client) {},
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotSubAccounts = len(conf.SubAccounts)
			return fakeCommandBroadcaster{start: func(context.Context) error { started = true; return nil }, close: func() error { return nil }}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !started || gotSubAccounts != 0 {
		t.Fatalf("started = %v with %d sub-accounts, want primary alone", started, gotSubAccounts)
	}
	if len(notified) != 1 || !strings.Contains(notified[0], "alt") || !strings.Contains(notified[0], "expired_token") {
		t.Fatalf("notifications = %q, want one naming the skipped sub-account", notified)
	}
}

// Relative and absolute spellings of one cache file must be caught as a duplicate.
func TestRunBroadcasterCommandRejectsEquivalentCachePaths(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	err = runBroadcasterCommand(context.Background(), commandOptions{ConfigPath: "config.yml"}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Accounts.PrimaryCachePath = "cache/live_token.json"
			cfg.Accounts.SubAccounts = []broadcaster.SubAccountFile{{
				ID:        "alt",
				Enabled:   true,
				CachePath: filepath.Join(wd, "cache", "..", "cache", "live_token.json"),
			}}
			return cfg, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "duplicate account cache path") {
		t.Fatalf("expected duplicate cache path error, got %v", err)
	}
}

// A sub-account sign-in nobody completes must give up at its timeout, not the device code's expiry.
func TestLoadAccountTokenStopsAtLoginTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, deviceLoginClient(t, new(atomic.Bool)))
		start := time.Now()
		_, err := loadAccountToken(ctx, filepath.Join(t.TempDir(), "token.json"), io.Discard, nil, time.Minute)
		if err == nil {
			t.Fatal("expected the unfinished sign-in to fail")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Minute {
			t.Fatalf("sign-in ran for %s, want it bounded by the one-minute timeout", elapsed)
		}
	})
}

// The source returned after sign-in must keep refreshing once the sign-in deadline has passed.
func TestLoadAccountTokenSourceOutlivesLoginTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		approved := new(atomic.Bool)
		approved.Store(true)
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, deviceLoginClient(t, approved))
		var persisted atomic.Int32
		src, err := loadAccountToken(ctx, filepath.Join(t.TempDir(), "token.json"), io.Discard, func(*oauth2.Token) { persisted.Add(1) }, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Minute)
		if _, err := src.Token(); err != nil {
			t.Fatalf("refresh after the sign-in deadline failed: %v", err)
		}
		if got := persisted.Load(); got != 2 {
			t.Fatalf("persisted %d tokens, want the sign-in and the refresh", got)
		}
		time.Sleep(time.Hour)
		synctest.Wait()
	})
}

// deviceLoginClient answers Microsoft device-code sign-in, completing it once
// approved is set, and issues one-second tokens so every call refreshes.
func deviceLoginClient(t *testing.T, approved *atomic.Bool) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		respond := func(code int, body string) (*http.Response, error) {
			return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{"Content-Type": {"application/json"}}}, nil
		}
		switch req.URL.String() {
		case "https://login.live.com/oauth20_connect.srf":
			return respond(http.StatusOK, `{"device_code":"device","user_code":"code","verification_uri":"https://www.microsoft.com/link","expires_in":900,"interval":1}`)
		case "https://login.live.com/oauth20_token.srf":
			if err := req.ParseForm(); err != nil {
				return nil, err
			}
			if req.Form.Get("grant_type") != "refresh_token" && !approved.Load() {
				return respond(http.StatusBadRequest, `{"error":"authorization_pending"}`)
			}
			return respond(http.StatusOK, `{"access_token":"access","token_type":"bearer","refresh_token":"refresh","expires_in":1}`)
		default:
			t.Errorf("unexpected request %s %s", req.Method, req.URL)
			return nil, errors.New("unexpected request")
		}
	})}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type staticOAuthTokenSource struct{}

func (staticOAuthTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "token"}, nil
}

func testNewXSAPIClient(context.Context, xsapi.TokenSource, *http.Client, *slog.Logger) (*xsapi.Client, error) {
	return &xsapi.Client{}, nil
}

type fakeCommandBroadcaster struct {
	start func(context.Context) error
	wait  func() error
	close func() error
}

func (f fakeCommandBroadcaster) Start(ctx context.Context) error {
	return f.start(ctx)
}

// Wait returns the simulated runtime result, or a normal shutdown when omitted.
func (f fakeCommandBroadcaster) Wait() error {
	if f.wait == nil {
		return nil
	}
	return f.wait()
}

func (f fakeCommandBroadcaster) Close() error {
	return f.close()
}
