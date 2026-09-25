package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
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
		LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token)) (oauth2.TokenSource, error) {
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
		LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token)) (oauth2.TokenSource, error) {
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
					LoadAccountToken: func(context.Context, string, io.Writer, func(*oauth2.Token)) (oauth2.TokenSource, error) {
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
		LoadAccountToken: func(ctx context.Context, _ string, _ io.Writer, _ func(*oauth2.Token)) (oauth2.TokenSource, error) {
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

func (fakeCommandBroadcaster) HealthHandler() http.Handler { return http.NotFoundHandler() }

// Default outbound requests must be bounded so a hung Xbox call fails.
func TestDefaultCommandHTTPClientHasTimeout(t *testing.T) {
	if got := (commandDeps{}).withDefaults().HTTPClient.Timeout; got != defaultHTTPTimeout {
		t.Fatalf("default HTTP client timeout = %s, want %s", got, defaultHTTPTimeout)
	}
}

// The health endpoint serves the broadcaster's probes until stopped.
func TestServeHealthServesUntilStopped(t *testing.T) {
	stop, err := serveHealth("", http.NotFoundHandler(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	stop()

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	stop, err = serveHealth(addr, handler, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want 200", resp.StatusCode)
	}
	stop()
	if _, err := http.Get("http://" + addr + "/healthz"); err == nil {
		t.Fatal("health endpoint still served after stop")
	}
}
