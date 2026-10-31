package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	broadcaster "github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/df-mc/go-xsapi/v2"
	"golang.org/x/oauth2"
)

func TestRunBroadcasterCommandStartsAndClosesBroadcaster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var output strings.Builder
	started, closed := false, false
	var closedClients int
	deps := testDeps(func(broadcaster.Config) (commandBroadcaster, error) {
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
	})
	deps.Stdout = &output
	deps.CloseXSAPIClient = func(*slog.Logger, *xsapi.Client) { closedClients++ }
	if err := runBroadcasterCommand(ctx, commandOptions{ConfigPath: "/base/config.yml"}, deps); err != nil {
		t.Fatal(err)
	}
	if !started || !closed {
		t.Fatalf("started=%v closed=%v", started, closed)
	}
	if closedClients != 1 {
		t.Fatalf("expected the account's client to be closed, got %d", closedClients)
	}
	if got := output.String(); !strings.Contains(got, "starting go-mcxboxbroadcast") || strings.Contains(got, "msg=broadcasting") {
		t.Fatalf("unexpected lifecycle logs: %q", got)
	}
}

func TestRunBroadcasterCommandClosesXSAPIClientWhenStartFails(t *testing.T) {
	startErr := errors.New("start failed")
	var closedClients int
	deps := testDeps(func(broadcaster.Config) (commandBroadcaster, error) {
		return fakeCommandBroadcaster{start: func(context.Context) error { return startErr }}, nil
	})
	deps.CloseXSAPIClient = func(*slog.Logger, *xsapi.Client) { closedClients++ }
	err := runBroadcasterCommand(context.Background(), commandOptions{ConfigPath: "/base/config.yml"}, deps)
	if !errors.Is(err, startErr) {
		t.Fatalf("expected start error, got %v", err)
	}
	if closedClients != 1 {
		t.Fatalf("expected the account's client to be closed, got %d", closedClients)
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
			deps := testDeps(func(broadcaster.Config) (commandBroadcaster, error) {
				return fakeCommandBroadcaster{
					start: func(context.Context) error { return nil },
					wait:  func() error { return runErr },
					close: func() error {
						closed = true
						return tc.closeErr
					},
				}, nil
			})
			deps.CloseXSAPIClient = func(*slog.Logger, *xsapi.Client) { closedClients++ }
			result := make(chan error, 1)
			go func() {
				result <- runBroadcasterCommand(ctx, commandOptions{ConfigPath: "/base/config.yml"}, deps)
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
			if closedClients != 1 {
				t.Fatalf("expected the account's client to be closed, got %d", closedClients)
			}
		})
	}
}

// A configured proxy reaches the broadcaster and every authentication step.
func TestRunBroadcasterCommandAppliesConfiguredHTTPProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var gotClient, gotLiveAuthClient, gotXBLAuthClient *http.Client
	deps := testDeps(func(conf broadcaster.Config) (commandBroadcaster, error) {
		gotClient = conf.HTTPClient
		return fakeCommandBroadcaster{start: func(context.Context) error {
			cancel()
			return nil
		}}, nil
	})
	load := deps.LoadConfig
	deps.LoadConfig = func(path string) (broadcaster.ConfigFile, error) {
		cfg, err := load(path)
		cfg.HTTP.Proxy = "http://127.0.0.1:8080"
		return cfg, err
	}
	deps.NewLiveTokenSource = func(ctx context.Context, _ *oauth2.Token, _ io.Writer, _ func(*oauth2.Token)) oauth2.TokenSource {
		gotLiveAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
		return staticOAuthTokenSource{}
	}
	deps.NewXBLTokenSource = func(ctx context.Context, _ oauth2.TokenSource) xsapi.TokenSource {
		gotXBLAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
		return nil
	}
	if err := runBroadcasterCommand(ctx, commandOptions{ConfigPath: "/base/config.yml"}, deps); err != nil {
		t.Fatal(err)
	}
	if gotClient == nil {
		t.Fatal("expected configured HTTP client")
	}
	if gotLiveAuthClient != gotClient || gotXBLAuthClient != gotClient {
		t.Fatal("expected Live and Xbox auth contexts to use the configured HTTP client")
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

func TestRunBroadcasterCommandPreservesHTTPClientWithoutConfiguredProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	baseClient := &http.Client{}
	var gotLiveAuthClient, gotRuntimeClient *http.Client
	deps := testDeps(func(conf broadcaster.Config) (commandBroadcaster, error) {
		gotRuntimeClient = conf.HTTPClient
		return fakeCommandBroadcaster{start: func(context.Context) error {
			cancel()
			return nil
		}}, nil
	})
	deps.HTTPClient = baseClient
	deps.NewLiveTokenSource = func(ctx context.Context, _ *oauth2.Token, _ io.Writer, _ func(*oauth2.Token)) oauth2.TokenSource {
		gotLiveAuthClient, _ = ctx.Value(oauth2.HTTPClient).(*http.Client)
		return staticOAuthTokenSource{}
	}
	if err := runBroadcasterCommand(ctx, commandOptions{ConfigPath: "/base/config.yml"}, deps); err != nil {
		t.Fatal(err)
	}
	if gotRuntimeClient != baseClient {
		t.Fatal("expected HTTP client to be preserved when no proxy is configured")
	}
	if gotLiveAuthClient != nil {
		t.Fatal("expected Live auth to keep default HTTP behavior when no proxy is configured")
	}
}

// testDeps fakes every command dependency, with newBroadcaster standing in for broadcaster.New.
func testDeps(newBroadcaster func(broadcaster.Config) (commandBroadcaster, error)) commandDeps {
	return commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Session.JoinURL = "http://127.0.0.1:19132"
			return cfg, nil
		},
		LoadLiveToken: func(string) (*oauth2.Token, error) { return nil, errors.ErrUnsupported },
		NewLiveTokenSource: func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken:     func(string, *oauth2.Token) error { return nil },
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource { return nil },
		NewXSAPIClient: func(context.Context, xsapi.TokenSource, *http.Client, *slog.Logger) (*xsapi.Client, error) {
			return &xsapi.Client{}, nil
		},
		CloseXSAPIClient: func(*slog.Logger, *xsapi.Client) {},
		NewBroadcaster:   newBroadcaster,
	}
}

type staticOAuthTokenSource struct{}

func (staticOAuthTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "token"}, nil
}

type fakeCommandBroadcaster struct {
	start func(context.Context) error
	wait  func() error
	close func() error
}

func (fakeCommandBroadcaster) Health() broadcaster.Health {
	return broadcaster.Health{}
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

// Close returns the simulated close result, or success when omitted.
func (f fakeCommandBroadcaster) Close() error {
	if f.close == nil {
		return nil
	}
	return f.close()
}
