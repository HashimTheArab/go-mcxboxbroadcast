package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	broadcaster "github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/df-mc/go-xsapi"
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
		NewLiveTokenSource: func(*oauth2.Token, io.Writer) oauth2.TokenSource {
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
	started := false
	closed := false
	var gotSubAccounts int
	err := runBroadcasterCommand(ctx, commandOptions{
		ConfigPath: "/base/config.yml",
	}, commandDeps{
		Stdout: io.Discard,
		LoadConfig: func(string) (broadcaster.ConfigFile, error) {
			cfg := broadcaster.DefaultConfigFile()
			cfg.Session.RemoteAddress = "127.0.0.1"
			cfg.Session.RemotePort = "19132"
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
		NewLiveTokenSource: func(*oauth2.Token, io.Writer) oauth2.TokenSource {
			return staticOAuthTokenSource{}
		},
		SaveLiveToken: func(string, *oauth2.Token) error {
			return nil
		},
		LoadAccountToken: func(string, io.Writer) (oauth2.TokenSource, error) {
			return staticOAuthTokenSource{}, nil
		},
		NewXBLTokenSource: func(context.Context, oauth2.TokenSource) xsapi.TokenSource {
			return staticXBLTokenSource{}
		},
		NewBroadcaster: func(conf broadcaster.Config) (commandBroadcaster, error) {
			gotSubAccounts = len(conf.SubAccounts)
			return fakeCommandBroadcaster{
				start: func(context.Context) error {
					started = true
					cancel()
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
}

type staticOAuthTokenSource struct{}

func (staticOAuthTokenSource) Token() (*oauth2.Token, error) {
	return &oauth2.Token{AccessToken: "token"}, nil
}

type staticXBLTokenSource struct{}

func (staticXBLTokenSource) Token() (xsapi.Token, error) {
	return nil, nil
}

type fakeCommandBroadcaster struct {
	start func(context.Context) error
	close func() error
}

func (f fakeCommandBroadcaster) Start(ctx context.Context) error {
	return f.start(ctx)
}

func (f fakeCommandBroadcaster) Close() error {
	return f.close()
}
