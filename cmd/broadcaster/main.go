package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/xal/sisu"
	"golang.org/x/oauth2"
)

type commandOptions struct {
	ConfigPath string
	Debug      bool
}

type commandBroadcaster interface {
	Start(context.Context) error
	Close() error
}

type commandDeps struct {
	Stdout             io.Writer
	HTTPClient         *http.Client
	LoadConfig         func(string) (broadcaster.ConfigFile, error)
	LoadLiveToken      func(string) (*oauth2.Token, error)
	NewLiveTokenSource func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource
	SaveLiveToken      func(string, *oauth2.Token) error
	LoadAccountToken   func(context.Context, string, io.Writer, func(*oauth2.Token)) (oauth2.TokenSource, error)
	NewXBLTokenSource  func(context.Context, oauth2.TokenSource) xsapi.TokenSource
	NewXSAPIClient     func(context.Context, xsapi.TokenSource, *http.Client, *slog.Logger) (*xsapi.Client, error)
	CloseXSAPIClients  func(*slog.Logger, []*xsapi.Client)
	NewBroadcaster     func(broadcaster.Config) (commandBroadcaster, error)
}

func main() {
	var (
		configPath = flag.String("config", "config.yml", "configuration file path")
		debug      = flag.Bool("debug", false, "enable debug logs")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runBroadcasterCommand(ctx, commandOptions{ConfigPath: *configPath, Debug: *debug}, defaultCommandDeps()); err != nil {
		if code, ok := errors.AsType[sisu.ErrorCode](err); ok && code == sisu.ErrorCodeAgeVerificationRequired {
			fmt.Fprintln(os.Stderr, "broadcaster: authentication failed: complete Xbox/Microsoft age verification for this account, then retry")
		} else {
			slog.Error("broadcaster", "err", err)
		}
		os.Exit(1)
	}
}

func runBroadcasterCommand(ctx context.Context, opts commandOptions, deps commandDeps) error {
	deps = deps.withDefaults()
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.ConfigPath == "" {
		opts.ConfigPath = "config.yml"
	}
	cfg, err := deps.LoadConfig(opts.ConfigPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	httpClient, err := cfg.HTTP.Client(deps.HTTPClient)
	if err != nil {
		return fmt.Errorf("configure http client: %w", err)
	}
	authCtx := ctx
	if httpClient != nil && strings.TrimSpace(cfg.HTTP.Proxy) != "" {
		authCtx = context.WithValue(authCtx, oauth2.HTTPClient, httpClient)
	}
	level := slog.LevelInfo
	if opts.Debug || cfg.DebugMode {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(deps.Stdout, &slog.HandlerOptions{Level: level}))
	log.Debug("debug logging enabled")
	for _, note := range cfg.Notes {
		log.Warn("config adjusted", "note", note)
	}
	var xblClients []*xsapi.Client
	defer func() {
		deps.CloseXSAPIClients(log, xblClients)
	}()

	baseDir := filepath.Dir(opts.ConfigPath)
	cachePath := resolveConfigPath(baseDir, cfg.Accounts.PrimaryCachePath)
	if err := validateSubAccountCachePaths(baseDir, cachePath, cfg.Accounts.SubAccounts); err != nil {
		return err
	}

	var notifier broadcaster.Notifier
	if cfg.Notifications.Enabled {
		notifier = broadcaster.SlackNotifier{WebhookURL: cfg.Notifications.WebhookURL, Client: httpClient}
	}
	// Sign-in prompts (device-code URL and code) also go to the webhook so a
	// headless instance whose token dies can still be recovered.
	authOut := signInWriter(deps.Stdout, notifier, log)

	tok, err := deps.LoadLiveToken(cachePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn("could not load token cache", "err", err)
	}
	live := deps.NewLiveTokenSource(authCtx, tok, authOut, savePersistedToken(log, deps.SaveLiveToken, cachePath))
	tok, err = live.Token()
	if err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}
	if err := deps.SaveLiveToken(cachePath, tok); err != nil {
		log.Warn("could not save token cache", "err", err)
	}
	xblSource := deps.NewXBLTokenSource(authCtx, live)
	xblClient, err := deps.NewXSAPIClient(authCtx, xblSource, httpClient, log)
	if err != nil {
		return fmt.Errorf("authenticate xbox live: %w", err)
	}
	xblClients = append(xblClients, xblClient)

	runtime, err := cfg.RuntimeConfig(broadcaster.RuntimeConfigInput{
		XBLClient:      xblClient,
		XBLTokenSource: xblSource,
		XUID:           xblClient.UserInfo().XUID,
		HTTPClient:     httpClient,
		Log:            log,
		BaseDir:        baseDir,
	})
	if err != nil {
		return fmt.Errorf("configure: %w", err)
	}
	for _, account := range cfg.Accounts.SubAccounts {
		if !account.Enabled {
			continue
		}
		subCachePath, err := subAccountCachePath(baseDir, account)
		if err != nil {
			return fmt.Errorf("configure sub-account %q: %w", account.ID, err)
		}
		subLive, err := deps.LoadAccountToken(authCtx, subCachePath, authOut, savePersistedToken(log, deps.SaveLiveToken, subCachePath))
		if err != nil {
			return fmt.Errorf("authenticate sub-account %q: %w", account.ID, err)
		}
		subXBLSource := deps.NewXBLTokenSource(authCtx, subLive)
		subXBLClient, err := deps.NewXSAPIClient(authCtx, subXBLSource, httpClient, log.With("sub_account", account.ID))
		if err != nil {
			return fmt.Errorf("authenticate sub-account %q xbox live: %w", account.ID, err)
		}
		xblClients = append(xblClients, subXBLClient)
		runtime.SubAccounts = append(runtime.SubAccounts, broadcaster.SubAccountConfig{
			ID:             account.ID,
			Enabled:        true,
			XBLClient:      subXBLClient,
			XBLTokenSource: subXBLSource,
			XUID:           subXBLClient.UserInfo().XUID,
		})
	}

	b, err := deps.NewBroadcaster(runtime)
	if err != nil {
		return fmt.Errorf("configure broadcaster: %w", err)
	}
	if err := b.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	log.Info("broadcasting", "target", runtime.Server.Address())

	<-ctx.Done()
	if err := b.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

func defaultCommandDeps() commandDeps {
	return commandDeps{}
}

func (d commandDeps) withDefaults() commandDeps {
	if d.Stdout == nil {
		d.Stdout = os.Stdout
	}
	if d.HTTPClient == nil {
		d.HTTPClient = http.DefaultClient
	}
	if d.LoadConfig == nil {
		d.LoadConfig = broadcaster.LoadConfigFile
	}
	if d.LoadLiveToken == nil {
		d.LoadLiveToken = broadcaster.LoadLiveToken
	}
	if d.NewLiveTokenSource == nil {
		d.NewLiveTokenSource = broadcaster.NewLiveTokenSourceWithPersist
	}
	if d.SaveLiveToken == nil {
		d.SaveLiveToken = broadcaster.SaveLiveToken
	}
	if d.LoadAccountToken == nil {
		d.LoadAccountToken = loadAccountToken
	}
	if d.NewXBLTokenSource == nil {
		d.NewXBLTokenSource = broadcaster.NewXBLTokenSource
	}
	if d.NewXSAPIClient == nil {
		d.NewXSAPIClient = broadcaster.NewXSAPIClient
	}
	if d.CloseXSAPIClients == nil {
		d.CloseXSAPIClients = closeXSAPIClients
	}
	if d.NewBroadcaster == nil {
		d.NewBroadcaster = func(conf broadcaster.Config) (commandBroadcaster, error) {
			return broadcaster.New(conf)
		}
	}
	return d
}

func closeXSAPIClients(log *slog.Logger, clients []*xsapi.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, client := range clients {
		if client == nil {
			continue
		}
		if client.HTTPClient() == nil && client.MPSD() == nil && client.TokenSource() == nil {
			continue
		}
		if err := client.CloseContext(ctx); err != nil && log != nil {
			log.Warn("close xbox live client", "err", err)
		}
	}
}

func validateSubAccountCachePaths(base, primaryCachePath string, accounts []broadcaster.SubAccountFile) error {
	owners := map[string]string{primaryCachePath: "primary"}
	for _, account := range accounts {
		if !account.Enabled {
			continue
		}
		subCachePath, err := subAccountCachePath(base, account)
		if err != nil {
			return fmt.Errorf("configure sub-account %q: %w", account.ID, err)
		}
		if owner, ok := owners[subCachePath]; ok {
			return fmt.Errorf("duplicate account cache path for %q and %q: %s", owner, account.ID, subCachePath)
		}
		owners[subCachePath] = account.ID
	}
	return nil
}

func defaultCachePath() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "cache/live_token.json"
	}
	return filepath.Join(dir, "mcxboxbroadcast-go", "live_token.json")
}

func loadAccountToken(ctx context.Context, path string, out io.Writer, persist func(*oauth2.Token)) (oauth2.TokenSource, error) {
	tok, err := broadcaster.LoadLiveToken(path)
	if err != nil {
		tok = nil
	}
	src := broadcaster.NewLiveTokenSourceWithPersist(ctx, tok, out, persist)
	tok, err = src.Token()
	if err != nil {
		return nil, err
	}
	return src, broadcaster.SaveLiveToken(path, tok)
}

// savePersistedToken saves rotated tokens to the cache path, logging failures.
func savePersistedToken(log *slog.Logger, save func(string, *oauth2.Token) error, path string) func(*oauth2.Token) {
	return func(tok *oauth2.Token) {
		if save == nil {
			return
		}
		if err := save(path, tok); err != nil && log != nil {
			log.Warn("could not save rotated token cache", "path", path, "err", err)
		}
	}
}

// signInWriter tees device-code sign-in prompts to the notifier so headless
// instances surface re-authentication requests.
func signInWriter(out io.Writer, notifier broadcaster.Notifier, log *slog.Logger) io.Writer {
	if notifier == nil {
		return out
	}
	return &notifyingWriter{out: out, notifier: notifier, log: log}
}

type notifyingWriter struct {
	out      io.Writer
	notifier broadcaster.Notifier
	log      *slog.Logger
}

func (w *notifyingWriter) Write(p []byte) (int, error) {
	message := strings.TrimSpace(string(p))
	if message != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := w.notifier.Notify(ctx, message); err != nil && w.log != nil {
			w.log.Warn("send sign-in notification", "err", err)
		}
		cancel()
	}
	return w.out.Write(p)
}

func subAccountCachePath(base string, account broadcaster.SubAccountFile) (string, error) {
	if account.CachePath != "" {
		return resolveConfigPath(base, account.CachePath), nil
	}
	if account.ID == "" {
		return "", errors.New("sub-account id or cachePath is required")
	}
	return filepath.Join(base, "cache", "sub_accounts", account.ID, "live_token.json"), nil
}

func resolveConfigPath(base, path string) string {
	if path == "" {
		return defaultCachePath()
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(base, path)
}
