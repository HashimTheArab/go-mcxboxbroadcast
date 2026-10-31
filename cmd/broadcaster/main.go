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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/xal/sisu"
	"github.com/sandertv/gophertunnel/minecraft/auth"
	"golang.org/x/oauth2"
)

type commandOptions struct {
	ConfigPath string
	Debug      bool
}

type commandBroadcaster interface {
	Start(context.Context) error
	Wait() error
	Close() error
	Health() broadcaster.Health
}

type commandDeps struct {
	Stdout             io.Writer
	HTTPClient         *http.Client
	LoadConfig         func(string) (broadcaster.ConfigFile, error)
	LoadLiveToken      func(string) (*oauth2.Token, error)
	NewLiveTokenSource func(context.Context, *oauth2.Token, io.Writer, func(*oauth2.Token)) oauth2.TokenSource
	SaveLiveToken      func(string, *oauth2.Token) error
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
		log := slog.New(newLogHandler(os.Stderr, slog.LevelInfo, shouldColorLogs(os.Stderr)))
		if code, ok := errors.AsType[sisu.ErrorCode](err); ok && code == sisu.ErrorCodeAgeVerificationRequired {
			log.Error("authentication failed: complete Xbox/Microsoft age verification for this account, then retry")
		} else {
			log.Error("broadcaster", "err", err)
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
		authCtx = auth.WithContextClient(authCtx, httpClient)
	}
	level := slog.LevelInfo
	if opts.Debug || cfg.DebugMode {
		level = slog.LevelDebug
	}
	log := slog.New(newLogHandler(deps.Stdout, level, shouldColorLogs(deps.Stdout)))
	log.Info("starting go-mcxboxbroadcast", "config", opts.ConfigPath, "debug", level == slog.LevelDebug)
	log.Debug("debug logging enabled")
	for _, note := range cfg.Notes {
		log.Warn("config adjusted", "note", note)
	}
	// Metrics bind before sign-in, so a stuck or failing login shows as a reachable target with no series.
	var running atomic.Pointer[commandBroadcaster]
	if listen := strings.TrimSpace(cfg.Metrics.Listen); listen != "" {
		stopMetrics, err := serveMetrics(ctx, listen, func() broadcaster.Health {
			if b := running.Load(); b != nil {
				return (*b).Health()
			}
			return broadcaster.Health{}
		}, log)
		if err != nil {
			return fmt.Errorf("serve metrics: %w", err)
		}
		defer stopMetrics()
	}
	var xblClients []*xsapi.Client
	defer func() {
		deps.CloseXSAPIClients(log, xblClients)
	}()

	baseDir, err := filepath.Abs(filepath.Dir(opts.ConfigPath))
	if err != nil {
		return fmt.Errorf("resolve config directory: %w", err)
	}
	cachePath := resolveConfigPath(baseDir, cfg.Accounts.PrimaryCachePath)

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

	b, err := deps.NewBroadcaster(runtime)
	if err != nil {
		return fmt.Errorf("configure broadcaster: %w", err)
	}
	running.Store(&b)
	if err := b.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	runErr := b.Wait()
	closeErr := b.Close()
	if runErr != nil {
		runErr = fmt.Errorf("run: %w", runErr)
	}
	if closeErr != nil {
		closeErr = fmt.Errorf("close: %w", closeErr)
	}
	return errors.Join(runErr, closeErr)
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

func defaultCachePath() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "cache/live_token.json"
	}
	return filepath.Join(dir, "mcxboxbroadcast-go", "live_token.json")
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

// resolveConfigPath returns a clean path so equivalent spellings compare equal.
func resolveConfigPath(base, path string) string {
	if path == "" {
		return defaultCachePath()
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(base, path)
}
