package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/sandertv/gophertunnel/minecraft/auth"
	"golang.org/x/oauth2"
)

func main() {
	var (
		configPath = flag.String("config", "config.yml", "configuration file path")
		debug      = flag.Bool("debug", false, "enable debug logs")
	)
	flag.Parse()

	cfg, err := broadcaster.LoadConfigFile(*configPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	level := slog.LevelInfo
	if *debug || cfg.DebugMode {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	baseDir := filepath.Dir(*configPath)
	cachePath := resolveConfigPath(baseDir, cfg.Accounts.PrimaryCachePath)
	tok, err := broadcaster.LoadLiveToken(cachePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn("could not load token cache", "err", err)
	}
	live := auth.RefreshTokenSourceWriter(tok, os.Stdout)
	tok, err = live.Token()
	if err != nil {
		log.Error("authenticate", "err", err)
		os.Exit(1)
	}
	if err := broadcaster.SaveLiveToken(cachePath, tok); err != nil {
		log.Warn("could not save token cache", "err", err)
	}

	runtime, err := cfg.RuntimeConfig(broadcaster.RuntimeConfigInput{
		TokenSource:     broadcaster.NewXBLTokenSource(ctx, live),
		LiveTokenSource: live,
		HTTPClient:      http.DefaultClient,
		Log:             log,
		BaseDir:         baseDir,
	})
	if err != nil {
		log.Error("configure", "err", err)
		os.Exit(1)
	}
	accountCachePaths := map[string]string{cachePath: "primary"}
	for _, account := range cfg.Accounts.SubAccounts {
		if !account.Enabled {
			continue
		}
		subCachePath, err := subAccountCachePath(baseDir, account)
		if err != nil {
			log.Error("configure sub-account", "id", account.ID, "err", err)
			os.Exit(1)
		}
		if owner, ok := accountCachePaths[subCachePath]; ok {
			log.Error("duplicate account cache path", "id", account.ID, "owner", owner, "path", subCachePath)
			os.Exit(1)
		}
		accountCachePaths[subCachePath] = account.ID
		subLive, err := loadAccountToken(subCachePath, os.Stdout)
		if err != nil {
			log.Error("authenticate sub-account", "id", account.ID, "err", err)
			os.Exit(1)
		}
		runtime.SubAccounts = append(runtime.SubAccounts, broadcaster.SubAccountConfig{
			ID:          account.ID,
			Enabled:     true,
			TokenSource: broadcaster.NewXBLTokenSource(ctx, subLive),
		})
	}

	b, err := broadcaster.New(runtime)
	if err != nil {
		log.Error("configure", "err", err)
		os.Exit(1)
	}
	if err := b.Start(ctx); err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	log.Info("broadcasting", "target", runtime.Server.Address())

	<-ctx.Done()
	if err := b.Close(); err != nil {
		log.Error("close", "err", err)
		os.Exit(1)
	}
}

func defaultCachePath() string {
	dir, err := os.UserCacheDir()
	if err != nil {
		return "cache/live_token.json"
	}
	return filepath.Join(dir, "mcxboxbroadcast-go", "live_token.json")
}

func loadAccountToken(path string, out *os.File) (oauth2.TokenSource, error) {
	tok, err := broadcaster.LoadLiveToken(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	src := auth.RefreshTokenSourceWriter(tok, out)
	tok, err = src.Token()
	if err != nil {
		return nil, err
	}
	return src, broadcaster.SaveLiveToken(path, tok)
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
