package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mcxboxbroadcast/broadcaster-go"
	"github.com/sandertv/gophertunnel/minecraft/auth"
)

func main() {
	var (
		host      = flag.String("host", "127.0.0.1", "target Bedrock server host")
		port      = flag.Uint("port", 19132, "target Bedrock server port")
		name      = flag.String("name", "MCXboxBroadcast", "host name shown in the friend list")
		world     = flag.String("world", "", "world name shown in the friend list")
		players   = flag.Int("players", 1, "displayed player count")
		max       = flag.Int("max-players", 20, "displayed maximum player count")
		query     = flag.Bool("query", true, "query target server status before announcements")
		cachePath = flag.String("cache", defaultCachePath(), "Microsoft Live token cache path")
		debug     = flag.Bool("debug", false, "enable debug logs")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tok, err := broadcaster.LoadLiveToken(*cachePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn("could not load token cache", "err", err)
	}
	live := auth.RefreshTokenSourceWriter(tok, os.Stdout)
	tok, err = live.Token()
	if err != nil {
		log.Error("authenticate", "err", err)
		os.Exit(1)
	}
	if err := broadcaster.SaveLiveToken(*cachePath, tok); err != nil {
		log.Warn("could not save token cache", "err", err)
	}

	worldName := *world
	if worldName == "" {
		worldName = *name
	}
	b, err := broadcaster.New(broadcaster.Config{
		TokenSource:     broadcaster.NewXBLTokenSource(ctx, live),
		LiveTokenSource: live,
		Server: broadcaster.ServerInfo{
			Host: *host,
			Port: uint16(*port),
		},
		Status: broadcaster.Status{
			HostName:      *name,
			WorldName:     worldName,
			Players:       *players,
			MaxPlayers:    *max,
			QueryTarget:   *query,
			QueryTimeout:  5 * time.Second,
			QueryFallback: true,
		},
		UpdateInterval: 30 * time.Second,
		HTTPClient:     http.DefaultClient,
		Log:            log,
	})
	if err != nil {
		log.Error("configure", "err", err)
		os.Exit(1)
	}
	if err := b.Start(ctx); err != nil {
		log.Error("start", "err", err)
		os.Exit(1)
	}
	log.Info("broadcasting", "target", fmt.Sprintf("%s:%d", *host, *port))

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
