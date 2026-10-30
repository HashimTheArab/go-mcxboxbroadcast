package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/HashimTheArab/go-mcxboxbroadcast"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	advertisedDesc = prometheus.NewDesc("mcxboxbroadcast_session_advertised",
		"Whether the account's own session was recently found open. Says nothing about another player's access.",
		[]string{"xuid", "gamertag", "version", "protocol"}, nil)
	friendsDesc = prometheus.NewDesc("mcxboxbroadcast_friends",
		"Friends on the account's Xbox friend list.",
		[]string{"xuid", "gamertag"}, nil)
	friendLimitDesc = prometheus.NewDesc("mcxboxbroadcast_friend_limit",
		"Xbox friend list limit.",
		[]string{"xuid", "gamertag"}, nil)
	joinsDesc = prometheus.NewDesc("mcxboxbroadcast_relayed_joins_total",
		"Join offers relayed to the target since start, labelled with the primary account.",
		[]string{"xuid", "gamertag"}, nil)
)

// healthCollector reads a fresh Health snapshot on every scrape.
type healthCollector struct {
	health func() broadcaster.Health
	now    func() time.Time
}

func (c healthCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- advertisedDesc
	ch <- friendsDesc
	ch <- friendLimitDesc
	ch <- joinsDesc
}

func (c healthCollector) Collect(ch chan<- prometheus.Metric) {
	health := c.health()
	now := c.now()
	protocol := strconv.Itoa(int(health.Protocol))
	for _, account := range health.Accounts {
		if account.XUID == "" {
			continue
		}
		advertised := 0.0
		if account.AdvertisedAt(now) {
			advertised = 1
		}
		ch <- prometheus.MustNewConstMetric(advertisedDesc, prometheus.GaugeValue, advertised, account.XUID, account.Gamertag, health.Version, protocol)
		ch <- prometheus.MustNewConstMetric(friendLimitDesc, prometheus.GaugeValue, broadcaster.MaxFriends, account.XUID, account.Gamertag)
		if !account.FriendsCheckedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(friendsDesc, prometheus.GaugeValue, float64(account.Friends), account.XUID, account.Gamertag)
		}
	}
	if len(health.Accounts) > 0 && health.Accounts[0].XUID != "" {
		primary := health.Accounts[0]
		ch <- prometheus.MustNewConstMetric(joinsDesc, prometheus.CounterValue, float64(health.RelayedJoins), primary.XUID, primary.Gamertag)
	}
}

// serveMetrics serves /metrics on listen until ctx ends. A bind failure is returned immediately.
func serveMetrics(ctx context.Context, listen string, health func() broadcaster.Health, log *slog.Logger) (func(), error) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(healthCollector{health: health, now: time.Now})
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server stopped", "err", err)
		}
	}()
	log.Info("serving metrics", "addr", listener.Addr().String())
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}, nil
}
