package broadcaster

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/df-mc/go-xsapi/v2/presence"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

const (
	defaultPresenceHeartbeat = 300 * time.Second
)

// PresenceClient updates Xbox user presence so the broadcaster account remains
// visible as active while its MPSD session is published.
type PresenceClient struct {
	XUID   string
	Client *http.Client
}

func (c PresenceClient) Update(ctx context.Context) (time.Duration, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.XUID == "" {
		return defaultPresenceHeartbeat, errors.New("xuid is empty")
	}
	heartbeat := defaultPresenceHeartbeat
	presenceClient := presence.New(c.clientWithHeartbeat(&heartbeat), xsts.UserInfo{XUID: c.XUID})
	if err := presenceClient.Update(ctx, presence.TitleRequest{State: presence.StateActive}); err != nil {
		return defaultPresenceHeartbeat, err
	}
	return heartbeat, nil
}

func (c PresenceClient) Run(ctx context.Context, log *slog.Logger) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		heartbeat, err := c.Update(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if log != nil {
				log.Error("update presence", "err", err)
			}
		} else if log != nil {
			log.Debug("presence updated", "next_update", heartbeat)
		}
		timer := time.NewTimer(heartbeat)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func (c PresenceClient) client() *http.Client {
	if c.Client != nil {
		return c.Client
	}
	return http.DefaultClient
}

func (c PresenceClient) clientWithHeartbeat(heartbeat *time.Duration) *http.Client {
	base := c.client()
	client := new(http.Client)
	*client = *base
	client.Transport = heartbeatTransport{base: base.Transport, heartbeat: heartbeat}
	return client
}

type heartbeatTransport struct {
	base      http.RoundTripper
	heartbeat *time.Duration
}

func (t heartbeatTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	resp, err := base.RoundTrip(req)
	if err == nil && resp != nil && t.heartbeat != nil {
		*t.heartbeat = heartbeatAfter(resp.Header.Get("X-Heartbeat-After"))
	}
	return resp, err
}

func heartbeatAfter(header string) time.Duration {
	seconds, err := strconv.Atoi(header)
	if err != nil || seconds <= 0 {
		return defaultPresenceHeartbeat
	}
	return time.Duration(seconds) * time.Second
}
