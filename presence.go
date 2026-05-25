package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/df-mc/go-xsapi"
)

const (
	presenceStateActive      = `{"state":"active"}`
	defaultPresenceHeartbeat = 300 * time.Second
)

// PresenceClient updates Xbox user presence so the broadcaster account remains
// visible as active while its MPSD session is published.
type PresenceClient struct {
	TokenSource xsapi.TokenSource
	Client      *http.Client
}

func (c PresenceClient) Update(ctx context.Context) (time.Duration, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.TokenSource == nil {
		return defaultPresenceHeartbeat, errors.New("token source is nil")
	}
	tok, err := c.TokenSource.Token()
	if err != nil {
		return defaultPresenceHeartbeat, fmt.Errorf("request token: %w", err)
	}
	xuid := tok.DisplayClaims().XUID
	if xuid == "" {
		return defaultPresenceHeartbeat, errors.New("xuid is empty")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, presenceURL(xuid), strings.NewReader(presenceStateActive))
	if err != nil {
		return defaultPresenceHeartbeat, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Xbl-Contract-Version", "3")
	tok.SetAuthHeader(req)

	resp, err := c.client().Do(req)
	if err != nil {
		return defaultPresenceHeartbeat, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return defaultPresenceHeartbeat, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	return heartbeatAfter(resp.Header.Get("X-Heartbeat-After")), nil
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

func presenceURL(xuid string) string {
	return fmt.Sprintf("https://userpresence.xboxlive.com/users/xuid(%s)/devices/current/titles/current", xuid)
}

func heartbeatAfter(header string) time.Duration {
	seconds, err := strconv.Atoi(header)
	if err != nil || seconds <= 0 {
		return defaultPresenceHeartbeat
	}
	return time.Duration(seconds) * time.Second
}
