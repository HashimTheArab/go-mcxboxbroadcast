package broadcaster

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/df-mc/go-xsapi/v2/presence"
)

const (
	defaultPresenceHeartbeat = 300 * time.Second
)

// PresenceClient updates Xbox user presence so the broadcaster account remains
// visible as active while its MPSD session is published.
type PresenceClient struct {
	XUID     string
	Presence *presence.Client
}

func (c PresenceClient) Update(ctx context.Context) (time.Duration, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.Presence == nil {
		return defaultPresenceHeartbeat, errors.New("presence client is nil")
	}
	operationCtx, cancel := xboxOperationContext(ctx)
	result, err := c.Presence.Update(operationCtx, presence.TitleRequest{State: presence.StateActive})
	cancel()
	if err != nil {
		return defaultPresenceHeartbeat, err
	}
	if result.HeartbeatAfter <= 0 {
		return defaultPresenceHeartbeat, nil
	}
	return result.HeartbeatAfter, nil
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
