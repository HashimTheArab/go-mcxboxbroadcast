package broadcaster

import (
	"context"
	"log/slog"
	"time"

	"github.com/df-mc/go-xsapi/v2/presence"
)

const defaultPresenceHeartbeat = 300 * time.Second

// updatePresence marks the account active, which keeps its session visible, and returns
// when Xbox wants the next heartbeat.
func updatePresence(ctx context.Context, client *presence.Client) (time.Duration, error) {
	ctx, cancel := xboxOperationContext(ctx)
	defer cancel()
	result, err := client.Update(ctx, presence.TitleRequest{State: presence.StateActive})
	if err != nil {
		return defaultPresenceHeartbeat, err
	}
	if result.HeartbeatAfter <= 0 {
		return defaultPresenceHeartbeat, nil
	}
	return result.HeartbeatAfter, nil
}

// runPresence sends presence heartbeats until ctx ends.
func runPresence(ctx context.Context, client *presence.Client, log *slog.Logger) {
	for {
		heartbeat, err := updatePresence(ctx, client)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("update presence", "err", err)
		} else {
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
