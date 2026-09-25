package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	reconnectBackoffBase    = 5 * time.Second
	reconnectBackoffMax     = 2 * time.Minute
	sessionRecoveryAttempts = 6
	// subAccountRetryTimeout bounds each unpublished sub-account's retry, which holds b.mu like
	// targeted sub-account recovery does.
	subAccountRetryTimeout = 15 * time.Second
)

// sessionLoop owns metadata updates and full recovery so the two cannot race
// or bypass each other's backoff after a failed publication.
func (b *Broadcaster) sessionLoop() {
	ticker := time.NewTicker(b.conf.UpdateInterval)
	defer ticker.Stop()
	consecutiveFailures := 0
	for {
		b.mu.Lock()
		var signalingDone <-chan struct{}
		if b.canRecreateSignaling() && b.signaling != nil {
			signalingDone = b.signaling.Context().Done()
		}
		b.mu.Unlock()
		select {
		case <-b.ctx.Done():
			return
		case <-signalingDone:
			if !b.recoverSession("signaling connection lost") {
				return
			}
			consecutiveFailures = 0
		case <-ticker.C:
			issue := b.sessionHealthIssue()
			if issue.reason != "" && issue.subAccountID == "" {
				if b.canRecreateSignaling() {
					if !b.recoverSession(issue.reason) {
						return
					}
					consecutiveFailures = 0
					continue
				}
				b.warn("session is unhealthy but signaling is statically configured; cannot re-create", "reason", issue.reason)
			}
			err := b.refreshSession(issue)
			if err == nil {
				consecutiveFailures = 0
				continue
			}
			if b.ctx.Err() != nil {
				return
			}
			if !countsAsPrimaryUpdateFailure(err) {
				consecutiveFailures = 0
				b.log.Error("update sub-account sessions", "err", err)
				continue
			}
			consecutiveFailures++
			b.log.Error("update session", "err", err)
			if consecutiveFailures == 1 {
				b.notifySessionUpdateFailure(b.ctx, err)
			}
			if consecutiveFailures >= sessionUpdateFailureLimit {
				if !b.recoverSession("repeated session update failures") {
					return
				}
				consecutiveFailures = 0
			}
		}
	}
}

// refreshSession repairs an unhealthy sub-account, updates metadata, then
// retries unpublished sub-accounts, each with its own request budget so
// optional accounts cannot consume the primary's time.
func (b *Broadcaster) refreshSession(issue sessionHealthIssue) error {
	if issue.subAccountID != "" {
		ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
		err := b.recoverSessionHealthIssue(ctx, issue)
		cancel()
		if err != nil {
			b.log.Error("recover sub-account session health", "sub_account", issue.subAccountID, "reason", issue.reason, "err", err)
		} else {
			b.info("sub-account session recovered", "sub_account", issue.subAccountID, "reason", issue.reason)
		}
	}
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	err := b.Update(ctx)
	cancel()
	// Retries run after the primary's update so a stalled sub-account cannot delay it.
	b.retryUnpublishedSubAccounts(b.ctx)
	return err
}

// canRecreateSignaling reports whether signaling can be rebuilt by the broadcaster.
func (b *Broadcaster) canRecreateSignaling() bool {
	return b.conf.Signaling == nil
}

// recoverSession retries one recovery episode, then stops the broadcaster if
// rebuilding cannot restore service. The command can then exit for its supervisor
// to restart it with fresh authentication and client state.
func (b *Broadcaster) recoverSession(reason string) bool {
	if b.ctx.Err() != nil {
		return false
	}
	if !b.canRecreateSignaling() {
		b.warn("session is unhealthy but signaling is statically configured; cannot re-create", "reason", reason)
		return true
	}
	b.mu.Lock()
	b.recovering = true
	b.mu.Unlock()
	b.warn("re-creating xbox live session", "reason", reason)
	failures := 0
	err := retryWithBackoff(b.ctx, reconnectBackoffBase, reconnectBackoffMax, sessionRecoveryAttempts, b.recreateSession, func(err error, next time.Duration) {
		failures++
		b.log.Error("session recovery failed", "reason", reason, "attempt", failures, "err", err, "retry_in", next)
		if failures == 1 {
			b.notify(b.ctx, "Xbox session recovery failed; retrying with backoff: "+err.Error())
		}
	})
	b.mu.Lock()
	b.recovering = false
	if err != nil && b.ctx.Err() == nil {
		b.failure = fmt.Errorf("Xbox session recovery exhausted after %d attempts (%s): %w", sessionRecoveryAttempts, reason, err)
		b.cancel()
	}
	b.mu.Unlock()
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			b.log.Error("stopping broadcaster after failed recovery", "reason", reason, "err", err)
		}
		return false
	}
	b.info("xbox live session recovered", "reason", reason)
	if failures != 0 {
		b.notify(b.ctx, "Xbox session recovered.")
	}
	return true
}

// retryWithBackoff retries a failed operation up to limit times, preserving the
// last error. Cancellation stops retries, and the final failure does not announce
// another retry that will never run.
func retryWithBackoff(ctx context.Context, base, max time.Duration, limit int, attempt func() error, onError func(error, time.Duration)) error {
	delay := base
	for n := 0; n < limit; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := attempt()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if n == limit-1 {
			return err
		}
		if onError != nil {
			onError(err, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		if delay *= 2; delay > max {
			delay = max
		}
	}
	return errors.New("session recovery requires at least one attempt")
}
