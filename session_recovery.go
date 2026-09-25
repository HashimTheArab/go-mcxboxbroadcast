package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	reconnectBackoffBase    = 5 * time.Second
	reconnectBackoffMax     = 2 * time.Minute
	sessionRecoveryAttempts = 6
)

// sessionLoop owns metadata updates and full recovery so the two cannot race
// or bypass each other's backoff after a failed publication.
func (b *Broadcaster) sessionLoop() {
	ticker := time.NewTicker(b.conf.UpdateInterval)
	defer ticker.Stop()
	activityTicker := time.NewTicker(activityCheckInterval)
	defer activityTicker.Stop()
	activities := make(map[string]activityObservation)
	b.activityHealthIssue(activities, time.Now())
	consecutiveFailures := 0
	for {
		b.mu.Lock()
		var signalingDone <-chan struct{}
		if b.canRecreateSignaling() && b.signaling != nil {
			signalingDone = b.signaling.Context().Done()
		}
		b.mu.Unlock()
		var issue sessionHealthIssue
		select {
		case <-b.ctx.Done():
			return
		case <-signalingDone:
			if !b.recoverSession(sessionHealthIssue{reason: "signaling connection lost"}) {
				return
			}
			consecutiveFailures = 0
			continue
		case <-activityTicker.C:
			issue = b.activityHealthIssue(activities, time.Now())
			if issue.reason == "" {
				continue
			}
		case <-ticker.C:
			issue = b.sessionHealthIssue()
			if err := b.retryStaleSessionCloses(); err != nil {
				b.warn("retry closing replaced xbox live sessions", "err", err)
			}
		}
		if issue.reason != "" && issue.subAccountID == "" && !b.republishPrimarySession(issue) {
			if b.canRecreateSignaling() {
				if !b.recoverSession(issue) {
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
			if !b.recoverSession(sessionHealthIssue{reason: "repeated session update failures"}) {
				return
			}
			consecutiveFailures = 0
		}
	}
}

// republishPrimarySession discards the primary MPSD session named by issue so
// the following Update publishes a new one over the existing signaling and
// listener. It reports false when the announcer cannot republish.
func (b *Broadcaster) republishPrimarySession(issue sessionHealthIssue) bool {
	b.mu.Lock()
	announcer, ok := nonceAnnouncer(b.announcer)
	b.mu.Unlock()
	if !ok {
		return false
	}
	session := announcer.session()
	if issue.activity != nil {
		if issue.activity.announcer != announcer.XBLAnnouncer {
			return true
		}
		session = issue.activity.session
	}
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	discarded, err := announcer.discardSession(ctx, session)
	if err != nil {
		b.log.Error("discard unhealthy xbox live session", "reason", issue.reason, "err", err)
		return true
	}
	if !discarded {
		return true
	}
	b.warn("republishing xbox live session", "reason", issue.reason)
	if err := session.Close(); err != nil {
		b.warn("close unhealthy xbox live session", "err", err)
		b.retainStaleSessions([]io.Closer{session})
	}
	return true
}

// refreshSession repairs an unhealthy sub-account, then updates metadata with
// its own request budget so an optional account cannot consume the primary's time.
func (b *Broadcaster) refreshSession(issue sessionHealthIssue) error {
	if issue.subAccountID != "" {
		ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
		err := b.recoverSessionHealthIssue(ctx, issue)
		cancel()
		if err != nil {
			b.log.Error("recover sub-account session health", "sub_account", issue.subAccountID, "reason", issue.reason, "err", err)
		} else {
			b.info("sub-account session recovered", "sub_account", issue.subAccountID, "reason", issue.reason)
			return nil
		}
	}
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	return b.Update(ctx)
}

// canRecreateSignaling reports whether signaling can be rebuilt by the broadcaster.
func (b *Broadcaster) canRecreateSignaling() bool {
	return b.conf.Signaling == nil
}

// recoverSession retries one recovery episode, then stops the broadcaster if
// rebuilding cannot restore service. The command can then exit for its supervisor
// to restart it with fresh authentication and client state.
func (b *Broadcaster) recoverSession(issue sessionHealthIssue) bool {
	if b.ctx.Err() != nil {
		return false
	}
	if !b.canRecreateSignaling() {
		b.warn("session is unhealthy but signaling is statically configured; cannot re-create", "reason", issue.reason)
		return true
	}
	b.mu.Lock()
	if issue.activity != nil && !b.primaryActivityCurrentLocked(issue.activity) {
		b.mu.Unlock()
		return true
	}
	b.recovering.Store(true)
	b.mu.Unlock()
	b.warn("re-creating xbox live session", "reason", issue.reason)
	failures := 0
	err := retryWithBackoff(b.ctx, reconnectBackoffBase, reconnectBackoffMax, sessionRecoveryAttempts, b.recreateSession, func(err error, next time.Duration) {
		failures++
		b.log.Error("session recovery failed", "reason", issue.reason, "attempt", failures, "err", err, "retry_in", next)
		if failures == 1 {
			b.notify(b.ctx, "Xbox session recovery failed; retrying with backoff: "+err.Error())
		}
	})
	b.mu.Lock()
	b.recovering.Store(false)
	if err != nil && b.ctx.Err() == nil {
		b.failure = fmt.Errorf("Xbox session recovery exhausted after %d attempts (%s): %w", sessionRecoveryAttempts, issue.reason, err)
		b.cancel()
	}
	b.mu.Unlock()
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			b.log.Error("stopping broadcaster after failed recovery", "reason", issue.reason, "err", err)
		}
		return false
	}
	b.info("xbox live session recovered", "reason", issue.reason)
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
