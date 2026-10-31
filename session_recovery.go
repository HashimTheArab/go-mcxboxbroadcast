package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/room"
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
	var activity activityObservation
	b.activityHealthIssue(&activity, time.Now())
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
			issue = b.activityHealthIssue(&activity, time.Now())
			if issue.reason == "" {
				continue
			}
		case <-ticker.C:
			b.closeDrainedSessions()
			issue = b.sessionHealthIssue()
		case <-b.healthCheck:
			issue = b.sessionHealthIssue()
			if issue.reason == "" {
				continue
			}
		}
		if issue.rotate {
			if b.rotate(issue) {
				consecutiveFailures = 0
				continue
			}
			if b.ctx.Err() != nil {
				return
			}
			issue.reason = "session rotation failed"
		}
		if issue.reason != "" {
			if issue.activity != nil && !b.canRecreateSignaling() {
				b.reportStaticActivityRecoveryFailure(issue)
				continue
			}
			if b.canRecreateSignaling() {
				if !b.recoverSession(issue) {
					return
				}
				consecutiveFailures = 0
				continue
			}
			b.log.Warn("session is unhealthy but signaling is statically configured; cannot re-create", "reason", issue.reason)
		}
		err := b.refreshSession()
		if err == nil {
			consecutiveFailures = 0
			continue
		}
		if b.ctx.Err() != nil {
			return
		}
		consecutiveFailures++
		b.log.Error("update session", "err", err)
		if consecutiveFailures == 1 {
			b.notify(b.ctx, "Xbox session update failed: "+err.Error())
		}
		if consecutiveFailures >= sessionUpdateFailureLimit {
			if !b.recoverSession(sessionHealthIssue{reason: "repeated session update failures"}) {
				return
			}
			consecutiveFailures = 0
		}
	}
}

// rotate reports whether the session rotated; on failure the caller falls
// back to full recreation, as Java restarts.
func (b *Broadcaster) rotate(issue sessionHealthIssue) bool {
	b.log.Info("rotating session", "reason", issue.reason)
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	if err := b.rotateSession(ctx); err != nil {
		b.log.Error("rotate session; re-creating instead", "err", err)
		return false
	}
	b.log.Info("rotated session")
	return true
}

// refreshSession updates the session metadata under its own request budget.
func (b *Broadcaster) refreshSession() error {
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	return b.Update(ctx)
}

// canRecreateSignaling reports whether signaling can be rebuilt by the broadcaster.
func (b *Broadcaster) canRecreateSignaling() bool {
	return b.conf.Signaling == nil
}

// reportStaticActivityRecoveryFailure reports an unrecoverable activity only
// if the probed publication is still current.
func (b *Broadcaster) reportStaticActivityRecoveryFailure(issue sessionHealthIssue) {
	if !b.activityCurrent(issue.activity) {
		return
	}
	err := errors.New("cannot recover the Xbox activity with Config.Signaling; provide SignalingFactory to enable session recovery")
	b.log.Error("activity recovery failed", "reason", issue.reason, "err", err)
	b.notify(b.ctx, err.Error())
}

// recoverSession retries one recovery episode, then stops the broadcaster if
// rebuilding cannot restore service. The command can then exit for its supervisor
// to restart it with fresh authentication and client state.
func (b *Broadcaster) recoverSession(issue sessionHealthIssue) bool {
	if b.ctx.Err() != nil {
		return false
	}
	if !b.canRecreateSignaling() {
		b.log.Warn("session is unhealthy but signaling is statically configured; cannot re-create", "reason", issue.reason)
		return true
	}
	b.mu.Lock()
	if issue.activity != nil && !b.activityCurrentLocked(issue.activity) {
		b.mu.Unlock()
		return true
	}
	b.recovering = true
	b.mu.Unlock()
	b.log.Warn("re-creating xbox live session", "reason", issue.reason)
	failures := 0
	err := retryWithBackoff(b.ctx, reconnectBackoffBase, reconnectBackoffMax, sessionRecoveryAttempts, b.recreateSession, func(err error, next time.Duration) {
		failures++
		b.log.Error("session recovery failed", "reason", issue.reason, "attempt", failures, "err", err, "retry_in", next)
		if failures == 1 {
			b.notify(b.ctx, "Xbox session recovery failed; retrying with backoff: "+err.Error())
		}
	})
	b.mu.Lock()
	b.recovering = false
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
	b.log.Info("xbox live session recovered", "reason", issue.reason)
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

const (
	sessionMemberCap = 30 // members the MinecraftLobby template allows
	// sessionRotateMembers rotates before sessionMemberCap so joins in
	// progress still fit; players stay members while they play.
	sessionRotateMembers = 28
	// sessionRotateGrace keeps a rotated-out session joinable for players mid-join.
	sessionRotateGrace = 30 * time.Second
	// sessionUpdateFailureLimit is how many consecutive update failures
	// trigger a full session recreation.
	sessionUpdateFailureLimit = 3
)

type sessionHealthIssue struct {
	reason   string
	activity *publishedActivity
	rotate   bool // the session is nearly full rather than broken
}

// sessionHealthIssue reports why the current session needs rotation or
// recovery, or an empty issue when it is healthy.
func (b *Broadcaster) sessionHealthIssue() sessionHealthIssue {
	b.mu.Lock()
	defer b.mu.Unlock()
	counter, ok := unwrapAnnouncer(b.announcer).(memberCounter)
	if !ok {
		return sessionHealthIssue{}
	}
	count, published, lost := counter.memberCount()
	switch {
	case !published:
		return sessionHealthIssue{}
	case lost:
		return sessionHealthIssue{reason: "mpsd session lost"}
	case count >= sessionRotateMembers:
		return sessionHealthIssue{reason: fmt.Sprintf("session has %d/%d members", count, sessionMemberCap), rotate: true}
	}
	return sessionHealthIssue{}
}

// membersChanged requests an early health check once a session nears its member cap.
func (b *Broadcaster) membersChanged(count int) {
	if count < sessionRotateMembers {
		return
	}
	select {
	case b.healthCheck <- struct{}{}:
	default:
	}
}

// rotateSession moves the activity to a fresh session on the same signaling
// connection and retires the old one, like the Java rotation.
func (b *Broadcaster) rotateSession(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started || b.recovering {
		return errors.New("broadcaster is not running")
	}
	announcer, err := b.publishSession(ctx, "")
	if err != nil {
		return err
	}
	b.retire(b.announcer)
	b.announcer = announcer
	return nil
}

// retiredSession is a rotated-out session and when it was retired.
type retiredSession struct {
	announcer room.Announcer
	at        time.Time
}

// retire keeps a rotated-out session for players mid-join, then hides it if
// configured. Its members stay so they keep playing. The caller must hold b.mu.
func (b *Broadcaster) retire(announcer room.Announcer) {
	b.retiredSessions = append(b.retiredSessions, retiredSession{announcer: announcer, at: time.Now()})
	if !b.conf.HideOldSessions {
		return
	}
	hider, ok := unwrapAnnouncer(announcer).(sessionHider)
	if !ok {
		return
	}
	ctx, log := b.ctx, b.log
	go func() {
		select {
		case <-time.After(sessionRotateGrace):
		case <-ctx.Done():
			return
		}
		hideCtx, cancel := xboxOperationContext(ctx)
		defer cancel()
		if err := hider.hide(hideCtx); err != nil {
			log.Warn("hide old session", "err", err)
			return
		}
		log.Debug("hid old session")
	}()
}

// closeDrainedSessions closes retired sessions past their grace period that
// only the bot still belongs to, so they stop holding RTA subscriptions.
func (b *Broadcaster) closeDrainedSessions() {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.retiredSessions[:0]
	for _, retired := range b.retiredSessions {
		counter, ok := unwrapAnnouncer(retired.announcer).(memberCounter)
		count, published := 0, false
		if ok {
			count, published, _ = counter.memberCount()
		}
		if !published || count > 1 || time.Since(retired.at) < sessionRotateGrace {
			kept = append(kept, retired)
			continue
		}
		if err := retired.announcer.Close(); err != nil {
			b.log.Warn("close drained session", "err", err)
			kept = append(kept, retired)
			continue
		}
		b.log.Debug("closed drained session")
	}
	clear(b.retiredSessions[len(kept):])
	b.retiredSessions = kept
}

// recreateSession tears down and rebuilds signaling, sessions and the relay after a drop.
func (b *Broadcaster) recreateSession() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.ctx.Err() != nil || !b.started {
		return errors.New("broadcaster is shut down")
	}
	_ = b.releaseSessions()

	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		return fmt.Errorf("re-create signaling: %w", err)
	}
	if sig.Context().Err() != nil {
		_ = closeSignaling(sig)
		return errors.New("re-create signaling: factory returned signaling with dead context")
	}
	b.signaling = sig
	b.log.Debug("nethernet signaling re-created", "network_id", sig.NetworkID())

	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	if err := b.publishOn(ctx, sig); err != nil {
		_ = closeSignaling(sig)
		b.signaling = nil
		return fmt.Errorf("re-create session: %w", err)
	}
	b.log.Debug("join relay restarted", "network_id", sig.NetworkID(), "target", b.conf.JoinURL)

	// Re-showcase the gallery image so a swapped file does not require a
	// process restart; the upload no-ops when the image is already set.
	go b.uploadGallery()
	return nil
}
