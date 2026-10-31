package broadcaster

import (
	"context"
	"time"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

const (
	activityCheckInterval    = time.Minute
	activityGracePeriod      = 2 * time.Minute
	activityRecoveryCooldown = 10 * time.Minute
	activityMissingLimit     = 3
)

// publishedActivity identifies the account and session whose handle should exist.
type publishedActivity struct {
	xuid      string
	ref       mpsd.SessionReference
	announcer *room.XBLAnnouncer
	session   *mpsd.Session
	client    *mpsd.Client
}

// activityObservation keeps consecutive misses and recovery timing for the published session.
type activityObservation struct {
	publication  publishedActivity
	checkAfter   time.Time
	recoverAfter time.Time
	misses       int
}

// publishedActivity snapshots the publication identity without holding locks during HTTP calls.
func (b *Broadcaster) publishedActivity() (publishedActivity, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started || b.recovering {
		return publishedActivity{}, false
	}
	xuid := b.primaryXUID()
	xbl, ok := xblAnnouncer(b.announcer)
	if !ok || xuid == "" {
		return publishedActivity{}, false
	}
	xbl.Lock()
	defer xbl.Unlock()
	if xbl.Client == nil || xbl.SessionReference.Name == "" {
		return publishedActivity{}, false
	}
	return publishedActivity{xuid: xuid, ref: xbl.SessionReference, announcer: xbl, session: xbl.Session, client: xbl.Client}, true
}

// primaryActivityCurrentLocked reports whether activity still identifies the
// primary publication. The caller must hold b.mu so Update cannot replace it.
func (b *Broadcaster) primaryActivityCurrentLocked(activity *publishedActivity) bool {
	if activity == nil {
		return true
	}
	xbl, ok := xblAnnouncer(b.announcer)
	if !ok || xbl != activity.announcer {
		return false
	}
	xbl.Lock()
	defer xbl.Unlock()
	return xbl.Session == activity.session && xbl.SessionReference == activity.ref
}

// primaryActivityCurrent reports whether activity still identifies the primary
// publication without holding the broadcaster lock during later network work.
func (b *Broadcaster) primaryActivityCurrent(activity *publishedActivity) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.primaryActivityCurrentLocked(activity)
}

// activityHealthIssue checks the account's own directory handle. This detects lost
// publication, but cannot prove that another account can see or join the session.
// The session loop owns state and serializes any resulting recovery.
func (b *Broadcaster) activityHealthIssue(state *activityObservation, now time.Time) sessionHealthIssue {
	publication, ok := b.publishedActivity()
	if !ok {
		return sessionHealthIssue{}
	}
	if state.publication != publication {
		state.publication = publication
		state.misses = 0
		state.checkAfter = now.Add(activityGracePeriod)
	}
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	activities, err := publication.client.ActivitiesForUsers(ctx, publication.ref.ServiceConfigID, []string{publication.xuid})
	found := false
	for _, activity := range activities {
		if activity.OwnerXUID == publication.xuid && activity.SessionReference.Equal(publication.ref) && (activity.RelatedInfo == nil || !activity.RelatedInfo.Closed) {
			found = true
			break
		}
	}
	// Xbox may not show a new session during its grace period, so only a found handle counts then.
	// Checks run through grace and the recovery cooldown so a rotating or recovered session reads as advertised.
	settling := now.Before(state.checkAfter)
	// An external Update can replace the session while the query runs; its old result no longer applies.
	published, ok := b.publishedActivity()
	current := ok && published == publication
	if current {
		b.health.checked(err == nil && (found || !settling), found, now)
	}
	switch {
	case err != nil || found:
		state.misses = 0
		if err != nil && b.ctx.Err() == nil {
			b.debug("check published activity", "err", err)
		}
	case settling || now.Before(state.recoverAfter):
		// Misses count only after the grace period and the recovery cooldown.
	default:
		state.misses++
	}
	if state.misses < activityMissingLimit {
		return sessionHealthIssue{}
	}
	// Discard a replaced session's result rather than replacing the new session again.
	if !current {
		return sessionHealthIssue{}
	}
	state.misses = 0
	state.recoverAfter = now.Add(activityRecoveryCooldown)
	return sessionHealthIssue{reason: "published activity handle missing or closed", activity: &publication}
}
