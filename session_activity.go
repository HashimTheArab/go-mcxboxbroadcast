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
	id        string
	xuid      string
	ref       mpsd.SessionReference
	announcer *room.XBLAnnouncer
	session   *mpsd.Session
	client    *mpsd.Client
}

// activityObservation keeps consecutive misses and recovery timing for one account.
type activityObservation struct {
	publication  publishedActivity
	checkAfter   time.Time
	recoverAfter time.Time
	misses       int
}

// publishedActivities snapshots publication identities without holding locks during HTTP calls.
func (b *Broadcaster) publishedActivities() []publishedActivity {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started || b.recovering {
		return nil
	}
	var publications []publishedActivity
	add := func(id, xuid string, announcer room.Announcer) {
		xbl, ok := xblAnnouncer(announcer)
		if !ok || xuid == "" {
			return
		}
		xbl.Lock()
		defer xbl.Unlock()
		if xbl.Client != nil && xbl.SessionReference.Name != "" {
			publications = append(publications, publishedActivity{id: id, xuid: xuid, ref: xbl.SessionReference, announcer: xbl, session: xbl.Session, client: xbl.Client})
		}
	}
	add("", b.primaryXUID(), b.announcer)
	for _, sub := range b.subAnnouncers {
		add(sub.id, sub.xuid, sub.announcer)
	}
	return publications
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

// activityHealthIssue checks each account's own directory handle. This detects lost
// publication, but cannot prove that another account can see or join the session.
// The session loop owns observations and serializes any resulting recovery.
func (b *Broadcaster) activityHealthIssue(observations map[string]activityObservation, now time.Time) sessionHealthIssue {
	publications := b.publishedActivities()
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	for _, publication := range publications {
		state := observations[publication.id]
		if state.publication != publication {
			state.publication = publication
			state.misses = 0
			state.checkAfter = now.Add(activityGracePeriod)
		}
		if now.Before(state.checkAfter) || now.Before(state.recoverAfter) {
			observations[publication.id] = state
			continue
		}
		activities, err := publication.client.ActivitiesForUsers(ctx, publication.ref.ServiceConfigID, []string{publication.xuid})
		found := false
		for _, activity := range activities {
			if activity.OwnerXUID == publication.xuid && activity.SessionReference.Equal(publication.ref) && (activity.RelatedInfo == nil || !activity.RelatedInfo.Closed) {
				found = true
				break
			}
		}
		if err != nil || found {
			state.misses = 0
			if err != nil && b.ctx.Err() == nil {
				b.debug("check published activity", "sub_account", publication.id, "err", err)
			}
		} else {
			state.misses++
		}
		observations[publication.id] = state
		if state.misses < activityMissingLimit {
			continue
		}
		// An external Update can replace a sub-account while the query runs.
		// Discard its old result rather than replacing the new session again.
		for _, current := range b.publishedActivities() {
			if current == publication {
				state.misses = 0
				state.recoverAfter = now.Add(activityRecoveryCooldown)
				observations[publication.id] = state
				return sessionHealthIssue{reason: "published activity handle missing or closed", subAccountID: publication.id, activity: &publication}
			}
		}
	}
	return sessionHealthIssue{}
}
