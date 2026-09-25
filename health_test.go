package broadcaster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Liveness must fail once publications stop succeeding, and readiness also
// while starting or recovering.
func TestHealthHandlerReportsSessionFreshness(t *testing.T) {
	b := &Broadcaster{conf: Config{UpdateInterval: 30 * time.Second}}
	handler := b.HealthHandler()
	probe := func(path string) int {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}
	check := func(state string, healthz, readyz int) {
		t.Helper()
		if got := probe("/healthz"); got != healthz {
			t.Fatalf("%s: /healthz = %d, want %d", state, got, healthz)
		}
		if got := probe("/readyz"); got != readyz {
			t.Fatalf("%s: /readyz = %d, want %d", state, got, readyz)
		}
	}

	check("starting", http.StatusOK, http.StatusServiceUnavailable)
	b.markVerified()
	check("published", http.StatusOK, http.StatusOK)
	b.recovering.Store(true)
	check("recovering", http.StatusOK, http.StatusServiceUnavailable)
	b.recovering.Store(false)
	b.lastVerified.Store(time.Now().Add(-b.healthStaleAfter() - time.Second).UnixNano())
	check("stale", http.StatusServiceUnavailable, http.StatusServiceUnavailable)

	b.markVerified()
	b.ctx, b.cancel = context.WithCancel(t.Context())
	check("running", http.StatusOK, http.StatusOK)
	b.cancel()
	check("stopped", http.StatusServiceUnavailable, http.StatusServiceUnavailable)
}

// An Update served from the announcer's cache must not count as confirmed
// when Xbox cannot be reached to verify the session.
func TestUpdateOnlyCountsVerifiedSession(t *testing.T) {
	f := newFakeXbox(t)
	b, _ := newFakeXboxBroadcaster(t, f)
	b.lastVerified.Store(1)
	f.mu.Lock()
	f.failGets = true
	f.mu.Unlock()
	if err := b.Update(t.Context()); err == nil {
		t.Fatal("Update succeeded without verifying the session")
	}
	if got := b.lastVerified.Load(); got != 1 {
		t.Fatal("unverified cached Update refreshed the health timestamp")
	}

	f.mu.Lock()
	f.failGets = false
	f.mu.Unlock()
	if err := b.Update(t.Context()); err != nil {
		t.Fatal(err)
	}
	if b.lastVerified.Load() == 1 {
		t.Fatal("verified Update did not refresh the health timestamp")
	}
}

// A session Xbox deleted is found by the next Update, even without an RTA
// notification, and closed so the health check republishes it.
func TestUpdateClosesSessionDeletedByXbox(t *testing.T) {
	f := newFakeXbox(t)
	b, nonce := newFakeXboxBroadcaster(t, f)
	// Settle the joined member's nonce so the next Update writes nothing.
	if err := b.Update(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.deleted = true
	f.mu.Unlock()
	if err := b.Update(t.Context()); err == nil {
		t.Fatal("Update succeeded for a session Xbox deleted")
	}
	if nonce.session().Context().Err() == nil {
		t.Fatal("deleted session was left open")
	}
	if issue := b.sessionHealthIssue(); issue.reason != "mpsd session lost" {
		t.Fatalf("health issue = %+v, want lost MPSD session", issue)
	}
}
