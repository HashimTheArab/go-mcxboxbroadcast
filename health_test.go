package broadcaster

import (
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
	b.markPublished()
	check("published", http.StatusOK, http.StatusOK)
	b.recovering.Store(true)
	check("recovering", http.StatusOK, http.StatusServiceUnavailable)
	b.recovering.Store(false)
	b.lastPublished.Store(time.Now().Add(-b.healthStaleAfter() - time.Second).UnixNano())
	check("stale", http.StatusServiceUnavailable, http.StatusServiceUnavailable)
}
