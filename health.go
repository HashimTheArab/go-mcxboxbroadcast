package broadcaster

import (
	"fmt"
	"net/http"
	"time"
)

// minHealthStaleAfter keeps short update intervals from failing liveness
// during one ordinary recovery episode.
const minHealthStaleAfter = 5 * time.Minute

// HealthHandler returns an http.Handler serving Kubernetes-style probes.
// /healthz fails once Xbox has not confirmed the primary session for several
// update intervals, or after the broadcaster stops; /readyz also fails while
// starting or recovering.
func (b *Broadcaster) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if problem := b.unhealthy(time.Now()); problem != "" {
			http.Error(w, problem, http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		problem := b.unhealthy(time.Now())
		switch {
		case problem != "":
		case b.lastVerified.Load() == 0:
			problem = "starting"
		case b.recovering.Load():
			problem = "recovering the xbox live session"
		}
		if problem != "" {
			http.Error(w, problem, http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	return mux
}

// unhealthy describes why the broadcaster is stopped or its session is
// unconfirmed for too long, or returns "" while starting or healthy.
func (b *Broadcaster) unhealthy(now time.Time) string {
	b.cancelMu.Lock()
	ctx := b.ctx
	b.cancelMu.Unlock()
	if ctx != nil && ctx.Err() != nil {
		return "stopped"
	}
	last := b.lastVerified.Load()
	if last == 0 {
		return ""
	}
	age := now.Sub(time.Unix(0, last))
	if age <= b.healthStaleAfter() {
		return ""
	}
	return fmt.Sprintf("xbox live has not confirmed the session for %s", age.Round(time.Second))
}

// healthStaleAfter is how long the primary session may go unconfirmed
// before liveness fails.
func (b *Broadcaster) healthStaleAfter() time.Duration {
	return max(minHealthStaleAfter, 5*b.conf.UpdateInterval)
}
