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
// /healthz fails once no primary session publication has succeeded for
// several update intervals; /readyz also fails while starting or recovering.
func (b *Broadcaster) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if problem := b.staleness(time.Now()); problem != "" {
			http.Error(w, problem, http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		problem := b.staleness(time.Now())
		switch {
		case problem != "":
		case b.lastPublished.Load() == 0:
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

// staleness describes why the last primary publication is too old, or returns
// "" when it is recent or none has been attempted yet.
func (b *Broadcaster) staleness(now time.Time) string {
	last := b.lastPublished.Load()
	if last == 0 {
		return ""
	}
	age := now.Sub(time.Unix(0, last))
	if age <= b.healthStaleAfter() {
		return ""
	}
	return fmt.Sprintf("no successful xbox live session update for %s", age.Round(time.Second))
}

// healthStaleAfter is how long the primary session may go without a
// successful publication before liveness fails.
func (b *Broadcaster) healthStaleAfter() time.Duration {
	return max(minHealthStaleAfter, 5*b.conf.UpdateInterval)
}
