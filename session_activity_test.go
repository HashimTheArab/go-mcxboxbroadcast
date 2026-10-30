package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

// activityTestBroadcaster uses the real MPSD HTTP adapter with a controlled directory response.
func activityTestBroadcaster(t *testing.T, respond func(*http.Request) (*http.Response, error)) *Broadcaster {
	t.Helper()
	client := mpsd.New(&http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost || req.URL.Path != "/handles/query" {
			t.Fatalf("unexpected activity request: %s %s", req.Method, req.URL)
		}
		var body struct {
			Owners struct {
				XUIDs  []string `json:"xuids"`
				People any      `json:"people"`
			} `json:"owners"`
		}
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Owners.XUIDs) != 1 || body.Owners.XUIDs[0] != "123" || body.Owners.People != nil {
			t.Fatalf("query must select only the publishing owner: %+v", body.Owners)
		}
		response, err := respond(req)
		if response != nil {
			response.Request = req
		}
		return response, err
	})}, nil, xsts.UserInfo{XUID: "123"}, testBroadcasterLogger())
	return &Broadcaster{
		ctx: context.Background(), log: testBroadcasterLogger(), started: true,
		conf:      Config{XUID: "123", UpdateInterval: time.Hour},
		announcer: &room.XBLAnnouncer{Client: client, SessionReference: mpsd.SessionReference{ServiceConfigID: serviceConfigUUID, TemplateName: TemplateName, Name: "CURRENT"}},
	}
}

// activityResponse serializes a handle with the supplied owner and reference.
func activityResponse(t *testing.T, owner string, ref mpsd.SessionReference) *http.Response {
	t.Helper()
	data, err := json.Marshal(map[string]any{"results": []any{map[string]any{"ownerXuid": owner, "sessionRef": ref}}})
	if err != nil {
		t.Fatal(err)
	}
	return broadcasterResponse(http.StatusOK, string(data))
}

func TestActivityHealthRequiresConsecutiveConfirmedMisses(t *testing.T) {
	var result string
	calls := 0
	var b *Broadcaster
	b = activityTestBroadcaster(t, func(req *http.Request) (*http.Response, error) {
		calls++
		if !b.mu.TryLock() {
			t.Fatal("directory request holds broadcaster mutex")
		}
		b.mu.Unlock()
		deadline, ok := req.Context().Deadline()
		if !ok || time.Until(deadline) > 15*time.Second {
			t.Fatal("directory request lacks bounded context")
		}
		ref := b.announcer.(*room.XBLAnnouncer).SessionReference
		switch result {
		case "error":
			return broadcasterResponse(http.StatusForbidden, "forbidden"), nil
		case "healthy":
			ref.Name = strings.ToLower(ref.Name)
			ref.TemplateName = strings.ToLower(ref.TemplateName)
			return activityResponse(t, "123", ref), nil
		case "stale":
			ref.Name = "OLD"
			return activityResponse(t, "123", ref), nil
		case "wrong owner":
			return activityResponse(t, "456", ref), nil
		case "closed":
			data, err := json.Marshal(map[string]any{"results": []any{map[string]any{"ownerXuid": "123", "sessionRef": ref, "relatedInfo": map[string]bool{"closed": true}}}})
			if err != nil {
				t.Fatal(err)
			}
			return broadcasterResponse(http.StatusOK, string(data)), nil
		default:
			return broadcasterResponse(http.StatusOK, `{"results":[]}`), nil
		}
	})
	var state activityObservation
	now := time.Now()
	b.activityHealthIssue(&state, now)
	b.activityHealthIssue(&state, now.Add(time.Minute))
	if calls != 0 {
		t.Fatal("queried directory during startup grace period")
	}
	// Errors and healthy handles break the run; stale or another owner's handles do not satisfy it.
	for i, response := range []string{"missing", "missing", "error", "missing", "healthy", "wrong owner", "healthy", "closed", "stale", "wrong owner"} {
		result = response
		issue := b.activityHealthIssue(&state, now.Add(time.Duration(i+2)*time.Minute))
		if (issue.reason != "") != (i == 9) {
			t.Fatalf("response %d (%s) gave issue %+v", i, response, issue)
		}
	}
	queries := calls
	b.activityHealthIssue(&state, now.Add(12*time.Minute))
	if calls != queries {
		t.Fatal("queried again during recovery cooldown")
	}
	old := b.announcer.(*room.XBLAnnouncer)
	b.announcer = &room.XBLAnnouncer{Client: old.Client, SessionReference: old.SessionReference}
	for minute := 13; minute <= 20; minute++ {
		b.activityHealthIssue(&state, now.Add(time.Duration(minute)*time.Minute))
	}
	if calls != queries {
		t.Fatal("replacing the announcer bypassed the recovery cooldown")
	}
	for minute := 21; minute <= 23; minute++ {
		issue := b.activityHealthIssue(&state, now.Add(time.Duration(minute)*time.Minute))
		if (issue.reason != "") != (minute == 23) {
			t.Fatalf("minute %d after cooldown gave issue %+v", minute, issue)
		}
	}
}

func TestActivityHealthResetsForReplacementAndDiscardsOldQuery(t *testing.T) {
	var b *Broadcaster
	replaceDuringQuery := false
	b = activityTestBroadcaster(t, func(*http.Request) (*http.Response, error) {
		if replaceDuringQuery {
			old := b.announcer.(*room.XBLAnnouncer)
			b.mu.Lock()
			b.announcer = &room.XBLAnnouncer{Client: old.Client, SessionReference: old.SessionReference}
			b.mu.Unlock()
		}
		return broadcasterResponse(http.StatusOK, `{"results":[]}`), nil
	})
	var state activityObservation
	now := time.Now()
	for i := range 4 {
		b.activityHealthIssue(&state, now.Add(time.Duration(i)*time.Minute))
	}
	replaceDuringQuery = true
	if issue := b.activityHealthIssue(&state, now.Add(4*time.Minute)); issue.reason != "" {
		t.Fatal("old response requested recovery of replacement session")
	}
	replaceDuringQuery = false
	for i := 5; i < 9; i++ {
		if issue := b.activityHealthIssue(&state, now.Add(time.Duration(i)*time.Minute)); issue.reason != "" {
			t.Fatalf("replacement recovered before grace and three misses at minute %d", i)
		}
	}
	if issue := b.activityHealthIssue(&state, now.Add(9*time.Minute)); issue.reason == "" {
		t.Fatal("missing replacement did not recover after fresh observations")
	}
}

func TestSessionLoopRecoversMissingPublishedActivity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := activityTestBroadcaster(t, func(*http.Request) (*http.Response, error) {
			return broadcasterResponse(http.StatusOK, `{"results":[]}`), nil
		})
		b.ctx, b.cancel = context.WithCancel(context.Background())
		defer b.cancel()
		start := time.Now()
		rebuilds := 0
		b.conf.SignalingFactory = func(context.Context, Config) (nethernet.Signaling, error) {
			rebuilds++
			b.cancel()
			return nil, errors.New("stop after proving full recovery was requested")
		}
		b.sessionLoop()
		if rebuilds != 1 || time.Since(start) != 4*time.Minute {
			t.Fatalf("rebuilds=%d elapsed=%v, want recovery after grace and three missing observations", rebuilds, time.Since(start))
		}
	})
}

func TestSessionLoopReportsMissingActivityWithStaticSignaling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := activityTestBroadcaster(t, func(*http.Request) (*http.Response, error) {
			return broadcasterResponse(http.StatusOK, `{"results":[]}`), nil
		})
		b.ctx, b.cancel = context.WithCancel(context.Background())
		defer b.cancel()
		sig := &fakeSignaling{}
		b.conf.Signaling, b.signaling = sig, sig
		var notices []time.Duration
		start := time.Now()
		b.conf.Notifier = fakeNotifier{notify: func(_ context.Context, message string) {
			if !strings.Contains(message, "cannot recover") || !strings.Contains(message, "SignalingFactory") {
				t.Fatalf("missing explicit recovery failure: %s", message)
			}
			notices = append(notices, time.Since(start))
			if len(notices) == 2 {
				b.cancel()
			}
		}}
		b.sessionLoop()
		if len(notices) != 2 || notices[0] != 4*time.Minute || notices[1] != 16*time.Minute {
			t.Fatalf("recovery failure times = %v, want 4m and 16m with cooldown", notices)
		}
		if b.signaling != sig || sig.closed {
			t.Fatal("activity recovery replaced or closed the injected signaling")
		}
	})
}

func TestActivityProbeTimeoutBreaksMissStreak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := activityTestBroadcaster(t, func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		})
		var state activityObservation
		b.activityHealthIssue(&state, time.Now())
		state.misses = 2
		state.checkAfter = time.Now()
		start := time.Now()
		if issue := b.activityHealthIssue(&state, start); issue.reason != "" || state.misses != 0 {
			t.Fatal("timeout counted as a confirmed missing handle")
		}
		if elapsed := time.Since(start); elapsed != 15*time.Second {
			t.Fatalf("probe took %v, want 15s request budget", elapsed)
		}
	})
}

func TestActivityRecoveryIgnoresReplacedPrimaryAccount(t *testing.T) {
	b := activityTestBroadcaster(t, nil)
	publication, _ := b.publishedActivity()
	rebuilds := 0
	b.conf.SignalingFactory = func(context.Context, Config) (nethernet.Signaling, error) {
		rebuilds++
		return nil, errors.New("unexpected recovery")
	}
	b.announcer = &room.XBLAnnouncer{Client: publication.client, SessionReference: publication.ref}
	issue := sessionHealthIssue{reason: "published activity handle missing", activity: &publication}
	if !b.recoverSession(issue) {
		t.Fatal("stale activity issue stopped the session loop")
	}
	if rebuilds != 0 || b.recovering {
		t.Fatalf("stale activity issue started recovery: rebuilds=%d recovering=%v", rebuilds, b.recovering)
	}
}

func TestStaticActivityFailureIgnoresReplacedPrimaryAccount(t *testing.T) {
	b := activityTestBroadcaster(t, nil)
	publication, _ := b.publishedActivity()
	b.conf.Signaling = &fakeSignaling{}
	notices := 0
	b.conf.Notifier = fakeNotifier{notify: func(context.Context, string) { notices++ }}
	b.announcer = &room.XBLAnnouncer{Client: publication.client, SessionReference: publication.ref}
	b.reportStaticActivityRecoveryFailure(sessionHealthIssue{reason: "published activity handle missing", activity: &publication})
	if notices != 0 {
		t.Fatalf("stale activity issue sent %d recovery failure notifications", notices)
	}
}
