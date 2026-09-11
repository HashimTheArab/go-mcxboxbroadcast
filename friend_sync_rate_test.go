package broadcaster

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

func TestFriendSyncReadRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		name, url, retry string
		status           int
		delay            time.Duration
		initialReads     int
	}{
		{"pending", pendingRequestsURL, "45", 429, 45 * time.Second, 1},
		{"followers", peopleHubFollowersURL, "45", 429, 45 * time.Second, 2},
		{"following", peopleHubSocialURL, "45", 429, 45 * time.Second, 3},
		{"no retry header", peopleHubFollowersURL, "", 429, 20 * time.Second, 2},
		{"short retry header", pendingRequestsURL, "5", 429, 20 * time.Second, 1},
		{"service unavailable", pendingRequestsURL, "30", 503, 30 * time.Second, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				reads := make(chan timedFriendRead, 64)
				failed := false
				client := newFriendRateClient(reads, func(req *http.Request) *http.Response {
					if req.URL.String() == tc.url && !failed {
						failed = true
						resp := response(tc.status, "")
						resp.Header.Set("Retry-After", tc.retry)
						return resp
					}
					return response(200, `{"people":[]}`)
				})
				trigger := make(chan struct{}, 16)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				syncer := FriendSyncer{Client: client, Trigger: trigger,
					Config: FriendSyncConfig{AutoFollow: true, UpdateInterval: time.Hour}}
				go syncer.Run(ctx)
				synctest.Wait()
				assertFriendRateReads(t, reads, 0, friendRateReadURLs[:tc.initialReads]...)
				for range cap(trigger) {
					trigger <- struct{}{}
				}
				synctest.Wait()
				time.Sleep(tc.delay - time.Nanosecond)
				synctest.Wait()
				assertFriendRateReads(t, reads, 0)
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assertFriendRateReads(t, reads, tc.delay, friendRateReadURLs...)
				time.Sleep(20 * time.Second)
				synctest.Wait()
				assertFriendRateReads(t, reads, 0)
			})
		})
	}
}

func TestFriendSyncReadRetryRunsWithoutAnotherTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reads := make(chan timedFriendRead, 16)
		first := true
		client := newFriendRateClient(reads, func(*http.Request) *http.Response {
			if first {
				first = false
				resp := response(429, "")
				resp.Header.Set("Retry-After", "40")
				return resp
			}
			return response(200, `{"people":[]}`)
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go (FriendSyncer{Client: client, Config: FriendSyncConfig{
			AutoFollow: true, UpdateInterval: time.Hour}}).Run(ctx)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0, pendingRequestsURL)
		time.Sleep(40 * time.Second)
		synctest.Wait()
		assertFriendRateReads(t, reads, 40*time.Second, friendRateReadURLs...)
	})
}

func TestFriendSyncTriggerSpacingStartsAfterPassCompletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reads := make(chan timedFriendRead, 64)
		delayed := false
		client := newFriendRateClient(reads, func(req *http.Request) *http.Response {
			if req.URL.String() == peopleHubSocialURL && !delayed {
				delayed = true
				time.Sleep(5 * time.Second)
			}
			return response(200, `{"people":[]}`)
		})
		trigger := make(chan struct{}, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go (FriendSyncer{Client: client, Trigger: trigger, Config: FriendSyncConfig{
			AutoFollow: true, UpdateInterval: time.Hour}}).Run(ctx)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0, friendRateReadURLs...)
		for range cap(trigger) {
			trigger <- struct{}{}
		}
		time.Sleep(5 * time.Second)
		synctest.Wait()
		time.Sleep(20*time.Second - time.Nanosecond)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0)
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assertFriendRateReads(t, reads, 25*time.Second, friendRateReadURLs...)
		time.Sleep(20 * time.Second)
		synctest.Wait()
		assertFriendRateReads(t, reads, 0)
		trigger <- struct{}{}
		synctest.Wait()
		assertFriendRateReads(t, reads, 45*time.Second, friendRateReadURLs...)
		trigger <- struct{}{}
		synctest.Wait()
		close(trigger)
		cancel()
		synctest.Wait()
		time.Sleep(time.Minute)
		assertFriendRateReads(t, reads, 0)
	})
}

func TestFriendSyncCoalescesPollExpiryAndTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		passes := make(chan timedFriendRead, 16)
		expiries := make(chan timedFriendRead, 16)
		start := time.Now()
		client := &syncFriendClient{people: []Person{{XUID: "123", IsFollowedByCaller: true, IsFollowingCaller: true}},
			accept: func(context.Context) ([]Person, error) {
				passes <- timedFriendRead{"pass", time.Since(start)}
				return nil, nil
			}}
		trigger := make(chan struct{}, 16)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go (FriendSyncer{Client: client, Trigger: trigger, History: &friendRateHistory{start, expiries},
			Config: FriendSyncConfig{AutoFollow: true, ExpiryEnabled: true,
				UpdateInterval: 20 * time.Second, ExpiryCheck: 20 * time.Second}}).Run(ctx)
		synctest.Wait()
		assertFriendRateReads(t, passes, 0, "pass")
		assertFriendRateReads(t, expiries, 0, "expiry")
		time.Sleep(19 * time.Second)
		for range cap(trigger) {
			trigger <- struct{}{}
		}
		synctest.Wait()
		assertFriendRateReads(t, passes, 0)
		for _, at := range []time.Duration{20 * time.Second, 40 * time.Second} {
			time.Sleep(at - time.Since(start))
			synctest.Wait()
			assertFriendRateReads(t, passes, at, "pass")
			assertFriendRateReads(t, expiries, at, "expiry")
		}
	})
}

// friendRateReadURLs lists the three reads in an empty auto-follow pass.
var friendRateReadURLs = []string{pendingRequestsURL, peopleHubFollowersURL, peopleHubSocialURL}

// timedFriendRead records the request and elapsed time at the transport boundary.
type timedFriendRead struct {
	url string
	at  time.Duration
}

// newFriendRateClient keeps the real social API/error parsing with a local transport.
func newFriendRateClient(reads chan<- timedFriendRead, respond func(*http.Request) *http.Response) FriendClient {
	start := time.Now()
	return FriendClient{Social: newTestSocialClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		reads <- timedFriendRead{req.URL.String(), time.Since(start)}
		resp := respond(req)
		resp.Request = req
		return resp, nil
	})})}
}

// assertFriendRateReads checks all requests since the previous assertion, including timing.
func assertFriendRateReads(t *testing.T, reads <-chan timedFriendRead, at time.Duration, urls ...string) {
	t.Helper()
	for _, url := range urls {
		select {
		case got := <-reads:
			if got.url != url || got.at != at {
				t.Fatalf("request = %+v, want %s at %s", got, url, at)
			}
		default:
			t.Fatalf("missing request %s at %s", url, at)
		}
	}
	select {
	case extra := <-reads:
		t.Fatalf("unexpected request %+v", extra)
	default:
	}
}

// friendRateHistory records expiry scans while keeping the test friend active.
type friendRateHistory struct {
	start time.Time
	reads chan<- timedFriendRead
}

// LastSeen records that expiry ran and returns a recent visit so no removal is needed.
func (h *friendRateHistory) LastSeen(context.Context, string) (time.Time, bool, error) {
	h.reads <- timedFriendRead{"expiry", time.Since(h.start)}
	return time.Now(), true, nil
}

// Clear satisfies HistoryStore; the active test friend never needs removal.
func (h *friendRateHistory) Clear(context.Context, string) error { return nil }
