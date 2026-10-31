package broadcaster

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestSessionLoopStopsAfterRepeatedPublicationFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publishErr := errors.New("write activity handle: 403 Forbidden")
		lost, lose := context.WithCancel(context.Background())
		lose()
		var attempts []time.Duration
		var notices []string
		start := time.Now()
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: &cancelableSignaling{ctx: lost, networkID: "123"},
			conf: Config{
				XUID:           "123",
				JoinURL:        "http://127.0.0.1:19132",
				Status:         Status{HostName: "Host", WorldName: "World"},
				UpdateInterval: 30 * time.Second,
				SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
					attempts = append(attempts, time.Since(start))
					return &fakeSignaling{}, nil
				},
				Notifier: fakeNotifier{notify: func(_ context.Context, message string) {
					notices = append(notices, message)
				}},
			},
			announcerFactory: func(*Broadcaster) room.Announcer {
				return &fakeAnnouncer{announceErr: publishErr}
			},
		}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		defer b.cancel()
		done := make(chan struct{})
		go func() {
			b.sessionLoop()
			close(done)
		}()
		<-done
		if err := b.Wait(); !errors.Is(err, publishErr) {
			t.Fatalf("Wait() = %v, want publication failure", err)
		}
		want := []time.Duration{0, 5 * time.Second, 15 * time.Second, 35 * time.Second, 75 * time.Second, 155 * time.Second}
		if !reflect.DeepEqual(attempts, want) {
			t.Fatalf("rebuild times = %v, want %v; update ticks must not bypass recovery backoff", attempts, want)
		}
		if len(notices) != 1 || !strings.Contains(notices[0], "403 Forbidden") {
			t.Fatalf("notices = %v, want one recovery failure notice", notices)
		}
		// Failed recovery has no relay. Close must still release its announcer.
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestRetryWithBackoffRecoversTransientFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts, notices := 0, 0
		err := retryWithBackoff(context.Background(), time.Second, time.Minute, sessionRecoveryAttempts, func() error {
			attempts++
			if attempts < 3 {
				return errors.New("temporary failure")
			}
			return nil
		}, func(error, time.Duration) { notices++ })
		if err != nil || attempts != 3 || notices != 2 {
			t.Fatalf("err=%v attempts=%d notices=%d", err, attempts, notices)
		}
	})
}

func TestSessionLoopCancellationDoesNotBecomeRecoveryFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost, lose := context.WithCancel(context.Background())
		lose()
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: &cancelableSignaling{ctx: lost},
			conf: Config{UpdateInterval: time.Minute, SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				return nil, errors.New("temporary failure")
			}},
		}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { b.sessionLoop(); close(done) }()
		synctest.Wait()
		b.cancel()
		<-done
		if err := b.Wait(); err != nil {
			t.Fatalf("normal cancellation returned %v", err)
		}
	})
}

func TestSessionLoopDoesNotRebuildStaticSignaling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lost, lose := context.WithCancel(context.Background())
		lose()
		sig := &cancelableSignaling{ctx: lost}
		b := &Broadcaster{
			log:       testBroadcasterLogger(),
			started:   true,
			signaling: sig,
			announcer: &fakeAnnouncer{},
			conf:      Config{Signaling: sig, UpdateInterval: time.Minute},
		}
		b.ctx, b.cancel = context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { b.sessionLoop(); close(done) }()
		synctest.Wait()
		b.cancel()
		<-done
		if b.signaling != sig || b.failure != nil {
			t.Fatal("static signaling was rebuilt or treated as terminal failure")
		}
	})
}
