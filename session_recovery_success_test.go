package broadcaster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestBroadcasterRecoversSuccessiveSignalingDrops(t *testing.T) {
	var signals []*cancelableSignaling
	var disconnects []context.CancelFunc
	for i := range 3 {
		ctx, cancel := context.WithCancel(t.Context())
		t.Cleanup(cancel)
		signals = append(signals, &cancelableSignaling{ctx: ctx, networkID: fmt.Sprint(123456789 + i)})
		disconnects = append(disconnects, cancel)
	}
	var calls atomic.Int32
	recovered := make(chan struct{}, 3)
	announcers := make(chan *fakeAnnouncer, 3)
	status := &recoveryStatusProvider{}
	status.value.Store(&room.Status{HostName: "Host", WorldName: "World"})
	b := &Broadcaster{
		log: slog.New(recoveryLogHandler{Handler: slog.NewTextHandler(io.Discard, nil), recovered: recovered}),
		conf: Config{
			Server:               ServerInfo{Host: "127.0.0.1", Port: 19132},
			XUID:                 "123",
			MinecraftTokenSource: minecraftTokenSourceWithPMID{pmid: uuid.New()},
			ListenConfig:         minecraft.ListenConfig{AuthenticationDisabled: true},
			StatusProvider:       status,
			UpdateInterval:       time.Hour,
			SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				i := int(calls.Add(1)) - 1
				if i >= len(signals) {
					return nil, fmt.Errorf("unexpected signaling factory call %d", i+1)
				}
				return signals[i], nil
			},
		},
		announcerFactory: func(*Broadcaster) room.Announcer {
			announcer := &fakeAnnouncer{}
			announcers <- announcer
			return announcer
		},
	}
	if err := b.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeRecoveredBroadcaster(t, b) })
	previousAnnouncer := <-announcers
	for i := range 2 {
		b.mu.Lock()
		previousListener := b.listener
		b.mu.Unlock()
		disconnects[i]()
		select {
		case <-recovered:
		case <-time.After(5 * time.Second):
			t.Fatalf("signaling drop %d did not recover", i+1)
		}
		b.mu.Lock()
		listener, signaling, recovering := b.listener, b.signaling, b.recovering
		b.mu.Unlock()
		if listener == nil || listener == previousListener {
			t.Fatalf("recovery %d did not replace the listener", i+1)
		}
		if signaling != signals[i+1] || signaling.Context().Err() != nil || recovering {
			t.Fatalf("recovery %d did not finish with the replacement signaling", i+1)
		}
		if got := calls.Load(); got != int32(i+2) {
			t.Fatalf("signaling factory calls = %d, want %d", got, i+2)
		}
		if !previousAnnouncer.Closed() {
			t.Fatalf("recovery %d left the old session open", i+1)
		}
		currentAnnouncer := <-announcers
		worldName := fmt.Sprintf("Recovered world %d", i+1)
		status.value.Store(&room.Status{HostName: "Host", WorldName: worldName})
		if err := b.Update(t.Context()); err != nil {
			t.Fatalf("update after recovery %d: %v", i+1, err)
		}
		announced := currentAnnouncer.Status()
		if announced.WorldName != worldName {
			t.Fatalf("updated world = %q, want %q", announced.WorldName, worldName)
		}
		if len(announced.SupportedConnections) != 1 || string(announced.SupportedConnections[0].NetherNetID) != signals[i+1].networkID {
			t.Fatalf("recovery %d advertised stale signaling: %+v", i+1, announced.SupportedConnections)
		}
		previousAnnouncer = currentAnnouncer
	}
}

type recoveryStatusProvider struct {
	value atomic.Pointer[room.Status]
}

// RoomStatus safely reads metadata shared by the test and live listener.
func (p *recoveryStatusProvider) RoomStatus() room.Status { return *p.value.Load() }

type recoveryLogHandler struct {
	slog.Handler
	recovered chan<- struct{}
}

// Handle signals when recovery has installed its listener and cleared its state.
func (h recoveryLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "xbox live session recovered" {
		select {
		case h.recovered <- struct{}{}:
		default:
		}
	}
	return h.Handler.Handle(ctx, record)
}

// closeRecoveredBroadcaster verifies that recovered accept loops shut down cleanly.
func closeRecoveredBroadcaster(t *testing.T, b *Broadcaster) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		if err := b.Close(); err != nil {
			done <- err
			return
		}
		done <- b.Wait()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("shutdown after recovery: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("shutdown after recovery did not finish")
	}
}
