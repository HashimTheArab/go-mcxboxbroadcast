package broadcaster

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/df-mc/go-nethernet"
)

func TestWaitConcurrentWithStart(t *testing.T) {
	startErr := errors.New("signaling startup failed")
	for range 100 {
		b := &Broadcaster{
			log: testBroadcasterLogger(),
			conf: Config{SignalingFactory: func(context.Context, Config) (nethernet.Signaling, error) {
				return nil, startErr
			}},
		}
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := b.Start(t.Context()); !errors.Is(err, startErr) {
				t.Errorf("Start() = %v, want startup failure", err)
			}
		})
		wg.Go(func() {
			if err := b.Wait(); err != nil {
				t.Errorf("Wait() = %v, want no runtime recovery failure", err)
			}
		})
		wg.Wait()
	}
}
