package broadcaster

import (
	"context"
	"time"
)

const defaultXboxOperationTimeout = 15 * time.Second

// xboxOperationContext bounds one background Xbox API operation without
// imposing the same deadline on an entire sync pass or broadcaster lifetime.
func xboxOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, defaultXboxOperationTimeout)
}
