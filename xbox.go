package broadcaster

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	ServiceConfigID = "4fc10100-5f7a-4470-899b-280835760c07"
	TemplateName    = "MinecraftLobby"
	TitleID         = 896928775
)

var serviceConfigUUID = uuid.MustParse(ServiceConfigID)

const defaultXboxOperationTimeout = 15 * time.Second

// xboxOperationContext bounds one background Xbox API operation without
// imposing the same deadline on an entire sync pass or broadcaster lifetime.
func xboxOperationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, defaultXboxOperationTimeout)
}
