package broadcaster

import "github.com/google/uuid"

const (
	ServiceConfigID = "4fc10100-5f7a-4470-899b-280835760c07"
	TemplateName    = "MinecraftLobby"
	TitleID         = 896928775

	// XboxFriendLimit is the most friends Xbox Live allows one account.
	XboxFriendLimit = 1000

	xboxLiveRelyingParty = "http://xboxlive.com"
)

var serviceConfigUUID = uuid.MustParse(ServiceConfigID)
