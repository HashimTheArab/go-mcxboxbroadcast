package broadcaster

import (
	"net/http"

	"github.com/df-mc/go-xsapi"
)

type staticTokenSource struct{}

func (staticTokenSource) Token() (xsapi.Token, error) {
	return staticToken{}, nil
}

type staticToken struct{}

func (staticToken) SetAuthHeader(req *http.Request) {
	req.Header.Set("Authorization", "XBL3.0 x=user;token")
}

func (staticToken) String() string { return "XBL3.0 x=user;token" }

func (staticToken) DisplayClaims() xsapi.DisplayClaims {
	return xsapi.DisplayClaims{GamerTag: "Tester", XUID: "1", UserHash: "user"}
}
