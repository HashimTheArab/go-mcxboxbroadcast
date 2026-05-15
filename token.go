package broadcaster

import (
	"context"
	"fmt"
	"net/http"

	"github.com/df-mc/go-xsapi"
	"github.com/sandertv/gophertunnel/minecraft/auth"
	"golang.org/x/oauth2"
)

// NewXBLTokenSource adapts a Microsoft Live token source into an xsapi token
// source for Xbox Live session-directory calls.
func NewXBLTokenSource(ctx context.Context, live oauth2.TokenSource) xsapi.TokenSource {
	if ctx == nil {
		ctx = context.Background()
	}
	return &xblTokenSource{ctx: ctx, src: live}
}

type xblTokenSource struct {
	ctx context.Context
	src oauth2.TokenSource
}

func (x *xblTokenSource) Token() (xsapi.Token, error) {
	tok, err := x.src.Token()
	if err != nil {
		return nil, fmt.Errorf("request live token: %w", err)
	}
	xbl, err := auth.RequestXBLToken(x.ctx, tok, "http://xboxlive.com")
	if err != nil {
		return nil, fmt.Errorf("request xbl token: %w", err)
	}
	return xblToken{xbl}, nil
}

type xblToken struct {
	*auth.XBLToken
}

func (t xblToken) SetAuthHeader(req *http.Request) {
	req.Header.Set("Authorization", t.String())
}

func (t xblToken) DisplayClaims() xsapi.DisplayClaims {
	if len(t.AuthorizationToken.DisplayClaims.UserInfo) == 0 {
		return xsapi.DisplayClaims{}
	}
	claim := t.AuthorizationToken.DisplayClaims.UserInfo[0]
	return xsapi.DisplayClaims{
		GamerTag: claim.GamerTag,
		XUID:     claim.XUID,
		UserHash: claim.UserHash,
	}
}

func (t xblToken) String() string {
	claim := t.DisplayClaims()
	return fmt.Sprintf("XBL3.0 x=%s;%s", claim.UserHash, t.AuthorizationToken.Token)
}
