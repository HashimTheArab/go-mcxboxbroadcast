package broadcaster

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/df-mc/go-xsapi"
	"github.com/sandertv/gophertunnel/minecraft/service"
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

type staticMinecraftTokenSource struct{}

func (staticMinecraftTokenSource) Token() (*service.Token, error) {
	return &service.Token{AuthorizationHeader: "Bearer minecraft", ValidUntil: time.Now().Add(time.Hour)}, nil
}

func testImageFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "screenshot.jpg")
	if err := os.WriteFile(path, []byte("fake image bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
