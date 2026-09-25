package broadcaster

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"golang.org/x/oauth2"
)

// Saving must replace a loosely-permissioned cache with an owner-only file and leave no temp files.
func TestSaveLiveTokenReplacesFileOwnerOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live_token.json")
	if err := os.WriteFile(path, []byte(`{"access_token":"old"}`), 0o660); err != nil {
		t.Fatal(err)
	}
	if err := SaveLiveToken(path, &oauth2.Token{AccessToken: "new", RefreshToken: "refresh"}); err != nil {
		t.Fatal(err)
	}
	tok, err := LoadLiveToken(path)
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "new" || tok.RefreshToken != "refresh" {
		t.Fatalf("loaded token %+v", tok)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); runtime.GOOS != "windows" && perm != 0o600 {
		t.Fatalf("token cache mode = %v, want 0600", perm)
	}
	assertOnlyFiles(t, dir, "live_token.json")
}

// A failed save must keep the previous cache intact and clean up its temp file.
func TestSaveLiveTokenFailureKeepsPreviousCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "live_token.json")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := SaveLiveToken(path, &oauth2.Token{AccessToken: "new"}); err == nil {
		t.Fatal("expected replacing a directory to fail")
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("previous entry was disturbed: %v", err)
	}
	assertOnlyFiles(t, dir, "live_token.json")
}

func assertOnlyFiles(t *testing.T, dir string, want ...string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(want) {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("directory holds %v, want %v", names, want)
	}
}
