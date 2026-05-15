package main

import (
	"path/filepath"
	"testing"

	broadcaster "github.com/HashimTheArab/go-mcxboxbroadcast"
)

func TestSubAccountCachePathUsesExplicitPath(t *testing.T) {
	path, err := subAccountCachePath("/base", broadcaster.SubAccountFile{
		ID:        "alt",
		CachePath: "cache/alt.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join("/base", "cache", "alt.json") {
		t.Fatalf("unexpected path %q", path)
	}
}

func TestSubAccountCachePathDerivesFromID(t *testing.T) {
	path, err := subAccountCachePath("/base", broadcaster.SubAccountFile{ID: "alt"})
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join("/base", "cache", "sub_accounts", "alt", "live_token.json")
	if path != want {
		t.Fatalf("unexpected path %q", path)
	}
}

func TestSubAccountCachePathRequiresIDWhenPathOmitted(t *testing.T) {
	if _, err := subAccountCachePath("/base", broadcaster.SubAccountFile{}); err == nil {
		t.Fatal("expected error")
	}
}
