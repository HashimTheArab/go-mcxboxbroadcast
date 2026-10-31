package broadcaster

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestFileHistoryStoreRecordsAndClearsLastSeen(t *testing.T) {
	store := NewFileHistoryStore(filepath.Join(t.TempDir(), "player_history.json"))
	when := time.Now().Add(-time.Hour).Truncate(time.Second)

	if err := store.Seen(context.Background(), "1", when); err != nil {
		t.Fatal(err)
	}
	lastSeen, ok, err := store.LastSeen(context.Background(), "1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !lastSeen.Equal(when) {
		t.Fatalf("last seen = %s, %v", lastSeen, ok)
	}
	if err := store.Clear(context.Background(), "1"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.LastSeen(context.Background(), "1"); err != nil || ok {
		t.Fatalf("expected cleared history, ok=%v err=%v", ok, err)
	}
}
