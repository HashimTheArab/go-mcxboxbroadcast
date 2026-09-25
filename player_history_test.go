package broadcaster

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFileHistoryStoreTracksSeesAndForgets(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "player_history.json")
	store := NewFileHistoryStore(path)
	tracked := time.Now().Add(-time.Hour).Truncate(time.Second)
	joined := tracked.Add(30 * time.Minute)

	if err := store.Track(ctx, "me", tracked, "1", "2"); err != nil {
		t.Fatal(err)
	}
	if err := store.Track(ctx, "me", joined, "1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Seen(ctx, "2", joined); err != nil {
		t.Fatal(err)
	}
	if err := store.Forget(ctx, "me", "1"); err != nil {
		t.Fatal(err)
	}
	// A fresh store reads what the first one saved.
	lastSeen, err := NewFileHistoryStore(path).LastSeen(ctx, "me")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := lastSeen["1"]; ok || len(lastSeen) != 1 || !lastSeen["2"].Equal(joined) {
		t.Fatalf("last seen = %v, want only 2 at %s", lastSeen, joined)
	}
}

// One account's removals and pruning must not reset or delete another's entries.
func TestFileHistoryStoreKeepsAccountsSeparate(t *testing.T) {
	ctx := context.Background()
	store := NewFileHistoryStore(filepath.Join(t.TempDir(), "player_history.json"))
	old := time.Now().Add(-10 * 24 * time.Hour).Truncate(time.Second)
	for _, account := range []string{"primary", "sub"} {
		if err := store.Track(ctx, account, old, "friend"); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Forget(ctx, "primary", "friend"); err != nil {
		t.Fatal(err)
	}
	sub, err := store.LastSeen(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	if !sub["friend"].Equal(old) {
		t.Fatalf("sub-account entry = %v, want untouched %s", sub["friend"], old)
	}
	// A join only refreshes accounts that still track the player.
	joined := time.Now().Truncate(time.Second)
	if err := store.Seen(ctx, "friend", joined); err != nil {
		t.Fatal(err)
	}
	primary, _ := store.LastSeen(ctx, "primary")
	sub, _ = store.LastSeen(ctx, "sub")
	if _, ok := primary["friend"]; ok || !sub["friend"].Equal(joined) {
		t.Fatalf("after join primary=%v sub=%v, want only sub refreshed", primary, sub)
	}
}

// History written before entries were keyed by account keeps every clock.
func TestFileHistoryStoreMigratesFlatHistory(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "player_history.json")
	if err := os.WriteFile(path, []byte(`{"1": 1700000000, "2": 1700000100}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewFileHistoryStore(path)
	if err := store.Forget(ctx, "primary", "2"); err != nil {
		t.Fatal(err)
	}
	for account, want := range map[string]int{"primary": 1, "sub": 2} {
		lastSeen, err := store.LastSeen(ctx, account)
		if err != nil {
			t.Fatal(err)
		}
		if len(lastSeen) != want || lastSeen["1"].Unix() != 1700000000 {
			t.Fatalf("%s history = %v, want %d entries keeping the old clock", account, lastSeen, want)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"accounts"`) {
		t.Fatalf("saved history is not keyed by account:\n%s", data)
	}
}

// A corrupt file is moved aside instead of failing every lookup forever.
func TestFileHistoryStoreQuarantinesCorruptFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "player_history.json")
	if err := os.WriteFile(path, []byte(`{"accounts": {`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewFileHistoryStore(path)
	if lastSeen, err := store.LastSeen(ctx, "me"); err != nil || len(lastSeen) != 0 {
		t.Fatalf("LastSeen() = %v, %v; want empty history", lastSeen, err)
	}
	if err := store.Track(ctx, "me", time.Now(), "1"); err != nil {
		t.Fatal(err)
	}
	moved, err := filepath.Glob(path + ".corrupt-*")
	if err != nil || len(moved) != 1 {
		t.Fatalf("quarantined files = %v, %v; want one", moved, err)
	}
	if temps, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(temps) != 0 {
		t.Fatalf("temporary files left behind: %v", temps)
	}
}

// Every account read before the first write keeps the flat history after a restart.
func TestFileHistoryStoreMigratesFlatHistoryForEveryReadAccount(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "player_history.json")
	if err := os.WriteFile(path, []byte(`{"1": 1700000000}`), 0o600); err != nil {
		t.Fatal(err)
	}
	b := &Broadcaster{log: testBroadcasterLogger(), ctx: ctx, conf: Config{
		XUID:          "primary",
		FriendHistory: NewFileHistoryStore(path),
		SubAccounts:   []SubAccountConfig{{ID: "sub", Enabled: true, XUID: "sub"}},
	}}
	b.primeFriendHistory()
	if err := b.conf.FriendHistory.Track(ctx, "primary", time.Now(), "2"); err != nil {
		t.Fatal(err)
	}
	sub, err := NewFileHistoryStore(path).LastSeen(ctx, "sub")
	if err != nil {
		t.Fatal(err)
	}
	if sub["1"].Unix() != 1700000000 {
		t.Fatalf("sub-account history after restart = %v, want the migrated clock", sub)
	}
}

// A failed write must be retried by the next update, even one that changes nothing.
func TestFileHistoryStoreRetriesFailedSave(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "cache")
	if err := os.Mkdir(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "player_history.json")
	store := NewFileHistoryStore(path)
	when := time.Now().Truncate(time.Second)
	if err := store.Track(ctx, "me", when, "1"); err == nil {
		t.Fatal("expected the save to fail in a read-only directory")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.Track(ctx, "me", when.Add(time.Hour), "1"); err != nil {
		t.Fatal(err)
	}
	lastSeen, err := NewFileHistoryStore(path).LastSeen(ctx, "me")
	if err != nil {
		t.Fatal(err)
	}
	if !lastSeen["1"].Equal(when) {
		t.Fatalf("saved history = %v, want 1 tracked at %s", lastSeen, when)
	}
}
