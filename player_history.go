package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileHistoryStore is a HistoryStore kept in one JSON file, with entries keyed
// by account. The file is read once and then served from memory, so one
// process must own it.
type FileHistoryStore struct {
	Path string
	// Log receives a warning when a corrupt file is moved aside. It may be nil.
	Log *slog.Logger

	mu       sync.Mutex
	loaded   bool
	accounts map[string]map[string]int64
	// legacy holds a file written before entries were keyed by account. Each
	// account starts from a copy of it, so upgrades keep existing clocks.
	legacy map[string]int64
}

// historyFile is the on-disk layout of a FileHistoryStore.
type historyFile struct {
	Accounts map[string]map[string]int64 `json:"accounts"`
}

func NewFileHistoryStore(path string) *FileHistoryStore {
	return &FileHistoryStore{Path: path}
}

func (s *FileHistoryStore) LastSeen(ctx context.Context, account string) (map[string]time.Time, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.load(); err != nil {
		return nil, err
	}
	entries := s.account(account)
	out := make(map[string]time.Time, len(entries))
	for xuid, seconds := range entries {
		out[xuid] = time.Unix(seconds, 0).UTC()
	}
	return out, nil
}

func (s *FileHistoryStore) Track(ctx context.Context, account string, when time.Time, xuids ...string) error {
	return s.update(ctx, func() bool {
		entries := s.account(account)
		changed := false
		for _, xuid := range xuids {
			if _, ok := entries[xuid]; !ok {
				entries[xuid] = when.Unix()
				changed = true
			}
		}
		return changed
	})
}

func (s *FileHistoryStore) Seen(ctx context.Context, xuid string, when time.Time) error {
	return s.update(ctx, func() bool {
		changed := false
		for _, entries := range s.accounts {
			if _, ok := entries[xuid]; ok {
				entries[xuid] = when.Unix()
				changed = true
			}
		}
		if _, ok := s.legacy[xuid]; ok {
			s.legacy[xuid] = when.Unix()
		}
		return changed
	})
}

func (s *FileHistoryStore) Forget(ctx context.Context, account string, xuids ...string) error {
	return s.update(ctx, func() bool {
		entries := s.account(account)
		changed := false
		for _, xuid := range xuids {
			if _, ok := entries[xuid]; ok {
				delete(entries, xuid)
				changed = true
			}
		}
		return changed
	})
}

// update applies change under the lock and saves when it reports a change.
func (s *FileHistoryStore) update(ctx context.Context, change func() bool) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.load(); err != nil {
		return err
	}
	if !change() {
		return nil
	}
	return s.save()
}

// account returns account's entries, creating them from legacy history if needed.
func (s *FileHistoryStore) account(account string) map[string]int64 {
	entries, ok := s.accounts[account]
	if !ok {
		entries = maps.Clone(s.legacy)
		if entries == nil {
			entries = map[string]int64{}
		}
		s.accounts[account] = entries
	}
	return entries
}

func (s *FileHistoryStore) load() error {
	if s.loaded {
		return nil
	}
	data, err := os.ReadFile(s.Path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.accounts = map[string]map[string]int64{}
	if len(data) > 0 {
		if err := s.decode(data); err != nil {
			s.quarantine(err)
		}
	}
	s.loaded = true
	return nil
}

// decode reads the account-keyed layout, or a flat XUID map from older versions.
func (s *FileHistoryStore) decode(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if _, ok := raw["accounts"]; ok {
		var file historyFile
		if err := json.Unmarshal(data, &file); err != nil {
			return err
		}
		for account, entries := range file.Accounts {
			if entries != nil {
				s.accounts[account] = entries
			}
		}
		return nil
	}
	legacy := map[string]int64{}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	s.legacy = legacy
	return nil
}

// quarantine moves an unreadable file aside so the store can start fresh.
func (s *FileHistoryStore) quarantine(cause error) {
	s.accounts = map[string]map[string]int64{}
	s.legacy = nil
	dest := fmt.Sprintf("%s.corrupt-%d", s.Path, time.Now().Unix())
	err := os.Rename(s.Path, dest)
	if s.Log != nil {
		s.Log.Warn("player history is corrupt; starting fresh", "path", s.Path, "moved_to", dest, "err", cause, "rename_err", err)
	}
}

func (s *FileHistoryStore) save() error {
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(historyFile{Accounts: s.accounts}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(s.Path)+".*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after the rename
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.Path)
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
