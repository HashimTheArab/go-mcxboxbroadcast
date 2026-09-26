package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"sync"
	"time"
)

// FileHistoryStore is a HistoryStore kept in one JSON file, with entries keyed
// by account. The file is read once and then served from memory, so one
// process must own it.
//
// A flat file from older versions is copied to each account the first time
// that account is read, so read every account before the first write.
type FileHistoryStore struct {
	Path string
	// Log receives a warning when a corrupt file is moved aside. It may be nil.
	Log *slog.Logger

	mu       sync.Mutex
	loaded   bool
	dirty    bool // memory holds changes a failed save did not write
	accounts map[string]*accountHistory
	legacy   map[string]int64 // flat history from before entries were keyed by account
}

// accountHistory is one account's part of the history file, in Unix seconds.
type accountHistory struct {
	Seen     map[string]int64 `json:"seen"`
	Removing map[string]int64 `json:"removing,omitempty"`
}

// historyFile is the on-disk layout of a FileHistoryStore.
type historyFile struct {
	Accounts map[string]*accountHistory `json:"accounts"`
}

func NewFileHistoryStore(path string) *FileHistoryStore {
	return &FileHistoryStore{Path: path}
}

func (s *FileHistoryStore) LastSeen(ctx context.Context, account string) (map[string]time.Time, error) {
	return s.read(ctx, account, func(h *accountHistory) map[string]int64 { return h.Seen })
}

func (s *FileHistoryStore) Removing(ctx context.Context, account string) (map[string]time.Time, error) {
	return s.read(ctx, account, func(h *accountHistory) map[string]int64 { return h.Removing })
}

func (s *FileHistoryStore) Track(ctx context.Context, account string, when time.Time, xuids ...string) error {
	return s.update(ctx, func() bool {
		seen := s.account(account).Seen
		changed := false
		for _, xuid := range xuids {
			if _, ok := seen[xuid]; !ok {
				seen[xuid] = when.Unix()
				changed = true
			}
		}
		return changed
	})
}

func (s *FileHistoryStore) Seen(ctx context.Context, xuid string, when time.Time) error {
	return s.update(ctx, func() bool {
		changed := false
		for _, h := range s.accounts {
			if _, ok := h.Seen[xuid]; ok {
				h.Seen[xuid] = when.Unix()
				changed = true
			}
		}
		if _, ok := s.legacy[xuid]; ok {
			s.legacy[xuid] = when.Unix()
		}
		return changed
	})
}

func (s *FileHistoryStore) MarkRemoving(ctx context.Context, account string, when time.Time, xuids ...string) error {
	return s.update(ctx, func() bool {
		h := s.account(account)
		if h.Removing == nil {
			h.Removing = map[string]int64{}
		}
		for _, xuid := range xuids {
			delete(h.Seen, xuid)
			h.Removing[xuid] = when.Unix()
		}
		return len(xuids) > 0
	})
}

func (s *FileHistoryStore) Forget(ctx context.Context, account string, xuids ...string) error {
	return s.update(ctx, func() bool {
		h := s.account(account)
		changed := false
		for _, xuid := range xuids {
			_, seen := h.Seen[xuid]
			_, removing := h.Removing[xuid]
			if seen || removing {
				delete(h.Seen, xuid)
				delete(h.Removing, xuid)
				changed = true
			}
		}
		return changed
	})
}

// read returns one of account's maps as times.
func (s *FileHistoryStore) read(ctx context.Context, account string, pick func(*accountHistory) map[string]int64) (map[string]time.Time, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.load(); err != nil {
		return nil, err
	}
	entries := pick(s.account(account))
	out := make(map[string]time.Time, len(entries))
	for xuid, seconds := range entries {
		out[xuid] = time.Unix(seconds, 0).UTC()
	}
	return out, nil
}

// update applies change under the lock and saves when it reports a change or
// an earlier save failed, so a failed write is retried by the next update.
func (s *FileHistoryStore) update(ctx context.Context, change func() bool) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.load(); err != nil {
		return err
	}
	if !change() && !s.dirty {
		return nil
	}
	s.dirty = true
	if err := s.save(); err != nil {
		return err
	}
	s.dirty = false
	return nil
}

// account returns account's history, creating it from legacy history if needed.
func (s *FileHistoryStore) account(account string) *accountHistory {
	h, ok := s.accounts[account]
	if !ok {
		h = &accountHistory{Seen: maps.Clone(s.legacy)}
		s.accounts[account] = h
	}
	if h.Seen == nil {
		h.Seen = map[string]int64{}
	}
	return h
}

func (s *FileHistoryStore) load() error {
	if s.loaded {
		return nil
	}
	data, err := os.ReadFile(s.Path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.accounts = map[string]*accountHistory{}
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
		for account, h := range file.Accounts {
			if h != nil {
				s.accounts[account] = h
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
	s.accounts = map[string]*accountHistory{}
	s.legacy = nil
	dest := fmt.Sprintf("%s.corrupt-%d", s.Path, time.Now().Unix())
	err := os.Rename(s.Path, dest)
	if s.Log != nil {
		s.Log.Warn("player history is corrupt; starting fresh", "path", s.Path, "moved_to", dest, "err", cause, "rename_err", err)
	}
}

func (s *FileHistoryStore) save() error {
	data, err := json.MarshalIndent(historyFile{Accounts: s.accounts}, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.Path, data)
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
