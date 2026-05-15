package broadcaster

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type FileHistoryStore struct {
	Path string

	mu sync.Mutex
}

func NewFileHistoryStore(path string) *FileHistoryStore {
	return &FileHistoryStore{Path: path}
}

func (s *FileHistoryStore) LastSeen(ctx context.Context, xuid string) (time.Time, bool, error) {
	if err := ctxErr(ctx); err != nil {
		return time.Time{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	history, err := s.load()
	if err != nil {
		return time.Time{}, false, err
	}
	seconds, ok := history[xuid]
	if !ok {
		return time.Time{}, false, nil
	}
	return time.Unix(seconds, 0).UTC(), true, nil
}

func (s *FileHistoryStore) Seen(ctx context.Context, xuid string, when time.Time) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	history, err := s.load()
	if err != nil {
		return err
	}
	history[xuid] = when.Unix()
	return s.save(history)
}

func (s *FileHistoryStore) Clear(ctx context.Context, xuid string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	history, err := s.load()
	if err != nil {
		return err
	}
	delete(history, xuid)
	return s.save(history)
}

func (s *FileHistoryStore) load() (map[string]int64, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]int64{}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return map[string]int64{}, nil
	}
	history := map[string]int64{}
	if err := json.Unmarshal(data, &history); err != nil {
		return nil, err
	}
	return history, nil
}

func (s *FileHistoryStore) save(history map[string]int64) error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.Path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.Path)
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
