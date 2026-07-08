// Package sync implements the cross-region replication transport: an owner-side
// fan-out server that streams WAL entries in seq order, and a follower-side
// consumer that applies them to the local backend and persists how far it has
// applied.
//
// The model is deliberately one-directional and single-stream-per-owner. The
// owner is the only writer; followers never push back, never reorder, and never
// resolve conflicts. All a follower must remember across restarts is the seq of
// the last entry it applied, so it can resume from exactly the right point.
package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	syncpkg "sync"
)

// Watermark persists a follower's last-applied WAL seq. It is the follower's
// only durable replication state; the owner keeps none.
//
// Durability model: every Set rewrites a small file via write-temp + rename so
// a crash can never leave a half-written value. Because every replicated op is
// idempotent (the five OpTypes all set an absolute value or delete), it is safe
// to persist the watermark once per applied batch and re-apply anything after
// the persisted point following a crash.
type Watermark struct {
	path string

	mu      syncpkg.Mutex
	applied uint64
}

// OpenWatermark loads the watermark file at path, creating its directory if
// needed. A missing or empty file means "nothing applied yet" (seq 0), so the
// first SubscribeRequest will ask for seq 1.
func OpenWatermark(path string) (*Watermark, error) {
	if path == "" {
		return nil, fmt.Errorf("sync: watermark path required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	w := &Watermark{path: path}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return w, nil
		}
		return nil, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return w, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("sync: corrupt watermark %q: %w", path, err)
	}
	w.applied = v
	return w, nil
}

// Applied returns the seq of the last entry durably applied.
func (w *Watermark) Applied() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.applied
}

// NextSeq returns the next seq the follower needs (Applied + 1). With no
// progress yet this is 1, matching the WAL's first assigned seq.
func (w *Watermark) NextSeq() uint64 {
	return w.Applied() + 1
}

// Set durably records seq as the last-applied position. It ignores values that
// do not advance the watermark so a redelivered batch cannot move it backwards.
func (w *Watermark) Set(seq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq <= w.applied {
		return nil
	}
	if err := w.writeAtomic(seq); err != nil {
		return err
	}
	w.applied = seq
	return nil
}

func (w *Watermark) writeAtomic(seq uint64) error {
	tmp := w.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(strconv.FormatUint(seq, 10)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, w.path)
}
