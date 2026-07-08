// Package wal implements the append-only, segmented write-ahead log.
//
// Segments named 00000000000000000001.wal etc. store protobuf WalEntry records.
// Compact deletes sealed segments once all followers have passed them, keeping
// the first segment as the implicit baseline for new followers.
package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/meridianplane/meridian-redis-bridge/proto"
	"google.golang.org/protobuf/proto"
)

const (
	segmentExt        = ".wal"
	segmentNameDigits = 20
	defaultSegmentMax = int64(64 << 20) // 64 MiB sealed roll-over
)

type FlushMode int

const (
	FlushNone     FlushMode = iota // let OS drain buffers
	FlushPeriodic                  // fsync every interval
	FlushSync                      // fsync after each write
)

// Options configures the WAL.
type Options struct {
	Dir            string
	SegmentMaxSize int64      // 0 -> default
	Flush          FlushMode  // 0 -> none
	FlushInterval  int        // ms, for periodic mode (0 -> 100ms)
}

// WAL is the append-only segmented log with snapshot + delta separation.
// Compact folds all sealed delta segments into a snapshot file; the active
// segment is untouched. New followers load the snapshot first.
type WAL struct {
	mu         sync.Mutex
	dir        string
	segmentMax int64

	snapshotSeq uint64        // barrier: WAL >= this seq is pinned during snapshot
	segments   []*segment    // WAL segments
	active     *segment      // current writing segment
	flushMode  FlushMode
	flushEvery int64 // ms
	flushStop  chan struct{}

	nextSeq     uint64
	notifyChans []chan struct{}
}


// segment is one sealed-or-active segment file.
type segment struct {
	id     uint64 // first seq in this segment
	endSeq uint64 // last seq in this segment (0 while active)
	path   string
	file   *os.File
	size   int64
}

// Open opens or creates a WAL under opts.Dir, recovering nextSeq from the
// segments already on disk.
func Open(opts Options) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("wal: Dir required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	if opts.SegmentMaxSize == 0 {
		opts.SegmentMaxSize = defaultSegmentMax
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 100
	}
	w := &WAL{dir: opts.Dir, segmentMax: opts.SegmentMaxSize,
		flushMode: opts.Flush, flushEvery: int64(opts.FlushInterval)}
	if err := w.loadSegments(); err != nil {
		return nil, err
	}
	if w.flushMode == FlushPeriodic {
		w.startFlusher()
	}
	return w, nil
}

func (w *WAL) loadSegments() error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return err
	}
	var ids []uint64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), segmentExt) {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(e.Name(), segmentExt), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)

	for i, id := range ids {
		seg := &segment{
			id:   id,
			path: filepath.Join(w.dir, fmt.Sprintf("%0*d%s", segmentNameDigits, id, segmentExt)),
		}
		f, err := os.OpenFile(seg.path, os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		seg.size = fi.Size()
		if i != len(ids)-1 { // sealed segments record their endSeq
			seg.endSeq, err = scanLastSeq(f)
			if err != nil {
				f.Close()
				return err
			}
		}
		f.Close()
		w.segments = append(w.segments, seg)
	}

	if len(w.segments) > 0 {
		last := w.segments[len(w.segments)-1]
		f, err := os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		last.file = f
		w.active = last
		lastSeq, err := scanLastSeq(f)
		if err != nil {
			return err
		}
		if lastSeq+1 > w.nextSeq {
			w.nextSeq = lastSeq + 1
		}
	}
	if w.nextSeq == 0 {
		w.nextSeq = 1
	}
	return nil
}

// rollLocked seals the active segment (if any) and starts a new one at the
// current nextSeq.
func (w *WAL) rollLocked() error {
	if w.active != nil {
		w.active.endSeq = w.nextSeq - 1
		if err := w.active.file.Sync(); err != nil {
			return err
		}
		if err := w.active.file.Close(); err != nil {
			return err
		}
		w.active.file = nil
	}
	id := w.nextSeq
	path := filepath.Join(w.dir, fmt.Sprintf("%0*d%s", segmentNameDigits, id, segmentExt))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	seg := &segment{id: id, path: path, file: f}
	w.segments = append(w.segments, seg)
	w.active = seg
	return nil
}

// Append assigns a seq_id to e, writes it durably, and returns the seq. The
// caller must NOT pre-assign e.SeqId.
func (w *WAL) Append(e *pb.WalEntry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		if err := w.rollLocked(); err != nil {
			return 0, err
		}
	}
	e.SeqId = w.nextSeq
	w.nextSeq++

	if err := writeRecord(w.active.file, e); err != nil {
		return 0, err
	}
	if fi, err := w.active.file.Stat(); err == nil {
		w.active.size = fi.Size()
	}
	if w.active.size >= w.segmentMax {
		if err := w.rollLocked(); err != nil {
			return 0, err
		}
	}
	if w.flushMode == FlushSync {
		if err := w.active.file.Sync(); err != nil {
			return 0, err
		}
	}
	w.notifyAllLocked()
	return e.SeqId, nil
}

func (w *WAL) startFlusher() {
	w.flushStop = make(chan struct{})
	go func() {
		t := time.NewTicker(time.Duration(w.flushEvery) * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-w.flushStop:
				return
			case <-t.C:
				w.mu.Lock()
				if w.active != nil && w.active.file != nil {
					_ = w.active.file.Sync()
				}
				w.mu.Unlock()
			}
		}
	}()

}// NextSeq returns the seq the next Append will assign.
func (w *WAL) NextSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.nextSeq
}

// ReadFrom returns up to maxBatch entries with seq_id >= fromSeq. Delta
// first: if fromSeq is within the delta range, serve delta only. Fall back
// to snapshot only when the follower is behind all delta.
func (w *WAL) ReadFrom(fromSeq uint64, maxBatch int) ([]*pb.WalEntry, error) {
	w.mu.Lock()
	segs := append([]*segment(nil), w.segments...)
	w.mu.Unlock()

	out := make([]*pb.WalEntry, 0, maxBatch)
	for _, seg := range segs {
		if seg.endSeq != 0 && seg.endSeq < fromSeq {
			continue
		}
		f, err := os.Open(seg.path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) { continue }
			return nil, err
		}
		scanRecords(f, func(e *pb.WalEntry) (bool, error) {
			if e.SeqId < fromSeq { return true, nil }
			out = append(out, e)
			if len(out) >= maxBatch { return false, nil }
			return true, nil
		})
		f.Close()
		if len(out) >= maxBatch { break }
	}
	return out, nil
}

// Notify returns a channel signalled (non-blocking) whenever new records are
// appended. Receivers re-poll ReadFrom on signal.
func (w *WAL) Notify() <-chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	w.notifyChans = append(w.notifyChans, ch)
	w.mu.Unlock()
	return ch
}

func (w *WAL) notifyAllLocked() {
	for _, ch := range w.notifyChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SegmentCount returns the number of segments for testing.
func (w *WAL) SegmentCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.segments)
}

// StartSnapshot pins the WAL at the current seq and returns it. The caller
// uses this seq as the barrier: no WAL segment with id >= this seq will be
// cleaned up until WriteSnapshot completes.
func (w *WAL) StartSnapshot() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshotSeq = w.nextSeq
	return w.snapshotSeq
}

// AbortSnapshot clears the snapshot barrier without writing a snapshot.
func (w *WAL) AbortSnapshot() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshotSeq = 0
}

// WriteSnapshot writes the snapshot to disk from pre-built entries and
// clears old segments before the snapshot barrier. Returns the snapshot
// seq (max seq covered).
func writeRecord(f *os.File, e *pb.WalEntry) error {
	body, err := proto.Marshal(e)
	if err != nil {
		return err
	}
	return AppendRecord(f, body)
}

// scanRecords iterates WalEntry records in f from the current offset. A
// truncated tail (a partial record left by a crash) is treated as a clean end
// of file by the underlying Scan.
func scanRecords(f *os.File, cb func(*pb.WalEntry) (bool, error)) error {
	return Scan(f, func(body []byte) (bool, error) {
		var e pb.WalEntry
		if err := proto.Unmarshal(body, &e); err != nil {
			return false, err
		}
		return cb(&e)
	})
}

// scanLastSeq returns the seq_id of the last well-formed record in f.
func scanLastSeq(f *os.File) (uint64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	var last uint64
	err := scanRecords(f, func(e *pb.WalEntry) (bool, error) {
		last = e.SeqId
		return true, nil
	})
	return last, err
}

// MinSeq returns the oldest available WAL seq.
func (w *WAL) MinSeq() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.segments) == 0 { return 0 }
	return w.segments[0].id
}

func (w *WAL) Close() error {
	if w.flushStop != nil {
		close(w.flushStop)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active != nil && w.active.file != nil {
		_ = w.active.file.Sync()
		_ = w.active.file.Close()
		w.active.file = nil
	}
	return nil
}
