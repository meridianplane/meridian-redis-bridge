package wal_test

import (
	"os"
	"testing"
	"time"

	"github.com/meridianplane/meridian-redis-bridge/internal/wal"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
)

func entry(key string) *pb.WalEntry {
	return &pb.WalEntry{Args: [][]byte{[]byte("SET"), []byte(key), []byte("v-" + key)}}
}

func openWAL(t *testing.T, opts wal.Options) *wal.WAL {
	t.Helper()
	w, err := wal.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	return w
}

func TestAppendAssignsMonotonicSeq(t *testing.T) {
	w := openWAL(t, wal.Options{Dir: t.TempDir()})
	for i := uint64(1); i <= 5; i++ {
		seq, err := w.Append(entry("k"))
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if seq != i {
			t.Fatalf("Append seq = %d, want %d", seq, i)
		}
	}
	if w.NextSeq() != 6 {
		t.Fatalf("NextSeq = %d, want 6", w.NextSeq())
	}
}

func TestReadFrom_OrderOffsetAndBatch(t *testing.T) {
	w := openWAL(t, wal.Options{Dir: t.TempDir()})
	for i := 0; i < 5; i++ {
		if _, err := w.Append(entry("k")); err != nil {
			t.Fatal(err)
		}
	}
	// maxBatch caps the slice.
	got, err := w.ReadFrom(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].SeqId != 1 || got[1].SeqId != 2 {
		t.Fatalf("ReadFrom(1,2) = %v", seqs(got))
	}
	// fromSeq skips earlier entries.
	got, err = w.ReadFrom(3, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].SeqId != 3 || got[2].SeqId != 5 {
		t.Fatalf("ReadFrom(3,100) = %v", seqs(got))
	}
}

func TestRecoveryAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(wal.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := w.Append(entry("k")); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w2 := openWAL(t, wal.Options{Dir: dir})
	if w2.NextSeq() != 4 {
		t.Fatalf("after reopen NextSeq = %d, want 4", w2.NextSeq())
	}
	got, err := w2.ReadFrom(1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("recovered %d entries, want 3", len(got))
	}
	// New appends continue the sequence.
	seq, _ := w2.Append(entry("k"))
	if seq != 4 {
		t.Fatalf("post-recovery Append seq = %d, want 4", seq)
	}
}

func TestSegmentRollSpansReads(t *testing.T) {
	dir := t.TempDir()
	// Tiny max size forces a roll after essentially every append.
	w := openWAL(t, wal.Options{Dir: dir, SegmentMaxSize: 1})
	const n = 10
	for i := 0; i < n; i++ {
		if _, err := w.Append(entry("k")); err != nil {
			t.Fatal(err)
		}
	}
	files, _ := os.ReadDir(dir)
	if len(files) < 2 {
		t.Fatalf("expected multiple segments, found %d", len(files))
	}
	got, err := w.ReadFrom(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("ReadFrom across segments = %d entries, want %d", len(got), n)
	}
	for i, e := range got {
		if e.SeqId != uint64(i+1) {
			t.Fatalf("entry %d has seq %d, want %d", i, e.SeqId, i+1)
		}
	}
}

func TestNotifySignalsOnAppend(t *testing.T) {
	w := openWAL(t, wal.Options{Dir: t.TempDir()})
	ch := w.Notify()
	if _, err := w.Append(entry("k")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Notify did not signal after Append")
	}
}

func TestTruncatedTailTreatedAsCleanEOF(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(wal.Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := w.Append(entry("k")); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	// Corrupt the tail: a varint header claiming a 5-byte body with no body
	// following simulates a crash mid-append.
	files, _ := os.ReadDir(dir)
	seg := dir + "/" + files[len(files)-1].Name()
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x05}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Reopen: the two good records survive, the partial tail is ignored, and
	// the next seq resumes right after the last good record.
	w2 := openWAL(t, wal.Options{Dir: dir})
	got, err := w2.ReadFrom(1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("recovered %d good entries past a truncated tail, want 2", len(got))
	}
	if w2.NextSeq() != 3 {
		t.Fatalf("NextSeq = %d after truncated tail, want 3", w2.NextSeq())
	}
}

// TestSnapshot_Barrier verifies StartSnapshot / AbortSnapshot barrier.
func TestSnapshot_Barrier(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(wal.Options{Dir: dir, SegmentMaxSize: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	w.Append(&pb.WalEntry{Args: bargs("SET", "x", "old")})
	if w.NextSeq() != 2 {
		t.Fatalf("NextSeq = %d, want 2", w.NextSeq())
	}
	barrier := w.StartSnapshot()
	if barrier != 2 {
		t.Fatalf("barrier = %d, want 2", barrier)
	}
	w.AbortSnapshot()
}

func strings2(ss [][]byte) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = string(s)
	}
	return out
}

func bargs(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func seqs(es []*pb.WalEntry) []uint64 {
	out := make([]uint64, len(es))
	for i, e := range es {
		out[i] = e.SeqId
	}
	return out
}
