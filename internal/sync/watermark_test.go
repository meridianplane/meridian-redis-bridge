package sync_test

import (
	"os"
	"path/filepath"
	"testing"

	rsync "github.com/meridianplane/meridian-redis-bridge/internal/sync"
)

func TestOpenWatermark_MissingFileStartsAtZero(t *testing.T) {
	wm, err := rsync.OpenWatermark(filepath.Join(t.TempDir(), "wm"))
	if err != nil {
		t.Fatalf("OpenWatermark: %v", err)
	}
	if wm.Applied() != 0 {
		t.Fatalf("Applied = %d, want 0", wm.Applied())
	}
	if wm.NextSeq() != 1 {
		t.Fatalf("NextSeq = %d, want 1 (matches WAL first seq)", wm.NextSeq())
	}
}

func TestWatermark_SetAdvancesAndIsMonotonic(t *testing.T) {
	wm, _ := rsync.OpenWatermark(filepath.Join(t.TempDir(), "wm"))
	if err := wm.Set(5); err != nil {
		t.Fatal(err)
	}
	if wm.Applied() != 5 || wm.NextSeq() != 6 {
		t.Fatalf("after Set(5): Applied=%d NextSeq=%d", wm.Applied(), wm.NextSeq())
	}
	// A lower or equal value (e.g. a redelivered batch) must not move it back.
	if err := wm.Set(3); err != nil {
		t.Fatal(err)
	}
	if wm.Applied() != 5 {
		t.Fatalf("Set(3) moved watermark backwards to %d", wm.Applied())
	}
	if err := wm.Set(5); err != nil {
		t.Fatal(err)
	}
	if wm.Applied() != 5 {
		t.Fatalf("Set(5) changed watermark to %d", wm.Applied())
	}
}

func TestWatermark_PersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm")
	wm, _ := rsync.OpenWatermark(path)
	if err := wm.Set(42); err != nil {
		t.Fatal(err)
	}
	reopened, err := rsync.OpenWatermark(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if reopened.Applied() != 42 {
		t.Fatalf("reopened Applied = %d, want 42", reopened.Applied())
	}
}

func TestOpenWatermark_CorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wm")
	if err := os.WriteFile(path, []byte("not-a-number"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := rsync.OpenWatermark(path); err == nil {
		t.Fatal("corrupt watermark should fail to open")
	}
}

func TestOpenWatermark_EmptyPathRejected(t *testing.T) {
	if _, err := rsync.OpenWatermark(""); err == nil {
		t.Fatal("empty path should be rejected")
	}
}

func TestWatermark_NoTempLeftBehind(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wm")
	wm, _ := rsync.OpenWatermark(path)
	if err := wm.Set(7); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("atomic write left a .tmp file behind")
	}
}
