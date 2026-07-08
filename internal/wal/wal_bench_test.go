package wal_test

import (
	"fmt"
	"testing"

	"github.com/meridianplane/meridian-redis-bridge/internal/wal"
	pb "github.com/meridianplane/meridian-redis-bridge/proto"
)

var flushModes = []struct {
	name string
	mode wal.FlushMode
}{
	{"none", wal.FlushNone},
	{"periodic", wal.FlushPeriodic},
	{"sync", wal.FlushSync},
}

// BenchmarkAppend_FlushMode measures throughput under each flush mode.
func BenchmarkAppend_FlushMode(b *testing.B) {
	for _, fm := range flushModes {
		b.Run(fm.name, func(b *testing.B) {
			dir := b.TempDir()
			w, err := wal.Open(wal.Options{Dir: dir, Flush: fm.mode, FlushInterval: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			entry := &pb.WalEntry{Args: [][]byte{
				[]byte("SET"), []byte("bench"), []byte("value"),
			}}
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := w.Append(entry); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkAppend_Parallel measures parallel throughput.
func BenchmarkAppend_Parallel(b *testing.B) {
	for _, fm := range flushModes {
		b.Run(fm.name, func(b *testing.B) {
			dir := b.TempDir()
			w, err := wal.Open(wal.Options{Dir: dir, Flush: fm.mode, FlushInterval: 1})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(p *testing.PB) {
				entry := &pb.WalEntry{Args: [][]byte{
					[]byte("SET"), []byte("bench"), []byte("value"),
				}}
				for p.Next() {
					if _, err := w.Append(entry); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
		})
	}
}

// BenchmarkAppend_SizeMeasures throughput with varying payload sizes.
func BenchmarkAppend_Size(b *testing.B) {
	sizes := []int{16, 128, 1024}
	for _, fm := range flushModes[:2] { // skip sync for large data
		for _, sz := range sizes {
			b.Run(fmt.Sprintf("%s/%db", fm.name, sz), func(b *testing.B) {
				dir := b.TempDir()
				w, err := wal.Open(wal.Options{Dir: dir, Flush: fm.mode, FlushInterval: 10})
				if err != nil {
					b.Fatal(err)
				}
				defer w.Close()

				val := make([]byte, sz)
				for i := range val {
					val[i] = byte('a' + i%26)
				}
				entry := &pb.WalEntry{Args: [][]byte{
					[]byte("SET"), []byte("k"), val,
				}}
				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := w.Append(entry); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
