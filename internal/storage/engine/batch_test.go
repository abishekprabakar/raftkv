package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

func batchOf(pairs ...string) *Batch {
	b := &Batch{}
	for i := 0; i+1 < len(pairs); i += 2 {
		b.Put([]byte(pairs[i]), []byte(pairs[i+1]))
	}
	return b
}

func TestBatchAppliesEveryOp(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	db.Put([]byte("gone"), []byte("x"))
	b := batchOf("a", "1", "b", "2")
	b.Delete([]byte("gone"))
	b.Put([]byte("a"), []byte("3")) // a later op on a key wins
	if err := db.Write(b); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get([]byte("a")); err != nil || string(v) != "3" {
		t.Fatalf("a = %q %v", v, err)
	}
	if v, err := db.Get([]byte("b")); err != nil || string(v) != "2" {
		t.Fatalf("b = %q %v", v, err)
	}
	if _, err := db.Get([]byte("gone")); err != ErrNotFound {
		t.Fatalf("gone: %v", err)
	}
}

func TestEmptyBatchIsANoOp(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	if err := db.Write(&Batch{}); err != nil {
		t.Fatal(err)
	}
}

func TestOversizeBatchRejected(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	b := batchOf("k", string(make([]byte, wal.MaxRecordBytes)))
	if err := db.Write(b); err != ErrTooLarge {
		t.Fatalf("got %v", err)
	}
}

func TestBatchSurvivesRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := smallOpts()
	opts.NoFlushOnClose = true
	opts.MemtableBytes = 1 << 30
	db := mustOpen(t, dir, opts)
	db.Write(batchOf("a", "1", "b", "2"))
	db.Put([]byte("c"), []byte("3"))
	db.Write(batchOf("d", "4"))
	db.Close()

	db = mustOpen(t, dir, smallOpts())
	defer db.Close()
	for _, kv := range [][2]string{{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}} {
		if v, err := db.Get([]byte(kv[0])); err != nil || string(v) != kv[1] {
			t.Fatalf("%s = %q %v", kv[0], v, err)
		}
	}
	// Sequence numbers must carry on past the last op of the last batch.
	db.Put([]byte("a"), []byte("new"))
	if v, _ := db.Get([]byte("a")); string(v) != "new" {
		t.Fatalf("a = %q: a new write lost to a replayed one", v)
	}
}

// A torn write must take the whole batch with it, however many ops had reached
// the disk. This is what makes a write and its session update atomic.
func TestTornBatchDisappearsWhole(t *testing.T) {
	dir := t.TempDir()
	opts := smallOpts()
	opts.NoFlushOnClose = true
	opts.MemtableBytes = 1 << 30
	db := mustOpen(t, dir, opts)
	db.Put([]byte("before"), []byte("kept"))
	db.Write(batchOf("a", "1", "b", "2", "c", "3"))
	db.Close()

	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	var path string
	for _, s := range segs {
		if info, _ := os.Stat(s); info.Size() > 0 {
			path = s
		}
	}
	data, _ := os.ReadFile(path)
	os.WriteFile(path, data[:len(data)-3], 0o644) // cut inside the last op

	db = mustOpen(t, dir, smallOpts())
	defer db.Close()
	if v, err := db.Get([]byte("before")); err != nil || string(v) != "kept" {
		t.Fatalf("before = %q %v", v, err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if v, err := db.Get([]byte(k)); err != ErrNotFound {
			t.Fatalf("%s survived a torn batch: %q %v", k, v, err)
		}
	}
}

func TestBatchIsAtomicToReaders(t *testing.T) {
	opts := smallOpts()
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			v := []byte(fmt.Sprintf("%d", i))
			if err := db.Write(batchOf("a", string(v), "b", string(v))); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
		}
		it := db.Scan(nil, nil)
		var a, b string
		for it.Next() {
			switch string(it.Key()) {
			case "a":
				a = string(it.Value())
			case "b":
				b = string(it.Value())
			}
		}
		it.Close()
		if a != b {
			t.Fatalf("a scan saw half a batch: a=%s b=%s", a, b)
		}
	}
}
