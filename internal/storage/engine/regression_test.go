package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

// overwriteAndFlush returns a hook that adds a newer version of k and flushes
// it, which drops every older version of k that is shadowed by a visible one.
// Running it between a read's two steps is what the snapshot tests are about.
func overwriteAndFlush(t *testing.T, db *DB, k []byte) func() {
	return func() {
		testHookBeforeCapture = nil
		if err := db.Put(k, []byte("v3")); err != nil {
			t.Error(err)
		}
		if err := db.Flush(); err != nil {
			t.Error(err)
		}
	}
}

// Regression: Get used to load its snapshot before capturing the version, so
// a flush in between could drop the only version the snapshot could see.
func TestGetSnapshotOrder(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	k := []byte("k")
	db.Put(k, []byte("v1"))
	db.Put(k, []byte("v2"))
	testHookBeforeCapture = overwriteAndFlush(t, db, k)
	v, err := db.Get(k)
	if err != nil {
		t.Fatalf("key has existed the whole time, but Get returned %q, %v", v, err)
	}
	if string(v) != "v3" {
		t.Fatalf("Get returned %q, want v3", v)
	}
}

// Regression: the same bug in Scan.
func TestScanSnapshotOrder(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	k := []byte("k")
	db.Put(k, []byte("v1"))
	db.Put(k, []byte("v2"))
	testHookBeforeCapture = overwriteAndFlush(t, db, k)
	it := db.Scan(nil, nil)
	defer it.Close()
	if !it.Next() {
		t.Fatalf("key has existed the whole time, but Scan returned nothing (err %v)", it.Err())
	}
	if string(it.Value()) != "v3" {
		t.Fatalf("Scan returned %q, want v3", it.Value())
	}
}

// The same bug without a hook: one writer counts up on a single key while
// readers check they never see less than the last acknowledged count. Small
// memtables and too few CPUs make flushes overlap with preempted reads.
func TestReadsNeverGoBackwardsUnderFlushes(t *testing.T) {
	if testing.Short() {
		t.Skip("takes 3 seconds")
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(2))
	opts := smallOpts()
	opts.MemtableBytes = 4 << 10
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()
	k := []byte("k")
	if err := db.Put(k, []byte("0000000000000000")); err != nil {
		t.Fatal(err)
	}
	var acked atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 16; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				lo := acked.Load()
				v, err := db.Get(k)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				if got, _ := strconv.ParseInt(string(v), 10, 64); got < lo {
					t.Errorf("read %d after %d was acknowledged", got, lo)
					return
				}
			}
		}()
	}
	end := time.Now().Add(3 * time.Second)
	for i := int64(1); time.Now().Before(end); i++ {
		if err := db.Put(k, []byte(fmt.Sprintf("%016d", i))); err != nil {
			t.Fatal(err)
		}
		acked.Store(i)
	}
	close(stop)
	wg.Wait()
}

// Regression: a value over the WAL's replay limit used to be acknowledged and
// then discarded by the next recovery.
func TestOversizeRecordRejected(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	if err := db.Put([]byte("big"), make([]byte, wal.MaxRecordBytes)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Put returned %v, want ErrTooLarge", err)
	}
	if err := db.Put([]byte("small"), []byte("v")); err != nil {
		t.Fatalf("a rejected write must not break later writes: %v", err)
	}
}

// Regression: calling Next again after the end used to panic, and after the
// upper bound it used to return keys past the bound.
func TestExhaustedIteratorStaysExhausted(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	empty := db.Scan(nil, nil)
	defer empty.Close()
	for i := 0; i < 3; i++ {
		if empty.Next() {
			t.Fatal("empty database returned a key")
		}
	}
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("c"), []byte("2"))
	bounded := db.Scan(nil, []byte("b"))
	defer bounded.Close()
	if !bounded.Next() || string(bounded.Key()) != "a" {
		t.Fatal("expected a")
	}
	for i := 0; i < 3; i++ {
		if bounded.Next() {
			t.Fatalf("returned %q past the upper bound", bounded.Key())
		}
	}
}

// Regression: a bit flip early in the newest WAL segment used to be treated
// as a torn tail, so Open dropped every acknowledged record after it.
func TestMidSegmentCorruptionRefusesOpen(t *testing.T) {
	dir := t.TempDir()
	opts := smallOpts()
	opts.NoFlushOnClose = true
	opts.MemtableBytes = 1 << 30
	db := mustOpen(t, dir, opts)
	for i := 0; i < 100; i++ {
		if err := db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	var path string
	for _, s := range segs {
		if info, _ := os.Stat(s); info.Size() > 0 {
			path = s
		}
	}
	data, _ := os.ReadFile(path)
	data[20] ^= 0x01 // inside the first of 100 records
	os.WriteFile(path, data, 0o644)

	db, err := Open(dir, smallOpts())
	if err == nil {
		defer db.Close()
		t.Fatalf("Open succeeded and dropped acknowledged records (WALTruncatedBytes=%d)", db.Stats().WALTruncatedBytes)
	}
	if !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("Open returned %v, want wal.ErrCorrupt", err)
	}
}
