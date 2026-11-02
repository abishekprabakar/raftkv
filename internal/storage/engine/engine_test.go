package engine

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

func smallOpts() Options {
	return Options{
		SyncMode:            wal.NoSync,
		MemtableBytes:       32 << 10,
		BlockBytes:          1024,
		L0CompactionTrigger: 2,
		BaseLevelBytes:      128 << 10,
		TargetFileBytes:     32 << 10,
		LevelMultiplier:     4,
	}
}

func mustOpen(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPutGetDelete(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	if err := db.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("a"))
	if err != nil || string(v) != "1" {
		t.Fatalf("get a = %q %v", v, err)
	}
	db.Put([]byte("a"), []byte("2"))
	if v, _ := db.Get([]byte("a")); string(v) != "2" {
		t.Fatalf("overwrite failed: %q", v)
	}
	if err := db.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("a")); err != ErrNotFound {
		t.Fatalf("after delete err = %v", err)
	}
	if _, err := db.Get([]byte("missing")); err != ErrNotFound {
		t.Fatalf("missing err = %v", err)
	}
}

// model is the reference implementation the engine is checked against.
type model struct {
	m map[string]string
}

func (m *model) apply(rnd *rand.Rand, db *DB, n int, keyspace int) error {
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("key%06d", rnd.IntN(keyspace))
		if rnd.IntN(10) == 0 {
			delete(m.m, k)
			if err := db.Delete([]byte(k)); err != nil {
				return err
			}
			continue
		}
		v := fmt.Sprintf("v%d-%s", i, randPad(rnd, 200))
		m.m[k] = v
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			return err
		}
	}
	return nil
}

func randPad(rnd *rand.Rand, max int) string {
	b := make([]byte, rnd.IntN(max))
	for i := range b {
		b[i] = 'a' + byte(rnd.IntN(26))
	}
	return string(b)
}

func (m *model) check(t *testing.T, db *DB, keyspace int) {
	t.Helper()
	for i := 0; i < keyspace; i++ {
		k := fmt.Sprintf("key%06d", i)
		want, ok := m.m[k]
		got, err := db.Get([]byte(k))
		if !ok {
			if err != ErrNotFound {
				t.Fatalf("get %s: want not found, got %q %v", k, got, err)
			}
			continue
		}
		if err != nil || string(got) != want {
			t.Fatalf("get %s: got %q %v want %q", k, got, err, want)
		}
	}
	// Full scan matches the sorted live key set exactly.
	keys := make([]string, 0, len(m.m))
	for k := range m.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	it := db.Scan(nil, nil)
	defer it.Close()
	i := 0
	for it.Next() {
		if i >= len(keys) {
			t.Fatalf("scan returned extra key %q", it.Key())
		}
		if string(it.Key()) != keys[i] || string(it.Value()) != m.m[keys[i]] {
			t.Fatalf("scan[%d] = %q/%q want %q/%q", i, it.Key(), it.Value(), keys[i], m.m[keys[i]])
		}
		i++
	}
	if it.Err() != nil || i != len(keys) {
		t.Fatalf("scan: %d keys, want %d, err %v", i, len(keys), it.Err())
	}
}

func TestModelWithFlushAndCompaction(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, smallOpts())
	rnd := rand.New(rand.NewPCG(7, 11))
	m := &model{m: map[string]string{}}
	const keyspace = 3000
	for round := 0; round < 5; round++ {
		if err := m.apply(rnd, db, 4000, keyspace); err != nil {
			t.Fatal(err)
		}
		m.check(t, db, keyspace)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	s := db.Stats()
	if s.Flushes == 0 || s.Compactions == 0 {
		t.Fatalf("expected background work, stats %+v", s)
	}
	nonEmpty := 0
	for _, n := range s.LevelFiles {
		if n > 0 {
			nonEmpty++
		}
	}
	if nonEmpty != 1 {
		t.Fatalf("Compact should leave one populated level, got %v", s.LevelFiles)
	}
	m.check(t, db, keyspace)
	t.Logf("levels files=%v bytes=%v compactions=%d trivial=%d", s.LevelFiles, s.LevelBytes, s.Compactions, s.TrivialMoves)

	// Reopen and verify everything survived.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = mustOpen(t, dir, smallOpts())
	defer db.Close()
	m.check(t, db, keyspace)
	// Keep writing after reopen: sequence numbers must continue correctly.
	if err := m.apply(rnd, db, 2000, keyspace); err != nil {
		t.Fatal(err)
	}
	m.check(t, db, keyspace)
}

func TestRecoveryFromWALWithoutFlush(t *testing.T) {
	dir := t.TempDir()
	opts := smallOpts()
	opts.NoFlushOnClose = true
	opts.MemtableBytes = 64 << 20
	db := mustOpen(t, dir, opts)
	for i := 0; i < 1000; i++ {
		db.Put([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	db.Delete([]byte("k5"))
	db.Close()

	db = mustOpen(t, dir, smallOpts())
	defer db.Close()
	s := db.Stats()
	if s.ReplayedRecords != 1001 {
		t.Fatalf("replayed %d records", s.ReplayedRecords)
	}
	if v, err := db.Get([]byte("k999")); err != nil || string(v) != "v999" {
		t.Fatalf("k999 = %q %v", v, err)
	}
	if _, err := db.Get([]byte("k5")); err != ErrNotFound {
		t.Fatal("tombstone not replayed")
	}
}

func TestTombstonesDroppedAtBottom(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	for i := 0; i < 2000; i++ {
		db.Put([]byte(fmt.Sprintf("k%05d", i)), bytes.Repeat([]byte("x"), 100))
	}
	for i := 0; i < 2000; i++ {
		db.Delete([]byte(fmt.Sprintf("k%05d", i)))
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	s := db.Stats()
	if s.TotalSSTBytes != 0 {
		t.Fatalf("expected all data gone after compaction, have %d bytes in %v", s.TotalSSTBytes, s.LevelFiles)
	}
	it := db.Scan(nil, nil)
	if it.Next() {
		t.Fatal("scan returned a deleted key")
	}
	it.Close()
}

func TestScanBoundsAndSnapshot(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		db.Put([]byte(k), []byte("v-"+k))
	}
	it := db.Scan([]byte("b"), []byte("d"))
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	it.Close()
	if fmt.Sprint(got) != "[b c]" {
		t.Fatalf("scan [b,d) = %v", got)
	}
	// An iterator opened before later writes does not see them.
	it = db.Scan(nil, nil)
	db.Put([]byte("a"), []byte("changed"))
	db.Put([]byte("zz"), []byte("new"))
	db.Delete([]byte("c"))
	got = got[:0]
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	it.Close()
	if fmt.Sprint(got) != "[a=v-a b=v-b c=v-c d=v-d e=v-e]" {
		t.Fatalf("snapshot violated: %v", got)
	}
}

func TestConcurrentWritersAndReaders(t *testing.T) {
	db := mustOpen(t, t.TempDir(), smallOpts())
	defer db.Close()
	const writers, perWriter = 8, 1500
	var writerWG, scanWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			for i := 0; i < perWriter; i++ {
				k := fmt.Sprintf("w%d-%05d", w, i)
				if err := db.Put([]byte(k), []byte(k)); err != nil {
					t.Error(err)
					return
				}
				if i%7 == 0 {
					if v, err := db.Get([]byte(k)); err != nil || string(v) != k {
						t.Errorf("read own write %s: %q %v", k, v, err)
						return
					}
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	scanWG.Add(1)
	go func() {
		defer scanWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			it := db.Scan(nil, nil)
			var prev []byte
			for it.Next() {
				if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
					t.Error("scan out of order")
				}
				if !bytes.Equal(it.Key(), it.Value()) {
					t.Errorf("value mismatch %q", it.Key())
				}
				prev = append(prev[:0], it.Key()...)
			}
			if it.Err() != nil {
				t.Error(it.Err())
			}
			it.Close()
		}
	}()
	writerWG.Wait()
	close(stop)
	scanWG.Wait()
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	it := db.Scan(nil, nil)
	n := 0
	for it.Next() {
		n++
	}
	it.Close()
	if n != writers*perWriter {
		t.Fatalf("have %d keys, want %d", n, writers*perWriter)
	}
}

func TestLockPreventsSecondOpen(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, smallOpts())
	defer db.Close()
	if _, err := Open(dir, smallOpts()); err == nil {
		t.Fatal("second open should fail")
	}
}
