package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
)

func buildTable(t *testing.T, n int, bloom int) (string, *Stats) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.sst")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, WriterOptions{BlockBytes: 512, BloomBitsPerKey: bloom})
	for i := 0; i < n; i++ {
		k := ikey.Encode(nil, []byte(fmt.Sprintf("key%06d", i*2)), uint64(i+1), ikey.KindValue)
		if err := w.Add(k, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	p, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if p.NumEntries != uint64(n) || p.MinSeq != 1 || p.MaxSeq != uint64(n) {
		t.Fatalf("props %+v", p)
	}
	return path, &Stats{}
}

func TestWriteReadSeek(t *testing.T) {
	const n = 1000
	path, stats := buildTable(t, n, 10)
	r, err := Open(path, stats)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.props.NumBlocks < 10 {
		t.Fatalf("expected many blocks, got %d", r.props.NumBlocks)
	}
	// Present keys.
	for i := 0; i < n; i += 37 {
		uk := []byte(fmt.Sprintf("key%06d", i*2))
		k, v, found, err := r.Get(ikey.SearchKey(nil, uk, ikey.MaxSeq))
		if err != nil || !found || string(ikey.UserKey(k)) != string(uk) || string(v) != fmt.Sprintf("value-%d", i) {
			t.Fatalf("get %s: %q %q %v %v", uk, k, v, found, err)
		}
	}
	// Missing key between two present keys lands on the next one.
	k, _, found, _ := r.Get(ikey.SearchKey(nil, []byte("key000003"), ikey.MaxSeq))
	if !found || string(ikey.UserKey(k)) != "key000004" {
		t.Fatalf("seek between keys landed on %q", k)
	}
	// Past the end.
	if _, _, found, _ := r.Get(ikey.SearchKey(nil, []byte("zzz"), ikey.MaxSeq)); found {
		t.Fatal("found key past end")
	}
	// Full scan is ordered and complete.
	it := r.Iter()
	if err := it.SeekToFirst(); err != nil {
		t.Fatal(err)
	}
	count := 0
	var prev []byte
	for ; it.Valid(); it.Next() {
		if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
			t.Fatal("out of order")
		}
		prev = append(prev[:0], it.Key()...)
		count++
	}
	if it.Err() != nil || count != n {
		t.Fatalf("scanned %d err %v", count, it.Err())
	}
}

func TestBloomFilter(t *testing.T) {
	keys := make([][]byte, 10000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("present-%d", i))
	}
	f := BuildBloom(keys, 10)
	for _, k := range keys {
		if !BloomMayContain(f, k) {
			t.Fatalf("false negative for %s", k)
		}
	}
	fp := 0
	const probes = 100000
	for i := 0; i < probes; i++ {
		if BloomMayContain(f, []byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / probes
	if rate > 0.02 {
		t.Fatalf("false positive rate %.4f too high for 10 bits/key", rate)
	}
	t.Logf("false positive rate %.4f", rate)
}

func TestCorruptBlockDetected(t *testing.T) {
	path, stats := buildTable(t, 200, 10)
	data, _ := os.ReadFile(path)
	data[10] ^= 0xFF // inside the first data block
	os.WriteFile(path, data, 0o644)
	r, err := Open(path, stats)
	if err != nil {
		t.Fatal(err) // index/footer untouched, open succeeds
	}
	defer r.Close()
	_, _, _, err = r.Get(ikey.SearchKey(nil, []byte("key000000"), ikey.MaxSeq))
	if err == nil {
		t.Fatal("expected corruption error")
	}
}
