package memtable

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
)

func TestOrderedInsertAndSeek(t *testing.T) {
	tb := New()
	rnd := rand.New(rand.NewPCG(1, 2))
	want := map[string]string{}
	seq := uint64(1)
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key%05d", rnd.IntN(2000))
		v := fmt.Sprintf("val%d", i)
		tb.Put(ikey.Encode(nil, []byte(k), seq, ikey.KindValue), []byte(v))
		seq++
		want[k] = v
	}
	// Iteration must be sorted and newest-first within a key.
	it := tb.Iter()
	it.SeekToFirst()
	var prev []byte
	n := 0
	for ; it.Valid(); it.Next() {
		if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
			t.Fatal("iterator out of order")
		}
		prev = append(prev[:0], it.Key()...)
		n++
	}
	if n != 5000 {
		t.Fatalf("iterated %d, want 5000", n)
	}
	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fk, fv, ok := tb.Seek(ikey.SearchKey(nil, []byte(k), ikey.MaxSeq))
		if !ok || string(ikey.UserKey(fk)) != k || string(fv) != want[k] {
			t.Fatalf("seek %s: ok=%v key=%q val=%q want %q", k, ok, fk, fv, want[k])
		}
	}
	if _, _, ok := tb.Seek(ikey.SearchKey(nil, []byte("zzz"), ikey.MaxSeq)); ok {
		t.Fatal("seek past end should be invalid")
	}
}

func TestConcurrentReadersSingleWriter(t *testing.T) {
	tb := New()
	const n = 20000
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				it := tb.Iter()
				it.SeekToFirst()
				var prev []byte
				for ; it.Valid(); it.Next() {
					if prev != nil && ikey.Compare(prev, it.Key()) >= 0 {
						t.Error("reader saw out-of-order keys")
						return
					}
					prev = it.Key()
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%08d", (i*7919)%n)
		tb.Put(ikey.Encode(nil, []byte(k), uint64(i+1), ikey.KindValue), []byte("v"))
	}
	close(stop)
	wg.Wait()
	if tb.Count() != n {
		t.Fatalf("count %d", tb.Count())
	}
}
