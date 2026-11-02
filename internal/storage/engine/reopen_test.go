package engine

import (
	"fmt"
	"testing"
)

// Regression: after reopen, L0 must still be searched newest-first.
func TestReopenKeepsL0Order(t *testing.T) {
	dir := t.TempDir()
	opts := smallOpts()
	opts.DisableAutoCompaction = true
	db := mustOpen(t, dir, opts)
	for round := 0; round < 3; round++ {
		for i := 0; i < 50; i++ {
			db.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("round%d", round)))
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	if s := db.Stats(); s.LevelFiles[0] != 3 {
		t.Fatalf("want 3 L0 files, have %v", s.LevelFiles)
	}
	db.Close()
	db = mustOpen(t, dir, opts)
	defer db.Close()
	for i := 0; i < 50; i++ {
		v, err := db.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || string(v) != "round2" {
			t.Fatalf("k%03d = %q %v after reopen", i, v, err)
		}
	}
}
