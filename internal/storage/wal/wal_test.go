package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func replayAll(t *testing.T, dir string) [][]byte {
	t.Helper()
	l, err := Open(dir, Options{Mode: GroupCommit})
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	if err := l.Replay(func(_ uint64, p []byte) error {
		out = append(out, append([]byte(nil), p...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestAppendReplay(t *testing.T) {
	for _, mode := range []SyncMode{GroupCommit, SyncEach, NoSync} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			l, err := Open(dir, Options{Mode: mode, GroupCommitWindow: 50 * time.Microsecond})
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Start(); err != nil {
				t.Fatal(err)
			}
			const n = 200
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if err := l.Append([]byte(fmt.Sprintf("rec-%03d", i))).Wait(); err != nil {
						t.Error(err)
					}
				}(i)
			}
			wg.Wait()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			got := replayAll(t, dir)
			if len(got) != n {
				t.Fatalf("replayed %d records, want %d", len(got), n)
			}
		})
	}
}

func TestRotateAndRemove(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{Mode: GroupCommit})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Start(); err != nil {
		t.Fatal(err)
	}
	l.Append([]byte("a"))
	l.Append([]byte("b"))
	old, err := l.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("c")).Wait(); err != nil {
		t.Fatal(err)
	}
	segs, _ := l.Segments()
	if len(segs) != 2 || segs[0] != old {
		t.Fatalf("segments = %v", segs)
	}
	if err := l.Remove(old); err != nil {
		t.Fatal(err)
	}
	if err := l.Remove(l.CurrentSegment()); err == nil {
		t.Fatal("removing active segment must fail")
	}
	l.Close()
	got := replayAll(t, dir)
	if len(got) != 1 || string(got[0]) != "c" {
		t.Fatalf("got %q", got)
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{Mode: GroupCommit})
	l.Start()
	l.Append([]byte("keep-me"))
	l.Append([]byte("also-keep")).Wait()
	l.Close()

	path := filepath.Join(dir, fmt.Sprintf("%020d.wal", 1))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// A header claiming 100 bytes, followed by only 3.
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], 100)
	f.Write(hdr[:])
	f.Write([]byte("xyz"))
	f.Close()

	got := replayAll(t, dir)
	if len(got) != 2 {
		t.Fatalf("got %d records, want 2", len(got))
	}
	info, _ := os.Stat(path)
	if info.Size() != int64(2*8+len("keep-me")+len("also-keep")) {
		t.Fatalf("tail not truncated: size %d", info.Size())
	}
}

func TestBitFlipDetected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{Mode: GroupCommit})
	l.Start()
	l.Append([]byte("first")).Wait()
	l.Append([]byte("second")).Wait()
	l.Close()

	path := filepath.Join(dir, fmt.Sprintf("%020d.wal", 1))
	data, _ := os.ReadFile(path)
	data[8+2] ^= 0x01 // flip a bit inside "first"
	os.WriteFile(path, data, 0o644)

	got := replayAll(t, dir)
	// The corrupted record is in the only (hence newest) segment, so the
	// log is truncated at that point: nothing after it is trusted.
	if len(got) != 0 {
		t.Fatalf("got %d records after corruption, want 0", len(got))
	}
}

func TestCorruptionInOldSegmentIsAnError(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{Mode: GroupCommit})
	l.Start()
	l.Append([]byte("first")).Wait()
	l.Rotate()
	l.Append([]byte("second")).Wait()
	l.Close()

	path := filepath.Join(dir, fmt.Sprintf("%020d.wal", 1))
	data, _ := os.ReadFile(path)
	data[8+1] ^= 0x80
	os.WriteFile(path, data, 0o644)

	l2, _ := Open(dir, Options{Mode: GroupCommit})
	err := l2.Replay(func(uint64, []byte) error { return nil })
	if err == nil {
		t.Fatal("expected ErrCorrupt")
	}
}
