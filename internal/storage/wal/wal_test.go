package wal

import (
	"encoding/binary"
	"errors"
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

// writeRecords returns the path of a one-segment log holding the given payloads.
func writeRecords(t *testing.T, dir string, payloads ...string) string {
	t.Helper()
	l, _ := Open(dir, Options{Mode: GroupCommit})
	l.Start()
	for _, p := range payloads {
		if err := l.Append([]byte(p)).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	return filepath.Join(dir, fmt.Sprintf("%020d.wal", 1))
}

func TestMidSegmentCorruptionIsAnError(t *testing.T) {
	cases := []struct {
		name   string
		offset int
	}{
		{"payload", 8 + 2}, // inside "first"
		{"crc", 5},
		{"length", 1}, // the length can no longer be trusted to find "second"
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := writeRecords(t, dir, "first", "second")
			data, _ := os.ReadFile(path)
			data[c.offset] ^= 0x01
			os.WriteFile(path, data, 0o644)

			l, _ := Open(dir, Options{Mode: GroupCommit})
			err := l.Replay(func(uint64, []byte) error { return nil })
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Replay returned %v, want ErrCorrupt: \"second\" is valid and must not be dropped", err)
			}
			if info, _ := os.Stat(path); info.Size() != int64(len(data)) {
				t.Fatalf("a corrupt log must be left alone, size went from %d to %d", len(data), info.Size())
			}
		})
	}
}

// Damage to the last record looks exactly like a write that never finished.
func TestDamagedLastRecordIsTreatedAsTornTail(t *testing.T) {
	dir := t.TempDir()
	path := writeRecords(t, dir, "first", "second")
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0x01
	os.WriteFile(path, data, 0o644)

	got := replayAll(t, dir)
	if len(got) != 1 || string(got[0]) != "first" {
		t.Fatalf("got %q, want only first", got)
	}
}

// Without fsync, nothing after the damage was promised, and keeping it would
// leave a hole. The caller replays what is missing from somewhere else.
func TestNoSyncLogKeepsThePrefixBeforeDamage(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{Mode: NoSync})
	l.Start()
	for _, p := range []string{"first", "second", "third"} {
		if err := l.Append([]byte(p)).Wait(); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	path := filepath.Join(dir, fmt.Sprintf("%020d.wal", 1))
	data, _ := os.ReadFile(path)
	data[8+len("first")+8+1] ^= 0x01 // inside "second"
	os.WriteFile(path, data, 0o644)

	l2, _ := Open(dir, Options{Mode: NoSync})
	var got []string
	if err := l2.Replay(func(_ uint64, p []byte) error {
		got = append(got, string(p))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != "[first]" {
		t.Fatalf("got %v, want only the prefix before the damage", got)
	}
}

// A finished segment must not depend on the page cache, or a power cut could keep
// the next segment and lose the end of this one.
func TestNoSyncRotateMakesTheSegmentDurable(t *testing.T) {
	l, _ := Open(t.TempDir(), Options{Mode: NoSync})
	l.Start()
	defer l.Close()
	l.Append([]byte("x")).Wait()
	if n := l.Stats().Syncs.Load(); n != 0 {
		t.Fatalf("%d fsyncs before rotating", n)
	}
	if _, err := l.Rotate(); err != nil {
		t.Fatal(err)
	}
	if n := l.Stats().Syncs.Load(); n != 1 {
		t.Fatalf("%d fsyncs after rotating, want 1", n)
	}
}

func TestOversizeRecordRejected(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{Mode: GroupCommit})
	l.Start()
	defer l.Close()
	if err := l.Append(make([]byte, MaxRecordBytes+1)).Wait(); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Append returned %v, want ErrTooLarge", err)
	}
	if err := l.Append([]byte("still works")).Wait(); err != nil {
		t.Fatalf("a rejected record must not poison the log: %v", err)
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
