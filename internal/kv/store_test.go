package kv

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// forBothBackends runs a test on the memory backend and on the real engine, which
// have to behave the same.
func forBothBackends(t *testing.T, test func(t *testing.T, open func() Backend)) {
	t.Run("mem", func(t *testing.T) {
		b := NewMemBackend()
		test(t, func() Backend { return b })
	})
	t.Run("lsm", func(t *testing.T) {
		dir := t.TempDir()
		var open *LSM
		test(t, func() Backend {
			if open != nil {
				open.Close()
			}
			var err error
			if open, err = OpenLSM(dir); err != nil {
				t.Fatal(err)
			}
			return open
		})
		if open != nil {
			open.Close()
		}
	})
}

func openStore(t *testing.T, b Backend) *Store {
	t.Helper()
	s, err := OpenStore(b)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func entry(index uint64, cmd Command) raft.Entry {
	return raft.Entry{Index: index, Term: 1, Data: encodeCommand(cmd)}
}

func wantValue(t *testing.T, s *Store, key, want string) {
	t.Helper()
	v, found, err := s.Get([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	if want == "" && found {
		t.Fatalf("%s = %q, want missing", key, v)
	}
	if want != "" && string(v) != want {
		t.Fatalf("%s = %q (found %v), want %q", key, v, found, want)
	}
}

func TestCommandRoundTrip(t *testing.T) {
	c := Command{ClientID: 7, Seq: 1 << 40, Kind: Append, Key: []byte("k"), Value: []byte("v")}
	got, err := decodeCommand(encodeCommand(c))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(c) {
		t.Fatalf("got %+v", got)
	}
	data := encodeCommand(c)
	for n := 0; n < 6 && n < len(data); n++ {
		if _, err := decodeCommand(data[:n]); err == nil && n < 4 {
			t.Fatalf("record cut to %d bytes accepted", n)
		}
	}
}

func TestStoreAppliesCommands(t *testing.T) {
	forBothBackends(t, func(t *testing.T, open func() Backend) {
		s := openStore(t, open())
		s.Apply(entry(1, Command{ClientID: 1, Seq: 1, Kind: Put, Key: []byte("a"), Value: []byte("1")}))
		s.Apply(entry(2, Command{ClientID: 1, Seq: 2, Kind: Append, Key: []byte("a"), Value: []byte("2")}))
		s.Apply(entry(3, Command{ClientID: 1, Seq: 3, Kind: Put, Key: []byte("b"), Value: []byte("x")}))
		s.Apply(entry(4, Command{ClientID: 1, Seq: 4, Kind: Delete, Key: []byte("b")}))
		wantValue(t, s, "a", "12")
		wantValue(t, s, "b", "")
		if i, _ := s.Position(); i != 4 {
			t.Fatalf("position %d", i)
		}
	})
}

func TestNoOpAdvancesThePosition(t *testing.T) {
	forBothBackends(t, func(t *testing.T, open func() Backend) {
		s := openStore(t, open())
		s.Apply(raft.Entry{Index: 1, Term: 3, Kind: raft.EntryNoop})
		if i, term := s.Position(); i != 1 || term != 3 {
			t.Fatalf("position %d/%d", i, term)
		}
	})
}

func TestRepeatedWriteAppliesOnce(t *testing.T) {
	forBothBackends(t, func(t *testing.T, open func() Backend) {
		s := openStore(t, open())
		cmd := Command{ClientID: 1, Seq: 1, Kind: Append, Key: []byte("a"), Value: []byte("x")}
		if _, dup := s.Apply(entry(1, cmd)); dup {
			t.Fatal("the first try is not a repeat")
		}
		if _, dup := s.Apply(entry(2, cmd)); !dup {
			t.Fatal("the second try is a repeat")
		}
		wantValue(t, s, "a", "x")
		if i, _ := s.Position(); i != 2 {
			t.Fatalf("a dropped repeat must still advance the position: %d", i)
		}
		// A different client with the same Seq is a different write.
		s.Apply(entry(3, Command{ClientID: 2, Seq: 1, Kind: Append, Key: []byte("a"), Value: []byte("y")}))
		wantValue(t, s, "a", "xy")
	})
}

func TestSessionsSurviveARestart(t *testing.T) {
	forBothBackends(t, func(t *testing.T, open func() Backend) {
		s := openStore(t, open())
		cmd := Command{ClientID: 1, Seq: 5, Kind: Append, Key: []byte("a"), Value: []byte("x")}
		s.Apply(entry(1, cmd))
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
		s = openStore(t, open())
		if i, _ := s.Position(); i != 1 {
			t.Fatalf("position %d", i)
		}
		if _, dup := s.Apply(entry(2, cmd)); !dup {
			t.Fatal("a restart forgot the session")
		}
		wantValue(t, s, "a", "x")
	})
}

func TestSnapshotRoundTrip(t *testing.T) {
	forBothBackends(t, func(t *testing.T, open func() Backend) {
		s := openStore(t, open())
		s.Apply(entry(1, Command{ClientID: 1, Seq: 1, Kind: Put, Key: []byte("a"), Value: []byte("1")}))
		s.Apply(entry(2, Command{ClientID: 2, Seq: 9, Kind: Put, Key: []byte("b"), Value: []byte("2")}))
		data := s.Snapshot()

		other := openStore(t, NewMemBackend())
		other.Apply(entry(1, Command{ClientID: 3, Seq: 1, Kind: Put, Key: []byte("stale"), Value: []byte("gone")}))
		other.Restore(2, 7, data)
		wantValue(t, other, "a", "1")
		wantValue(t, other, "b", "2")
		wantValue(t, other, "stale", "")
		if i, term := other.Position(); i != 2 || term != 7 {
			t.Fatalf("position %d/%d", i, term)
		}
		// The sessions came along, so a retried write is still dropped.
		if _, dup := other.Apply(entry(3, Command{ClientID: 2, Seq: 9, Kind: Put, Key: []byte("b"), Value: []byte("again")})); !dup {
			t.Fatal("a snapshot lost the session table")
		}
		if string(other.Snapshot()) != string(data) {
			t.Fatal("a dropped repeat must change nothing")
		}
	})
}

func TestCorruptSnapshotIsRefused(t *testing.T) {
	s := openStore(t, NewMemBackend())
	s.Apply(entry(1, Command{ClientID: 1, Seq: 1, Kind: Put, Key: []byte("a"), Value: []byte("1")}))
	data := s.Snapshot()
	data[1] ^= 1
	defer func() {
		if recover() == nil {
			t.Fatal("a damaged snapshot must not be loaded")
		}
	}()
	s.Restore(9, 1, data)
}

// ---- durability -----------------------------------------------------------------

func TestPowerLossKeepsOnlyWholeBatches(t *testing.T) {
	b := NewMemBackend()
	s := openStore(t, b)
	s.Apply(entry(1, Command{ClientID: 1, Seq: 1, Kind: Append, Key: []byte("a"), Value: []byte("1")}))
	s.Flush()
	s.Apply(entry(2, Command{ClientID: 1, Seq: 2, Kind: Append, Key: []byte("a"), Value: []byte("2")}))
	s.Apply(entry(3, Command{ClientID: 1, Seq: 3, Kind: Append, Key: []byte("a"), Value: []byte("3")}))
	b.Crash(1)

	s = openStore(t, b)
	wantValue(t, s, "a", "12")
	if i, _ := s.Position(); i != 2 {
		t.Fatalf("position %d: the store must say exactly how far it got", i)
	}
	// Raft replays entry 3 from its log, and it is not a repeat.
	if _, dup := s.Apply(entry(3, Command{ClientID: 1, Seq: 3, Kind: Append, Key: []byte("a"), Value: []byte("3")})); dup {
		t.Fatal("the session must have been lost with the write")
	}
	wantValue(t, s, "a", "123")
}

// The real engine has to honor the same promise as MemBackend: losing the end of
// its WAL leaves an earlier position, with no hole.
func TestLSMPowerLossLeavesAPrefix(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := openStore(t, l)
	for i := uint64(1); i <= 3; i++ {
		s.Apply(entry(i, Command{ClientID: 1, Seq: i, Kind: Append, Key: []byte("a"), Value: []byte(fmt.Sprint(i))}))
	}
	l.Close()

	segs, _ := filepath.Glob(filepath.Join(l.genDir(l.gen), "*.wal"))
	var path string
	for _, seg := range segs {
		if info, _ := os.Stat(seg); info.Size() > 0 {
			path = seg
		}
	}
	data, _ := os.ReadFile(path)
	// Damage the middle: the page cache can lose earlier pages and keep later ones.
	data[len(data)/2] ^= 0x01
	os.WriteFile(path, data, 0o644)

	l, err = OpenLSM(dir)
	if err != nil {
		t.Fatalf("damage to unsynced data must not stop a restart: %v", err)
	}
	defer l.Close()
	s = openStore(t, l)
	i, _ := s.Position()
	if i != 1 {
		t.Fatalf("position %d, want 1: the store must stop at the damage", i)
	}
	wantValue(t, s, "a", "1")
}

func TestReplaceSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.Apply([]Write{{Key: []byte("old"), Value: []byte("x")}})
	if err := l.Replace([]Write{{Key: []byte("new"), Value: []byte("y")}}); err != nil {
		t.Fatal(err)
	}
	l.Close()

	l, err = OpenLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, found, _ := l.Get([]byte("old")); found {
		t.Fatal("old contents survived Replace")
	}
	if v, _, _ := l.Get([]byte("new")); string(v) != "y" {
		t.Fatalf("new = %q", v)
	}
}

// A crash before CURRENT is renamed leaves a half-built generation, which must be
// ignored and removed.
func TestCrashDuringReplaceKeepsTheOldData(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	l.Apply([]Write{{Key: []byte("old"), Value: []byte("x")}})
	l.Flush()
	half := l.genDir(l.gen + 1)
	l.Close()
	if err := os.MkdirAll(half, 0o755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(half, "000001.sst"), []byte("half written"), 0o644)

	l, err = OpenLSM(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if v, _, _ := l.Get([]byte("old")); string(v) != "x" {
		t.Fatalf("old = %q", v)
	}
	if _, err := os.Stat(half); !os.IsNotExist(err) {
		t.Fatal("the half-built generation was left behind")
	}
}
