package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

func openFile(t *testing.T, dir string) *FileStorage {
	t.Helper()
	s, err := OpenFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func save(t *testing.T, s Storage, u Update) {
	t.Helper()
	if err := s.Save(u); err != nil {
		t.Fatal(err)
	}
}

func wantState(t *testing.T, s Storage, hard HardState, terms ...uint64) {
	t.Helper()
	d, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if d.Hard != hard {
		t.Fatalf("hard state %+v, want %+v", d.Hard, hard)
	}
	got := make([]uint64, len(d.Entries))
	for i, e := range d.Entries {
		if want := d.Snapshot.Index + uint64(i+1); e.Index != want {
			t.Fatalf("entry %d has index %d, want %d", i, e.Index, want)
		}
		got[i] = e.Term
	}
	if fmt.Sprint(got) != fmt.Sprint(terms) {
		t.Fatalf("entry terms %v, want %v", got, terms)
	}
}

func TestUpdateEncodingRoundTrip(t *testing.T) {
	cases := []Update{
		{HardState: &HardState{CurrentTerm: 7, VotedFor: 3}},
		{TruncateFrom: 9},
		{Entries: []Entry{{Index: 1, Term: 2, Kind: EntryNoop}, {Index: 2, Term: 2, Data: []byte("x")}}},
		{HardState: &HardState{CurrentTerm: 1 << 40}, TruncateFrom: 3, Entries: []Entry{{Index: 3, Term: 9, Data: []byte("data")}}},
		{HardState: &HardState{CurrentTerm: 9}, Snapshot: &Snapshot{Index: 40, Term: 8}, Entries: []Entry{{Index: 41, Term: 9, Data: []byte("x")}}},
	}
	for _, u := range cases {
		got, err := decodeUpdate(encodeUpdate(u))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, u) {
			t.Fatalf("got %+v want %+v", got, u)
		}
	}
}

func TestDecodeRejectsMalformedRecords(t *testing.T) {
	good := encodeUpdate(Update{HardState: &HardState{CurrentTerm: 7}, Entries: logWithTerms(1, 2)})
	if _, err := decodeUpdate(append(good[:len(good):len(good)], 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}
	for n := range good {
		if _, err := decodeUpdate(good[:n]); err == nil {
			t.Fatalf("record cut to %d bytes accepted", n)
		}
	}
}

func TestFileStorageReplaysEveryUpdate(t *testing.T) {
	dir := t.TempDir()
	s := openFile(t, dir)
	save(t, s, Update{HardState: &HardState{CurrentTerm: 1, VotedFor: 2}})
	save(t, s, Update{Entries: logWithTerms(1, 1, 1, 1)})
	save(t, s, Update{HardState: &HardState{CurrentTerm: 2}})
	save(t, s, Update{TruncateFrom: 3, Entries: []Entry{{Index: 3, Term: 2}}})
	save(t, s, Update{TruncateFrom: 2})
	save(t, s, Update{Entries: []Entry{{Index: 2, Term: 2}, {Index: 3, Term: 2}}})
	s.Close()

	// Several reopens, because each one starts a new WAL segment.
	for i := 0; i < 3; i++ {
		s = openFile(t, dir)
		wantState(t, s, HardState{CurrentTerm: 2}, 1, 2, 2)
		s.Close()
	}
}

func TestFileStorageKeepsEntryContents(t *testing.T) {
	dir := t.TempDir()
	s := openFile(t, dir)
	saved := []Entry{{Index: 1, Term: 1, Kind: EntryNoop}, {Index: 2, Term: 1, Data: []byte("payload")}}
	save(t, s, Update{Entries: saved})
	s.Close()
	s = openFile(t, dir)
	defer s.Close()
	if d, _ := s.Load(); !reflect.DeepEqual(d.Entries, saved) {
		t.Fatalf("got %+v want %+v", d.Entries, saved)
	}
}

func newestSegment(t *testing.T, dir string) string {
	t.Helper()
	segs, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	if err != nil || len(segs) == 0 {
		t.Fatalf("no segments: %v", err)
	}
	sort.Strings(segs)
	return segs[len(segs)-1]
}

// A Save that was cut off never returned, so nothing depended on it.
func TestFileStorageDropsTornTail(t *testing.T) {
	dir := t.TempDir()
	s := openFile(t, dir)
	save(t, s, Update{HardState: &HardState{CurrentTerm: 4, VotedFor: 2}, Entries: logWithTerms(1, 4)})
	s.Close()

	f, err := os.OpenFile(newestSegment(t, dir), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{200, 0, 0, 0, 1, 2, 3, 4, 5}) // a header claiming 200 bytes, then 1
	f.Close()

	s = openFile(t, dir)
	defer s.Close()
	wantState(t, s, HardState{CurrentTerm: 4, VotedFor: 2}, 1, 4)
}

// A node that lost a vote or entry it acknowledged could break safety, so
// damaged durable state must stop it from starting at all.
func TestFileStorageRefusesCorruption(t *testing.T) {
	dir := t.TempDir()
	s := openFile(t, dir)
	save(t, s, Update{HardState: &HardState{CurrentTerm: 4, VotedFor: 2}})
	save(t, s, Update{Entries: logWithTerms(1)})
	s.Close()

	path := newestSegment(t, dir)
	data, _ := os.ReadFile(path)
	data[9] ^= 0x01 // inside the first record, which holds the vote
	os.WriteFile(path, data, 0o644)

	if _, err := OpenFileStorage(dir); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("got %v, want wal.ErrCorrupt", err)
	}
}

func TestNodeRestoresFromFileStorage(t *testing.T) {
	dir := t.TempDir()
	start := func(s *FileStorage) (*Node, *recorder) {
		r := &recorder{store: NewMemStorage()}
		n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Storage: s, Transport: r, StateMachine: r, Reads: r})
		if err != nil {
			t.Fatal(err)
		}
		return n, r
	}
	s := openFile(t, dir)
	n, _ := start(s)
	receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, Entries: logWithTerms(1, 2)})
	receive(t, n, 3, RequestVoteArgs{Term: 3, CandidateID: 3, LastLogIndex: 2, LastLogTerm: 2})
	s.Close()

	s = openFile(t, dir)
	defer s.Close()
	n, r := start(s)
	st := n.Status()
	if st.Term != 3 || st.VotedFor != 3 || st.LastLogIndex != 2 || st.LastLogTerm != 2 {
		t.Fatalf("restored %+v", st)
	}
	receive(t, n, 2, RequestVoteArgs{Term: 3, CandidateID: 2, LastLogIndex: 9, LastLogTerm: 9})
	if got := sentOf[RequestVoteReply](r); len(got) != 1 || got[0].VoteGranted {
		t.Fatalf("voted twice after restart: %+v", got)
	}
}
