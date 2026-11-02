package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// logFrom returns entries starting at index first, with the given terms.
func logFrom(first uint64, terms ...uint64) []Entry {
	entries := make([]Entry, len(terms))
	for i, term := range terms {
		entries[i] = Entry{Index: first + uint64(i), Term: term, Data: []byte(fmt.Sprintf("cmd%d", first+uint64(i)))}
	}
	return entries
}

// followerWith returns node 1 as a follower holding a log, with commit reached.
func followerWith(t *testing.T, term, commit uint64, terms ...uint64) (*Node, *recorder, *MemStorage) {
	t.Helper()
	store := storeWith(t, term, None, logFrom(1, terms...)...)
	n, r := newNode(t, store, 1, 2, 3)
	if commit > 0 {
		receive(t, n, 2, AppendEntriesArgs{Term: term, LeaderID: 2, PrevLogIndex: uint64(len(terms)), PrevLogTerm: terms[len(terms)-1], LeaderCommit: commit})
	}
	r.clear()
	return n, r, store
}

// ---- log arithmetic -------------------------------------------------------------

func TestLogAfterCompaction(t *testing.T) {
	var l raftLog
	l.append(logFrom(1, 1, 1, 2, 2, 3)...)
	l.compactTo(3)
	if l.lastIndex() != 5 || l.term(3) != 2 || l.term(5) != 3 || l.entry(4).Index != 4 {
		t.Fatalf("log after compaction: %+v", l)
	}
	if got := l.slice(4, 10); len(got) != 2 || got[0].Index != 4 {
		t.Fatalf("slice %+v", got)
	}
	if got := l.firstIndexOfTerm(4); got != 4 {
		t.Fatalf("firstIndexOfTerm stopped at %d: index 3 is gone", got)
	}
	if got := l.lastIndexOfTerm(2); got != 4 {
		t.Fatalf("lastIndexOfTerm(2) = %d", got)
	}
	l.compactTo(5)
	if got := l.lastIndexOfTerm(3); got != 5 {
		t.Fatalf("lastIndexOfTerm found %d: the snapshot's own term counts", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("reading a discarded entry must panic")
		}
	}()
	l.entry(5)
}

// ---- compaction -----------------------------------------------------------------

func TestCompactLogDiscardsAppliedEntries(t *testing.T) {
	n, _, store := followerWith(t, 2, 3, 1, 2, 2, 2)
	if err := n.CompactLog(2); err != nil {
		t.Fatal(err)
	}
	st := n.Status()
	if st.SnapshotIndex != 2 || st.LastLogIndex != 4 {
		t.Fatalf("after compaction: %+v", st)
	}
	d, _ := store.Load()
	if d.Snapshot != (Snapshot{2, 2}) || len(d.Entries) != 2 || d.Entries[0].Index != 3 || d.Hard.CurrentTerm != 2 {
		t.Fatalf("durable state %+v", d)
	}
}

func TestCompactLogRefusesUnappliedEntries(t *testing.T) {
	n, _, _ := followerWith(t, 2, 1, 1, 2)
	if err := n.CompactLog(2); err == nil {
		t.Fatal("compacting past lastApplied must fail: the state machine does not hold those entries")
	}
}

func TestCompactLogIsANoOpBehindTheSnapshot(t *testing.T) {
	n, r, _ := followerWith(t, 2, 3, 1, 2, 2)
	n.CompactLog(3)
	r.clear()
	if err := n.CompactLog(2); err != nil || len(r.events) != 0 {
		t.Fatalf("err %v, events %v", err, r.events)
	}
}

func TestCompactedNodeStillAcceptsAppends(t *testing.T) {
	n, r, _ := followerWith(t, 2, 3, 1, 2, 2)
	n.CompactLog(3)
	r.clear()
	// Prev is the snapshot itself, which has no entry left to compare with.
	receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 2, Entries: logFrom(4, 2)})
	if got := sentOf[AppendEntriesReply](r); len(got) != 1 || !got[0].Success || got[0].MatchIndex != 4 {
		t.Fatalf("got %+v", got)
	}
}

// A delayed message can refer to entries this node has already discarded. They
// were committed, so they match, and the part past the snapshot still counts.
func TestDelayedAppendAcrossTheSnapshot(t *testing.T) {
	n, r, _ := followerWith(t, 2, 3, 1, 2, 2)
	n.CompactLog(3)
	r.clear()
	receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: logFrom(2, 2, 2, 2)})
	if got := sentOf[AppendEntriesReply](r); len(got) != 1 || !got[0].Success || got[0].MatchIndex != 4 {
		t.Fatalf("got %+v", got)
	}
	if st := n.Status(); st.LastLogIndex != 4 {
		t.Fatalf("log ends at %d", st.LastLogIndex)
	}
}

// ---- restart --------------------------------------------------------------------

func TestRestartAfterCompactionKeepsPosition(t *testing.T) {
	n, r, store := followerWith(t, 2, 3, 1, 2, 2, 2)
	n.CompactLog(3)

	restarted, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Storage: store, Transport: r, StateMachine: r, Reads: r})
	if err != nil {
		t.Fatal(err)
	}
	if st := restarted.Status(); st.SnapshotIndex != 3 || st.LastLogIndex != 4 || st.LastApplied != 3 || st.CommitIndex != 3 {
		t.Fatalf("restored %+v", st)
	}
}

func TestStateMachineBehindTheSnapshotIsAnError(t *testing.T) {
	store := NewMemStorage()
	store.Save(Update{HardState: &HardState{CurrentTerm: 2}, Snapshot: &Snapshot{5, 2}})
	r := &recorder{store: store} // the state machine starts empty
	if _, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Storage: r, Transport: r, StateMachine: r, Reads: r}); err == nil {
		t.Fatal("the entries through 5 are gone, so an empty state machine can never catch up")
	}
}

// The crash came after Restore finished and before Save. The state machine holds
// committed data the log has not caught up with, so the log has to follow it.
func TestRestartFinishesAnInterruptedInstall(t *testing.T) {
	cases := []struct {
		name  string
		terms []uint64
	}{
		{"log too short", []uint64{1}},
		{"log conflicts", []uint64{1, 1, 1, 1, 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := storeWith(t, 3, None, logFrom(1, c.terms...)...)
			r := &recorder{store: store, position: Snapshot{4, 3}}
			n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, Storage: r, Transport: r, StateMachine: r, Reads: r})
			if err != nil {
				t.Fatal(err)
			}
			if st := n.Status(); st.SnapshotIndex != 4 || st.LastLogIndex != 4 || st.LastApplied != 4 || st.CommitIndex != 4 {
				t.Fatalf("restored %+v", st)
			}
			if d, _ := store.Load(); d.Snapshot != (Snapshot{4, 3}) || len(d.Entries) != 0 {
				t.Fatalf("durable state %+v", d)
			}
		})
	}
}

// ---- InstallSnapshot ------------------------------------------------------------

func TestInstallSnapshotReplacesAConflictingLog(t *testing.T) {
	n, r, store := followerWith(t, 3, 0, 1, 1, 1)
	receive(t, n, 2, InstallSnapshotArgs{Term: 3, LeaderID: 2, LastIncludedIndex: 2, LastIncludedTerm: 3, Data: []byte("state")})
	st := n.Status()
	if st.SnapshotIndex != 2 || st.LastLogIndex != 2 || st.CommitIndex != 2 || st.LastApplied != 2 || st.Leader != 2 {
		t.Fatalf("after install: %+v", st)
	}
	if string(r.restored) != "state" {
		t.Fatalf("restored %q", r.restored)
	}
	if d, _ := store.Load(); d.Snapshot != (Snapshot{2, 3}) || len(d.Entries) != 0 {
		t.Fatalf("durable state %+v", d)
	}
	if got := sentOf[InstallSnapshotReply](r); len(got) != 1 || got[0].MatchIndex != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestInstallSnapshotKeepsAMatchingTail(t *testing.T) {
	n, r, store := followerWith(t, 3, 0, 1, 2, 3, 3)
	receive(t, n, 2, InstallSnapshotArgs{Term: 3, LeaderID: 2, LastIncludedIndex: 2, LastIncludedTerm: 2, Data: []byte("state")})
	if st := n.Status(); st.LastLogIndex != 4 || st.LastApplied != 2 || st.CommitIndex != 2 {
		t.Fatalf("Figure 13 step 6 keeps entries after the snapshot: %+v", st)
	}
	if fmt.Sprint(r.events) != "[restore send]" {
		t.Fatalf("events %v: nothing on disk needs to change", r.events)
	}
	if d, _ := store.Load(); len(d.Entries) != 4 {
		t.Fatalf("durable log %+v", d.Entries)
	}
}

// A delayed snapshot must not take the state machine backwards.
func TestStaleSnapshotDoesNothing(t *testing.T) {
	n, r, _ := followerWith(t, 2, 3, 1, 2, 2)
	receive(t, n, 2, InstallSnapshotArgs{Term: 2, LeaderID: 2, LastIncludedIndex: 2, LastIncludedTerm: 2, Data: []byte("old")})
	if r.restored != nil {
		t.Fatal("restored an old snapshot over newer state")
	}
	if got := sentOf[InstallSnapshotReply](r); len(got) != 1 || got[0].MatchIndex != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestSnapshotFromOldTermIsRefused(t *testing.T) {
	n, r, _ := followerWith(t, 5, 0, 1)
	receive(t, n, 2, InstallSnapshotArgs{Term: 4, LeaderID: 2, LastIncludedIndex: 9, LastIncludedTerm: 4})
	if r.restored != nil {
		t.Fatal("a deposed leader's snapshot was installed")
	}
	if got := sentOf[InstallSnapshotReply](r); len(got) != 1 || got[0].Term != 5 {
		t.Fatalf("got %+v", got)
	}
}

// If the crash comes between the two, this order leaves a state machine ahead of
// the log, which restart can repair. The other order leaves a log that says the
// entries are gone and a state machine that never got them.
func TestRestoreHappensBeforeSave(t *testing.T) {
	n, r, _ := followerWith(t, 3, 0, 1)
	receive(t, n, 2, InstallSnapshotArgs{Term: 3, LeaderID: 2, LastIncludedIndex: 5, LastIncludedTerm: 3})
	if fmt.Sprint(r.events) != "[restore save send]" {
		t.Fatalf("events %v", r.events)
	}
}

func TestLeaderSendsSnapshotToALaggingPeer(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	term := n.Status().Term
	for i := 0; i < 3; i++ {
		n.Propose([]byte("x"))
	}
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 4})
	n.CompactLog(n.Status().LastApplied)
	snap := n.Status().SnapshotIndex
	r.clear()

	// Node 3 has acknowledged nothing, so the next heartbeat finds its entries gone.
	for i := 0; i < n.cfg.HeartbeatTicks; i++ {
		n.Tick()
	}
	got := sentOf[InstallSnapshotArgs](r)
	if len(got) != 1 || got[0].LastIncludedIndex != snap || got[0].LastIncludedTerm != term || string(got[0].Data) != fmt.Sprintf("state@%d", snap) {
		t.Fatalf("got %+v, want a snapshot through %d", got, snap)
	}

	// Heartbeats must keep flowing, but the snapshot is not rebuilt each time.
	r.clear()
	for i := 0; i < n.cfg.HeartbeatTicks; i++ {
		n.Tick()
	}
	if len(sentOf[InstallSnapshotArgs](r)) != 0 || len(sentOf[AppendEntriesArgs](r)) == 0 {
		t.Fatalf("sent %+v", r.sent)
	}
	r.clear()
	for i := 0; i < n.cfg.ElectionTicks; i++ {
		n.Tick()
	}
	if len(sentOf[InstallSnapshotArgs](r)) == 0 {
		t.Fatal("a lost snapshot was never sent again")
	}

	// The reply moves the peer back onto ordinary replication.
	r.clear()
	receive(t, n, 3, InstallSnapshotReply{Term: term, MatchIndex: snap})
	if n.matchIndex[3] != snap || n.nextIndex[3] != snap+1 {
		t.Fatalf("match %d next %d", n.matchIndex[3], n.nextIndex[3])
	}
}

// ---- file storage ---------------------------------------------------------------

func walBytes(t *testing.T, dir string) int64 {
	t.Helper()
	segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	var total int64
	for _, s := range segs {
		info, err := os.Stat(s)
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func TestCompactionShrinksTheLog(t *testing.T) {
	dir := t.TempDir()
	s := openFile(t, dir)
	save(t, s, Update{HardState: &HardState{CurrentTerm: 2}, Entries: logFrom(1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2)})
	for i := uint64(11); i <= 200; i++ {
		save(t, s, Update{Entries: logFrom(i, 2)})
	}
	before := walBytes(t, dir)
	save(t, s, Update{HardState: &HardState{CurrentTerm: 2}, Snapshot: &Snapshot{199, 2}, Entries: logFrom(200, 2)})
	after := walBytes(t, dir)
	if after*10 > before {
		t.Fatalf("log went from %d to %d bytes", before, after)
	}
	s.Close()

	s = openFile(t, dir)
	defer s.Close()
	d, _ := s.Load()
	if d.Snapshot != (Snapshot{199, 2}) || len(d.Entries) != 1 || d.Entries[0].Index != 200 || d.Hard.CurrentTerm != 2 {
		t.Fatalf("after restart: %+v", d)
	}
}

// A crash can stop Save between writing the snapshot record and deleting old
// segments, partway through deleting them, or before the record is durable. Every
// one of those states has to replay.
func TestSnapshotSurvivesACrashAtAnyStep(t *testing.T) {
	build := func(t *testing.T) (string, []string) {
		dir := t.TempDir()
		s := openFile(t, dir)
		save(t, s, Update{HardState: &HardState{CurrentTerm: 2}, Entries: logFrom(1, 2, 2)})
		s.Close()
		s = openFile(t, dir) // a second segment
		save(t, s, Update{Entries: logFrom(3, 2, 2)})
		s.Close()
		s = openFile(t, dir) // a third, active segment
		segs, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
		save(t, s, Update{HardState: &HardState{CurrentTerm: 2}, Snapshot: &Snapshot{3, 2}, Entries: logFrom(4, 2)})
		s.Close()
		return dir, segs
	}
	wantSnapshot := func(t *testing.T, dir string) {
		t.Helper()
		s := openFile(t, dir)
		defer s.Close()
		d, _ := s.Load()
		if d.Snapshot != (Snapshot{3, 2}) || len(d.Entries) != 1 || d.Entries[0].Index != 4 {
			t.Fatalf("recovered %+v", d)
		}
	}
	t.Run("all old segments deleted", func(t *testing.T) {
		dir, _ := build(t)
		wantSnapshot(t, dir)
	})

	// Rebuild the states a crash would leave, by putting old segments back.
	t.Run("no old segments deleted", func(t *testing.T) {
		dir := t.TempDir()
		s := openFile(t, dir)
		save(t, s, Update{HardState: &HardState{CurrentTerm: 2}, Entries: logFrom(1, 2, 2)})
		s.Close()
		s = openFile(t, dir)
		old := s.log
		save(t, s, Update{Entries: logFrom(3, 2, 2)})
		if _, err := old.Rotate(); err != nil {
			t.Fatal(err)
		}
		if err := old.Append(encodeUpdate(Update{HardState: &HardState{CurrentTerm: 2}, Snapshot: &Snapshot{3, 2}, Entries: logFrom(4, 2)})).Wait(); err != nil {
			t.Fatal(err)
		}
		s.Close()
		wantSnapshot(t, dir)
	})
}
