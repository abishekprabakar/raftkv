package raft

import (
	"errors"
	"fmt"
	"testing"
)

// These tests drive one node by hand, one rule of Figure 2 at a time. Node 1 is
// the node under test unless stated otherwise.

// recorder is the world around a hand-driven node. It records the order of
// saves, sends and applies, and what was durable when each message left.
type recorder struct {
	store   *MemStorage
	events  []string
	sent    []outgoing
	applied []Entry

	durableAtSend []HardState // HardState on disk at each Send
	entriesAtSend []int       // log length on disk at each Send
}

func (r *recorder) Load() (HardState, []Entry, error) { return r.store.Load() }

func (r *recorder) Save(u Update) error {
	r.events = append(r.events, "save")
	return r.store.Save(u)
}

func (r *recorder) Send(to NodeID, msg Message) {
	r.events = append(r.events, "send")
	r.sent = append(r.sent, outgoing{to, msg})
	hard, entries, _ := r.store.Load()
	r.durableAtSend = append(r.durableAtSend, hard)
	r.entriesAtSend = append(r.entriesAtSend, len(entries))
}

func (r *recorder) Apply(e Entry) {
	r.events = append(r.events, "apply")
	r.applied = append(r.applied, e)
}

func (r *recorder) clear() { r.events, r.sent, r.durableAtSend, r.entriesAtSend = nil, nil, nil, nil }

// sentOf returns the messages of type T sent so far.
func sentOf[T Message](r *recorder) []T {
	var out []T
	for _, o := range r.sent {
		if m, ok := o.msg.(T); ok {
			out = append(out, m)
		}
	}
	return out
}

func newNode(t *testing.T, store *MemStorage, peers ...NodeID) (*Node, *recorder) {
	t.Helper()
	r := &recorder{store: store}
	n, err := NewNode(Config{ID: 1, Peers: peers, ElectionTicks: 10, HeartbeatTicks: 2, Seed: 1, Storage: r, Transport: r, StateMachine: r})
	if err != nil {
		t.Fatal(err)
	}
	return n, r
}

// logWithTerms returns entries at indexes 1, 2, ... with the given terms.
func logWithTerms(terms ...uint64) []Entry {
	entries := make([]Entry, len(terms))
	for i, term := range terms {
		entries[i] = Entry{Index: uint64(i + 1), Term: term, Data: []byte(fmt.Sprintf("cmd%d", i+1))}
	}
	return entries
}

// storeWith returns storage that already holds a term, a vote and a log.
func storeWith(t *testing.T, term uint64, votedFor NodeID, entries ...Entry) *MemStorage {
	t.Helper()
	s := NewMemStorage()
	if err := s.Save(Update{HardState: &HardState{CurrentTerm: term, VotedFor: votedFor}, Entries: entries}); err != nil {
		t.Fatal(err)
	}
	return s
}

func receive(t *testing.T, n *Node, from NodeID, msg Message) {
	t.Helper()
	if err := n.Receive(from, msg); err != nil {
		t.Fatal(err)
	}
}

func tickUntilCandidate(t *testing.T, n *Node) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if err := n.Tick(); err != nil {
			t.Fatal(err)
		}
		if n.Status().Role == Candidate {
			return
		}
	}
	t.Fatal("no election after 100 ticks")
}

// makeLeader runs node 1 through an election it wins with node 2's vote.
func makeLeader(t *testing.T, n *Node, r *recorder) {
	t.Helper()
	tickUntilCandidate(t, n)
	receive(t, n, 2, RequestVoteReply{Term: n.Status().Term, VoteGranted: true})
	if n.Status().Role != Leader {
		t.Fatalf("expected leader, got %v", n.Status().Role)
	}
	r.clear()
}

// ---- election ----------------------------------------------------------------

func TestFollowerStartsElectionAfterTimeout(t *testing.T) {
	n, r := newNode(t, storeWith(t, 4, None, logWithTerms(1, 3)...), 1, 2, 3)
	tickUntilCandidate(t, n)
	st := n.Status()
	if st.Term != 5 || st.VotedFor != 1 {
		t.Fatalf("term %d votedFor %d, want term 5 and a vote for itself", st.Term, st.VotedFor)
	}
	reqs := sentOf[RequestVoteArgs](r)
	if len(reqs) != 2 {
		t.Fatalf("sent %d RequestVote messages, want 2", len(reqs))
	}
	want := RequestVoteArgs{Term: 5, CandidateID: 1, LastLogIndex: 2, LastLogTerm: 3}
	if reqs[0] != want {
		t.Fatalf("got %+v want %+v", reqs[0], want)
	}
}

func TestElectionTimeoutsAreRandomized(t *testing.T) {
	seen := map[int]bool{}
	for seed := uint64(1); seed <= 50; seed++ {
		r := &recorder{store: NewMemStorage()}
		n, err := NewNode(Config{ID: 1, Peers: []NodeID{1, 2, 3}, ElectionTicks: 10, Seed: seed, Storage: r, Transport: r, StateMachine: r})
		if err != nil {
			t.Fatal(err)
		}
		ticks := 0
		for n.Status().Role != Candidate {
			ticks++
			n.Tick()
		}
		if ticks < 10 || ticks >= 20 {
			t.Fatalf("seed %d: timed out after %d ticks, want [10, 20)", seed, ticks)
		}
		seen[ticks] = true
	}
	if len(seen) < 5 {
		t.Fatalf("only %d distinct timeouts across 50 seeds: %v", len(seen), seen)
	}
}

func TestCandidateBecomesLeaderOnMajority(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3, 4, 5)
	tickUntilCandidate(t, n)
	term := n.Status().Term
	receive(t, n, 2, RequestVoteReply{Term: term, VoteGranted: true})
	if n.Status().Role != Candidate {
		t.Fatal("two votes of five is not a majority")
	}
	receive(t, n, 3, RequestVoteReply{Term: term, VoteGranted: false})
	if n.Status().Role != Candidate {
		t.Fatal("a refusal is not a vote")
	}
	r.clear()
	receive(t, n, 4, RequestVoteReply{Term: term, VoteGranted: true})
	if n.Status().Role != Leader {
		t.Fatal("three votes of five is a majority")
	}
	if got := len(sentOf[AppendEntriesArgs](r)); got != 4 {
		t.Fatalf("new leader sent %d AppendEntries, want 4", got)
	}
}

func TestDuplicateVotesCountOnce(t *testing.T) {
	n, _ := newNode(t, NewMemStorage(), 1, 2, 3, 4, 5)
	tickUntilCandidate(t, n)
	term := n.Status().Term
	for i := 0; i < 3; i++ {
		receive(t, n, 2, RequestVoteReply{Term: term, VoteGranted: true})
	}
	if n.Status().Role == Leader {
		t.Fatal("one voter repeated three times must not win an election")
	}
}

func TestVoteFromOldTermIsIgnored(t *testing.T) {
	n, _ := newNode(t, NewMemStorage(), 1, 2, 3)
	tickUntilCandidate(t, n)
	receive(t, n, 2, RequestVoteReply{Term: n.Status().Term - 1, VoteGranted: true})
	if n.Status().Role == Leader {
		t.Fatal("a vote from an earlier term must not count")
	}
}

func TestRejectVoteFromLowerTerm(t *testing.T) {
	n, r := newNode(t, storeWith(t, 5, None), 1, 2, 3)
	receive(t, n, 2, RequestVoteArgs{Term: 4, CandidateID: 2})
	if got := sentOf[RequestVoteReply](r); len(got) != 1 || got[0].VoteGranted || got[0].Term != 5 {
		t.Fatalf("got %+v", got)
	}
}

func TestRejectSecondCandidateInSameTerm(t *testing.T) {
	n, r := newNode(t, storeWith(t, 5, 2), 1, 2, 3)
	receive(t, n, 3, RequestVoteArgs{Term: 5, CandidateID: 3})
	if got := sentOf[RequestVoteReply](r); len(got) != 1 || got[0].VoteGranted {
		t.Fatalf("voted twice in one term: %+v", got)
	}
}

func TestSameCandidateCanAskAgain(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	for i := 0; i < 2; i++ {
		receive(t, n, 2, RequestVoteArgs{Term: 5, CandidateID: 2})
	}
	got := sentOf[RequestVoteReply](r)
	if len(got) != 2 || !got[0].VoteGranted || !got[1].VoteGranted {
		t.Fatalf("a lost reply must be repeatable: %+v", got)
	}
}

func TestRejectVoteForStaleLog(t *testing.T) {
	cases := []struct {
		name                string
		lastIndex, lastTerm uint64
		grant               bool
	}{
		{"older last term, longer log", 9, 2, false},
		{"same term, shorter log", 2, 3, false},
		{"same term, same length", 3, 3, true},
		{"newer last term, shorter log", 1, 4, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, r := newNode(t, storeWith(t, 5, None, logWithTerms(1, 3, 3)...), 1, 2, 3)
			receive(t, n, 2, RequestVoteArgs{Term: 6, CandidateID: 2, LastLogIndex: c.lastIndex, LastLogTerm: c.lastTerm})
			if got := sentOf[RequestVoteReply](r); len(got) != 1 || got[0].VoteGranted != c.grant {
				t.Fatalf("got %+v, want granted=%v", got, c.grant)
			}
		})
	}
}

func TestGrantedVoteResetsElectionTimer(t *testing.T) {
	n, _ := newNode(t, NewMemStorage(), 1, 2, 3)
	n.Tick()
	receive(t, n, 2, RequestVoteArgs{Term: 1, CandidateID: 2})
	if n.electionElapsed != 0 {
		t.Fatalf("elapsed %d after granting a vote", n.electionElapsed)
	}
}

// A candidate that can never win must not be able to hold off elections.
func TestRefusedVoteDoesNotResetElectionTimer(t *testing.T) {
	n, _ := newNode(t, storeWith(t, 1, None, logWithTerms(1)...), 1, 2, 3)
	n.Tick()
	n.Tick()
	receive(t, n, 2, RequestVoteArgs{Term: 2, CandidateID: 2})
	if n.electionElapsed != 2 {
		t.Fatalf("elapsed %d, want 2: refusing a vote must leave the timer alone", n.electionElapsed)
	}
}

func TestHigherTermClearsVoteAndStepsDown(t *testing.T) {
	store := NewMemStorage()
	n, r := newNode(t, store, 1, 2, 3)
	makeLeader(t, n, r)
	term := n.Status().Term
	receive(t, n, 3, AppendEntriesReply{Term: term + 1})
	st := n.Status()
	if st.Role != Follower || st.Term != term+1 || st.VotedFor != None || st.Leader != None {
		t.Fatalf("after a higher term: %+v", st)
	}
	if hard, _, _ := store.Load(); hard.CurrentTerm != term+1 || hard.VotedFor != None {
		t.Fatalf("new term not persisted: %+v", hard)
	}
}

func TestCandidateStepsDownForCurrentLeader(t *testing.T) {
	n, _ := newNode(t, NewMemStorage(), 1, 2, 3)
	tickUntilCandidate(t, n)
	term := n.Status().Term
	receive(t, n, 2, AppendEntriesArgs{Term: term, LeaderID: 2})
	st := n.Status()
	if st.Role != Follower || st.Term != term || st.Leader != 2 {
		t.Fatalf("got %+v", st)
	}
}

// ---- leader -------------------------------------------------------------------

func TestLeaderAppendsNoopAndSendsHeartbeats(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	tickUntilCandidate(t, n)
	receive(t, n, 2, RequestVoteReply{Term: n.Status().Term, VoteGranted: true})
	if e := n.log.entry(1); e.Kind != EntryNoop || e.Term != n.Status().Term {
		t.Fatalf("first entry %+v, want a no-op from the new term", e)
	}
	r.clear()
	n.Tick()
	if len(r.sent) != 0 {
		t.Fatal("heartbeat sent early")
	}
	n.Tick()
	if got := len(sentOf[AppendEntriesArgs](r)); got != 2 {
		t.Fatalf("sent %d heartbeats, want 2", got)
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	n, _ := newNode(t, NewMemStorage(), 1, 2, 3)
	if _, _, err := n.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v", err)
	}
}

func TestProposeRejectsOversizeEntry(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	if _, _, err := n.Propose(make([]byte, n.cfg.MaxEntryBytes+1)); !errors.Is(err, ErrEntryTooLarge) {
		t.Fatalf("got %v", err)
	}
	if _, _, err := n.Propose([]byte("still works")); err != nil {
		t.Fatal(err)
	}
}

func TestLeaderCommitsOnMajority(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3, 4, 5)
	tickUntilCandidate(t, n)
	term := n.Status().Term
	receive(t, n, 2, RequestVoteReply{Term: term, VoteGranted: true})
	receive(t, n, 3, RequestVoteReply{Term: term, VoteGranted: true})
	index, _, err := n.Propose([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: index})
	if n.Status().CommitIndex != 0 {
		t.Fatal("two copies of five is not a majority")
	}
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: index})
	if n.Status().CommitIndex != 0 {
		t.Fatal("a repeated reply from one follower must not count twice")
	}
	receive(t, n, 3, AppendEntriesReply{Term: term, Success: true, MatchIndex: index})
	if n.Status().CommitIndex != index {
		t.Fatalf("commit %d, want %d", n.Status().CommitIndex, index)
	}
	if len(r.applied) != int(index) || string(r.applied[index-1].Data) != "x" {
		t.Fatalf("applied %+v", r.applied)
	}
}

// Figure 8: an entry from an earlier term can sit on a majority and still be
// overwritten, so it must never be committed by counting copies.
func TestLeaderDoesNotCountOldTermEntries(t *testing.T) {
	n, r := newNode(t, storeWith(t, 2, None, logWithTerms(1, 2)...), 1, 2, 3)
	makeLeader(t, n, r) // term 3, with a no-op at index 3
	term := n.Status().Term
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 2})
	if c := n.Status().CommitIndex; c != 0 {
		t.Fatalf("committed index %d by counting entries from an earlier term", c)
	}
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 3})
	if c := n.Status().CommitIndex; c != 3 {
		t.Fatalf("commit %d: the current-term entry commits everything before it", c)
	}
}

func TestLeaderBacksUpAndRetries(t *testing.T) {
	store := storeWith(t, 5, None, logWithTerms(1, 1, 4, 4, 4)...)
	cases := []struct {
		name  string
		reply AppendEntriesReply
		next  uint64
	}{
		{"follower log too short", AppendEntriesReply{ConflictIndex: 3}, 3},
		{"leader has the conflicting term", AppendEntriesReply{ConflictTerm: 4, ConflictIndex: 3}, 6},
		{"leader lacks the conflicting term", AppendEntriesReply{ConflictTerm: 3, ConflictIndex: 2}, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n, r := newNode(t, store, 1, 2, 3)
			n.currentTerm = 5
			makeLeader(t, n, r)
			c.reply.Term = n.Status().Term
			n.nextIndex[2] = 7
			receive(t, n, 2, c.reply)
			if n.nextIndex[2] != c.next {
				t.Fatalf("nextIndex %d, want %d", n.nextIndex[2], c.next)
			}
			if got := sentOf[AppendEntriesArgs](r); len(got) != 1 || got[0].PrevLogIndex != c.next-1 {
				t.Fatalf("retry %+v", got)
			}
		})
	}
}

func TestLateRejectionCannotUndoProgress(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	term := n.Status().Term
	index, _, _ := n.Propose([]byte("x"))
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: index})
	receive(t, n, 2, AppendEntriesReply{Term: term, ConflictIndex: 1})
	if n.nextIndex[2] != index+1 || n.matchIndex[2] != index {
		t.Fatalf("next %d match %d after a late rejection", n.nextIndex[2], n.matchIndex[2])
	}
}

func TestReplyFromOldTermIsIgnored(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	term := n.Status().Term
	index, _, _ := n.Propose([]byte("x"))
	receive(t, n, 2, AppendEntriesReply{Term: term - 1, Success: true, MatchIndex: index})
	if n.matchIndex[2] != 0 {
		t.Fatal("a reply from an earlier term must not count")
	}
}

// ---- follower ----------------------------------------------------------------

func TestAppendRejectedFromOldTerm(t *testing.T) {
	n, r := newNode(t, storeWith(t, 5, None), 1, 2, 3)
	n.Tick()
	receive(t, n, 2, AppendEntriesArgs{Term: 4, LeaderID: 2})
	got := sentOf[AppendEntriesReply](r)
	if len(got) != 1 || got[0].Success || got[0].Term != 5 {
		t.Fatalf("got %+v", got)
	}
	if n.Status().Leader != None || n.electionElapsed != 1 {
		t.Fatal("a deposed leader must not reset the timer or be recognized")
	}
}

func TestAppendRejectedWhenLogTooShort(t *testing.T) {
	n, r := newNode(t, storeWith(t, 5, None, logWithTerms(1, 1)...), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 4})
	got := sentOf[AppendEntriesReply](r)
	if len(got) != 1 || got[0].Success || got[0].ConflictTerm != 0 || got[0].ConflictIndex != 3 {
		t.Fatalf("got %+v", got)
	}
}

func TestAppendRejectedOnTermMismatch(t *testing.T) {
	n, r := newNode(t, storeWith(t, 5, None, logWithTerms(1, 2, 2, 2)...), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 4, PrevLogTerm: 3})
	got := sentOf[AppendEntriesReply](r)
	if len(got) != 1 || got[0].Success || got[0].ConflictTerm != 2 || got[0].ConflictIndex != 2 {
		t.Fatalf("got %+v, want the follower's term 2 starting at index 2", got)
	}
	if n.Status().LastLogIndex != 4 {
		t.Fatal("a failed consistency check must not change the log")
	}
}

func TestConflictTruncatesAndReplaces(t *testing.T) {
	store := storeWith(t, 5, None, logWithTerms(1, 2, 2, 2)...)
	n, r := newNode(t, store, 1, 2, 3)
	entries := []Entry{{Index: 2, Term: 3, Data: []byte("new2")}, {Index: 3, Term: 3, Data: []byte("new3")}}
	receive(t, n, 2, AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries})
	got := sentOf[AppendEntriesReply](r)
	if len(got) != 1 || !got[0].Success || got[0].MatchIndex != 3 {
		t.Fatalf("got %+v", got)
	}
	_, durable, _ := store.Load()
	if len(durable) != 3 || string(durable[1].Data) != "new2" || string(durable[2].Data) != "new3" {
		t.Fatalf("durable log %+v", durable)
	}
	if n.Status().LastLogIndex != 3 {
		t.Fatalf("log has %d entries", n.Status().LastLogIndex)
	}
}

func TestTruncatingCommittedEntryPanics(t *testing.T) {
	n, _ := newNode(t, storeWith(t, 5, None, logWithTerms(1, 1, 1)...), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 3})
	defer func() {
		if recover() == nil {
			t.Fatal("overwriting a committed entry must stop the node, not corrupt it")
		}
	}()
	n.Receive(2, AppendEntriesArgs{Term: 5, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []Entry{{Index: 2, Term: 2}}})
}

// A delayed copy of an old message holds fewer entries than the log already
// does. Applying it must not erase what came later.
func TestDuplicateAppendDoesNotTruncate(t *testing.T) {
	n, _ := newNode(t, storeWith(t, 1, None), 1, 2, 3)
	all := logWithTerms(1, 1, 1)
	receive(t, n, 2, AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: all})
	receive(t, n, 2, AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: all[:1]})
	if got := n.Status().LastLogIndex; got != 3 {
		t.Fatalf("log shrank to %d entries", got)
	}
}

// A heartbeat proves the log matches only up to PrevLogIndex. Entries past it
// may be stale, even when the leader's commit index covers those positions.
func TestFollowerCommitStopsAtLastNewEntry(t *testing.T) {
	n, r := newNode(t, storeWith(t, 2, None, logWithTerms(1, 1, 1)...), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 3})
	if c := n.Status().CommitIndex; c != 1 {
		t.Fatalf("commit %d, want 1", c)
	}
	if len(r.applied) != 1 {
		t.Fatalf("applied %d entries", len(r.applied))
	}
}

func TestCommitIndexNeverDecreases(t *testing.T) {
	n, _ := newNode(t, storeWith(t, 2, None, logWithTerms(1, 1, 1)...), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 3})
	// A heartbeat sent later but with a smaller PrevLogIndex, because the leader
	// had backed up for this follower.
	receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 5})
	if c := n.Status().CommitIndex; c != 3 {
		t.Fatalf("commit went back to %d", c)
	}
}

func TestAppliesEachEntryOnceInOrder(t *testing.T) {
	n, r := newNode(t, storeWith(t, 2, None, logWithTerms(1, 1, 1, 1)...), 1, 2, 3)
	for _, commit := range []uint64{2, 2, 4, 3} {
		receive(t, n, 2, AppendEntriesArgs{Term: 2, LeaderID: 2, PrevLogIndex: 4, PrevLogTerm: 1, LeaderCommit: commit})
	}
	if len(r.applied) != 4 {
		t.Fatalf("applied %d entries", len(r.applied))
	}
	for i, e := range r.applied {
		if e.Index != uint64(i+1) {
			t.Fatalf("applied index %d at position %d", e.Index, i)
		}
	}
}

// ---- durability ---------------------------------------------------------------

func TestVoteIsDurableBeforeReply(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	receive(t, n, 2, RequestVoteArgs{Term: 3, CandidateID: 2})
	if fmt.Sprint(r.events) != "[save send]" {
		t.Fatalf("events %v, want one save and then the reply", r.events)
	}
	if got := r.durableAtSend[0]; got != (HardState{CurrentTerm: 3, VotedFor: 2}) {
		t.Fatalf("durable state when the reply left: %+v", got)
	}
}

func TestSelfVoteIsDurableBeforeRequestVote(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	tickUntilCandidate(t, n)
	want := HardState{CurrentTerm: 1, VotedFor: 1}
	for i, got := range r.durableAtSend {
		if got != want {
			t.Fatalf("message %d left with durable state %+v", i, got)
		}
	}
}

func TestAppendIsDurableBeforeReply(t *testing.T) {
	n, r := newNode(t, storeWith(t, 1, None), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: logWithTerms(1, 1)})
	if fmt.Sprint(r.events) != "[save send]" || r.entriesAtSend[0] != 2 {
		t.Fatalf("events %v, %v entries durable at send", r.events, r.entriesAtSend)
	}
}

func TestLeaderEntryIsDurableBeforeReplication(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	index, _, _ := n.Propose([]byte("x"))
	for i, got := range r.entriesAtSend {
		if uint64(got) != index {
			t.Fatalf("message %d left with %d entries durable, want %d", i, got, index)
		}
	}
}

// One message must cost one fsync, however many fields it changes.
func TestOneInputMakesOneSave(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 3, LeaderID: 2, Entries: logWithTerms(1)})
	if fmt.Sprint(r.events) != "[save send]" {
		t.Fatalf("events %v", r.events)
	}
}

func TestSingleNodeAppliesOnlyAfterSave(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1)
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		if err := n.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if n.Status().Role != Leader {
		t.Fatal("a single node is its own majority")
	}
	r.clear()
	if _, _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r.events) != "[save apply]" {
		t.Fatalf("events %v, want the save before the apply", r.events)
	}
}

func TestSaveFailureStopsNode(t *testing.T) {
	store := NewMemStorage()
	n, r := newNode(t, store, 1, 2, 3)
	store.FailNext = errors.New("disk full")
	if err := n.Receive(2, RequestVoteArgs{Term: 3, CandidateID: 2}); err == nil {
		t.Fatal("expected the failure to be reported")
	}
	if len(r.sent) != 0 {
		t.Fatal("a reply left without being durable")
	}
	if err := n.Tick(); err == nil {
		t.Fatal("a failed node must stay stopped")
	}
	if err := n.Receive(2, RequestVoteArgs{Term: 4, CandidateID: 2}); err == nil {
		t.Fatal("a failed node must stay stopped")
	}
	if len(r.sent) != 0 {
		t.Fatal("a failed node sent a message")
	}
}

func TestRestartRestoresState(t *testing.T) {
	store := NewMemStorage()
	n, _ := newNode(t, store, 1, 2, 3)
	receive(t, n, 2, AppendEntriesArgs{Term: 4, LeaderID: 2, Entries: logWithTerms(2, 4), LeaderCommit: 2})
	receive(t, n, 3, RequestVoteArgs{Term: 5, CandidateID: 3, LastLogIndex: 9, LastLogTerm: 9})

	restarted, _ := newNode(t, store, 1, 2, 3)
	st := restarted.Status()
	if st.Term != 5 || st.VotedFor != 3 || st.LastLogIndex != 2 || st.LastLogTerm != 4 {
		t.Fatalf("restored %+v", st)
	}
	if st.CommitIndex != 0 || st.LastApplied != 0 || st.Role != Follower {
		t.Fatalf("volatile state must start empty: %+v", st)
	}
}

// The reason votedFor is durable: without it a restarted node could vote for a
// second candidate in the same term, and two leaders could win that term.
func TestRestartCannotVoteTwice(t *testing.T) {
	store := NewMemStorage()
	n, _ := newNode(t, store, 1, 2, 3)
	receive(t, n, 2, RequestVoteArgs{Term: 5, CandidateID: 2})

	restarted, r := newNode(t, store, 1, 2, 3)
	receive(t, restarted, 3, RequestVoteArgs{Term: 5, CandidateID: 3})
	if got := sentOf[RequestVoteReply](r); len(got) != 1 || got[0].VoteGranted {
		t.Fatalf("voted twice in term 5: %+v", got)
	}
}

// Regression, found by simulation: answering every reply with a send let chains
// of messages pile up behind a slow follower, one more for each heartbeat,
// proposal and duplicate, until the network carried a storm.
func TestRepeatedReplySendsNothing(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	term := n.Status().Term
	n.Propose([]byte("a"))
	n.Propose([]byte("b"))
	r.clear()
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 2})
	if got := len(sentOf[AppendEntriesArgs](r)); got != 1 {
		t.Fatalf("a reply showing progress sent %d messages, want 1", got)
	}
	r.clear()
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 2})
	if len(r.sent) != 0 {
		t.Fatalf("a repeated reply sent %d messages", len(r.sent))
	}
}

func TestRepeatedRejectionSendsNothing(t *testing.T) {
	n, r := newNode(t, storeWith(t, 5, None, logWithTerms(1, 1, 1)...), 1, 2, 3)
	makeLeader(t, n, r)
	term := n.Status().Term
	reject := AppendEntriesReply{Term: term, ConflictIndex: 2}
	receive(t, n, 2, reject)
	if got := len(sentOf[AppendEntriesArgs](r)); got != 1 {
		t.Fatalf("a rejection that moves nextIndex sent %d messages, want 1", got)
	}
	r.clear()
	receive(t, n, 2, reject)
	if len(r.sent) != 0 {
		t.Fatalf("a repeated rejection sent %d messages", len(r.sent))
	}
}

// Stopping a chain must not strand a follower: the heartbeat picks it up.
func TestHeartbeatContinuesAfterLostReply(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	n.Propose([]byte("a"))
	r.clear()
	for i := 0; i < n.cfg.HeartbeatTicks; i++ {
		n.Tick()
	}
	got := sentOf[AppendEntriesArgs](r)
	if len(got) != 2 || len(got[0].Entries) == 0 {
		t.Fatalf("heartbeat did not resend unacknowledged entries: %+v", got)
	}
}
