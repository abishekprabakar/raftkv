package raft

import (
	"errors"
	"fmt"
	"testing"
)

// committedLeader returns node 1 as a leader that has committed its no-op.
func committedLeader(t *testing.T, peers ...NodeID) (*Node, *recorder) {
	t.Helper()
	n, r := newNode(t, NewMemStorage(), peers...)
	tickUntilCandidate(t, n)
	term := n.Status().Term
	voters := peers[1 : len(peers)/2+1]
	for _, v := range voters {
		receive(t, n, v, RequestVoteReply{Term: term, VoteGranted: true})
	}
	for _, v := range voters {
		receive(t, n, v, AppendEntriesReply{Term: term, Success: true, MatchIndex: 1})
	}
	if n.Status().CommitIndex != 1 {
		t.Fatalf("no-op not committed: %+v", n.Status())
	}
	r.clear()
	return n, r
}

func TestFollowerRefusesReads(t *testing.T) {
	n, _ := newNode(t, NewMemStorage(), 1, 2, 3)
	if err := n.ReadIndex(1); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v", err)
	}
}

// A new leader holds every committed entry, but it may not know which ones are
// committed. Until it commits in its own term, its commit index can lag what the
// last leader acknowledged to clients, so a read now could be stale.
func TestReadWaitsForCommitInCurrentTerm(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1, 2, 3)
	makeLeader(t, n, r)
	if err := n.ReadIndex(1); !errors.Is(err, ErrNotReady) {
		t.Fatalf("got %v, want ErrNotReady before the no-op commits", err)
	}
	receive(t, n, 2, AppendEntriesReply{Term: n.Status().Term, Success: true, MatchIndex: 1})
	if err := n.ReadIndex(1); err != nil {
		t.Fatal(err)
	}
}

func TestReadNeedsAMajorityAfterItArrives(t *testing.T) {
	n, r := committedLeader(t, 1, 2, 3, 4, 5)
	term := n.Status().Term
	if err := n.ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	reqs := sentOf[AppendEntriesArgs](r)
	if len(reqs) != 4 || reqs[0].ReadSeq == 0 {
		t.Fatalf("the read must start a round of messages: %+v", reqs)
	}
	seq := reqs[0].ReadSeq

	// An answer to a message sent before the read proves nothing about now.
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 1, ReadSeq: seq - 1})
	receive(t, n, 3, AppendEntriesReply{Term: term, Success: true, MatchIndex: 1, ReadSeq: seq - 1})
	if len(r.readsReady) != 0 {
		t.Fatal("confirmed by replies that predate the read")
	}
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 1, ReadSeq: seq})
	if len(r.readsReady) != 0 {
		t.Fatal("two of five is not a majority")
	}
	receive(t, n, 2, AppendEntriesReply{Term: term, Success: true, MatchIndex: 1, ReadSeq: seq})
	if len(r.readsReady) != 0 {
		t.Fatal("a repeated reply must not count twice")
	}
	receive(t, n, 3, AppendEntriesReply{Term: term, Success: true, MatchIndex: 1, ReadSeq: seq})
	if fmt.Sprint(r.readsReady) != "[7]" {
		t.Fatalf("ready %v", r.readsReady)
	}
}

// A follower that rejects the entries still accepts the leader, and that is what
// a read needs to know.
func TestRejectedAppendStillConfirmsLeadership(t *testing.T) {
	n, r := committedLeader(t, 1, 2, 3)
	n.ReadIndex(7)
	seq := sentOf[AppendEntriesArgs](r)[0].ReadSeq
	receive(t, n, 2, AppendEntriesReply{Term: n.Status().Term, ConflictIndex: 1, ReadSeq: seq})
	if fmt.Sprint(r.readsReady) != "[7]" {
		t.Fatalf("ready %v", r.readsReady)
	}
}

func TestReadIndexIgnoresRepliesFromOldTerms(t *testing.T) {
	n, r := committedLeader(t, 1, 2, 3)
	n.ReadIndex(7)
	seq := sentOf[AppendEntriesArgs](r)[0].ReadSeq
	receive(t, n, 2, AppendEntriesReply{Term: n.Status().Term - 1, Success: true, ReadSeq: seq})
	if len(r.readsReady) != 0 {
		t.Fatal("a reply from an earlier term cannot confirm this leader")
	}
}

func TestSingleNodeServesReadsAtOnce(t *testing.T) {
	n, r := newNode(t, NewMemStorage(), 1)
	for i := 0; i < 100 && n.Status().Role != Leader; i++ {
		n.Tick()
	}
	if err := n.ReadIndex(7); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r.readsReady) != "[7]" {
		t.Fatalf("ready %v", r.readsReady)
	}
}

func TestReadsAreReleasedInOrder(t *testing.T) {
	n, r := committedLeader(t, 1, 2, 3)
	n.ReadIndex(1)
	n.ReadIndex(2)
	seq := n.readSeq
	receive(t, n, 2, AppendEntriesReply{Term: n.Status().Term, Success: true, MatchIndex: 1, ReadSeq: seq})
	if fmt.Sprint(r.readsReady) != "[1 2]" {
		t.Fatalf("ready %v", r.readsReady)
	}
}

func TestSteppingDownFailsPendingReads(t *testing.T) {
	n, r := committedLeader(t, 1, 2, 3)
	n.ReadIndex(1)
	n.ReadIndex(2)
	receive(t, n, 2, AppendEntriesReply{Term: n.Status().Term + 1})
	if fmt.Sprint(r.readsFailed) != "[1 2]" || len(r.readsReady) != 0 {
		t.Fatalf("failed %v ready %v", r.readsFailed, r.readsReady)
	}
}

// A leader cut off from a majority confirms nothing. Its reads must not wait
// forever for a new term it may never hear about.
func TestUnconfirmedReadsExpire(t *testing.T) {
	n, r := committedLeader(t, 1, 2, 3)
	n.ReadIndex(1)
	for i := 0; i < readExpiryElections*n.cfg.ElectionTicks; i++ {
		n.Tick()
	}
	if fmt.Sprint(r.readsFailed) != "[1]" {
		t.Fatalf("failed %v", r.readsFailed)
	}
}
