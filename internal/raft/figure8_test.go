package raft

import (
	"fmt"
	"testing"
)

// The five servers of Figure 8.
const (
	s1 NodeID = iota + 1
	s2
	s3
	s4
	s5
)

// scripted is a cluster driven by hand, one message at a time.
type scripted struct {
	*cluster
}

func newScripted(t *testing.T) *scripted {
	t.Helper()
	// One entry per message, so that an old entry can travel without the new
	// term's no-op behind it. The figure needs exactly that.
	return &scripted{buildCluster(t, 1, 5, 1, func(NodeID) Storage { return NewMemStorage() })}
}

// deliver hands over every queued message from one node to another, oldest
// first, and leaves everything else in flight.
func (s *scripted) deliver(from, to NodeID) {
	s.t.Helper()
	s.deliverOnly(from, to, func(Message) bool { return true })
}

func (s *scripted) deliverOnly(from, to NodeID, wanted func(Message) bool) {
	s.t.Helper()
	var matching, rest []delivery
	for _, d := range s.queue {
		if d.from == from && d.to == to && wanted(d.msg) {
			matching = append(matching, d)
		} else {
			rest = append(rest, d)
		}
	}
	s.queue = rest
	for _, d := range matching {
		if err := s.nodes[to].Receive(from, d.msg); err != nil {
			s.t.Fatal(err)
		}
		s.checkLeaders()
	}
}

// exchange delivers from a to b, then b's answers back to a.
func (s *scripted) exchange(a, b NodeID) {
	s.t.Helper()
	s.deliver(a, b)
	s.deliver(b, a)
}

// lose throws away everything in flight.
func (s *scripted) lose() { s.queue = nil }

// timeout ticks one node, and only that node, until it starts an election.
func (s *scripted) timeout(id NodeID) {
	s.t.Helper()
	term := s.nodes[id].Status().Term
	for i := 0; i < 100 && s.nodes[id].Status().Term == term; i++ {
		if err := s.nodes[id].Tick(); err != nil {
			s.t.Fatal(err)
		}
	}
	if st := s.nodes[id].Status(); st.Term == term || st.Role != Candidate {
		s.t.Fatalf("node %d did not start an election: %+v", id, st)
	}
}

// win runs an election for id with these voters. Only vote traffic moves, so the
// new leader's first AppendEntries stay queued for the script to deal with.
func (s *scripted) win(id NodeID, voters ...NodeID) {
	s.t.Helper()
	s.timeout(id)
	isVote := func(m Message) bool {
		switch m.(type) {
		case RequestVoteArgs, RequestVoteReply:
			return true
		}
		return false
	}
	for _, v := range voters {
		s.deliverOnly(id, v, isVote)
		s.deliverOnly(v, id, isVote)
	}
	if st := s.nodes[id].Status(); st.Role != Leader {
		s.t.Fatalf("node %d should have won with votes from %v: %+v", id, voters, st)
	}
}

func (s *scripted) heartbeat(id NodeID) {
	s.t.Helper()
	for i := 0; i < s.nodes[id].cfg.HeartbeatTicks; i++ {
		if err := s.nodes[id].Tick(); err != nil {
			s.t.Fatal(err)
		}
	}
}

// settle delivers every message among live nodes, in order, until none are left.
func (s *scripted) settle() {
	s.t.Helper()
	for len(s.queue) > 0 {
		d := s.queue[0]
		s.queue = s.queue[1:]
		if s.isolated[d.to] {
			continue
		}
		if err := s.nodes[d.to].Receive(d.from, d.msg); err != nil {
			s.t.Fatal(err)
		}
		s.checkLeaders()
	}
}

// crash abandons a node. Its disk stays, and nothing reaches it until restart.
func (s *scripted) crash(id NodeID) {
	s.isolated[id] = true
	s.lose()
}

func (s *scripted) restart(id NodeID) {
	s.t.Helper()
	s.start(id)
	s.isolated[id] = false
}

// wantDisk checks the terms in a server's durable log.
func (s *scripted) wantDisk(step string, id NodeID, terms ...uint64) {
	s.t.Helper()
	d, err := s.stores[id].Load()
	if err != nil {
		s.t.Fatal(err)
	}
	got := make([]uint64, len(d.Entries))
	for i, e := range d.Entries {
		got[i] = e.Term
	}
	if fmt.Sprint(got) != fmt.Sprint(terms) {
		s.t.Fatalf("step (%s): S%d holds terms %v, want %v", step, id, got, terms)
	}
}

// upToC plays the steps both endings share. It ends at (c), with the term 2 entry
// on a majority and the term 4 entry on S1 alone.
func (s *scripted) upToC() {
	s.t.Helper()
	// The starting point of the figure: every server holds index 1 from term 1.
	s.win(s4, s1, s2, s3, s5)
	for _, p := range []NodeID{s1, s2, s3, s5} {
		s.exchange(s4, p)
	}
	s.lose()
	for _, id := range s.ids {
		s.wantDisk("start", id, 1)
	}

	// (a) S1 leads term 2 and partially replicates index 2, to S2 only. Every server
	// hears of term 2, so S5's next election is term 3, as in the figure.
	s.win(s1, s2, s3, s4, s5)
	s.deliver(s1, s2)
	s.lose()
	s.wantDisk("a", s1, 1, 2)
	s.wantDisk("a", s2, 1, 2)
	s.wantDisk("a", s3, 1)
	s.wantDisk("a", s4, 1)
	s.wantDisk("a", s5, 1)

	// (b) S1 crashes. S5 wins term 3 with votes from S3, S4 and itself, and takes
	// a different entry at index 2.
	s.crash(s1)
	s.win(s5, s3, s4)
	s.lose()
	s.wantDisk("b", s2, 1, 2)
	s.wantDisk("b", s3, 1)
	s.wantDisk("b", s4, 1)
	s.wantDisk("b", s5, 1, 3)

	// (c) S5 crashes and S1 restarts. S1 restarted in term 2, so its first try is
	// term 3, which it can't win because S3 and S4 already voted for S5. Term 4
	// works, and S1 puts its no-op at index 3.
	s.crash(s5)
	s.restart(s1)
	s.timeout(s1)
	s.lose()
	s.win(s1, s2, s3, s4)

	// S3 lacks index 2, so it rejects S1's first message, and S1 backs up and
	// sends it the term 2 entry alone.
	s.exchange(s1, s3)
	s.exchange(s1, s3)
	s.lose()

	// S2 already holds index 2, so S1's first message to it would carry the term 4
	// entry, which the figure keeps on S1 alone. Starting S2 one entry lower gets
	// S2's acknowledgment of index 2 without moving that entry. A leader may set
	// nextIndex lower at any time, because it is only a guess.
	s.nodes[s1].nextIndex[s2] = 2
	s.heartbeat(s1)
	s.exchange(s1, s2)
	s.lose()

	s.wantDisk("c", s1, 1, 2, 4)
	s.wantDisk("c", s2, 1, 2)
	s.wantDisk("c", s3, 1, 2)
	s.wantDisk("c", s4, 1)
	s.wantDisk("c", s5, 1, 3)

	// This is the rule the figure teaches. The term 2 entry is on three of five
	// servers, and it still must not count as committed. The script carries on
	// after a failure here, so that (d) can show the damage it does.
	if c := s.nodes[s1].Status().CommitIndex; c >= 2 {
		s.t.Errorf("step (c): S1 committed through index %d by counting an entry from term 2", c)
	}
}

// TestFigure8 replays Figure 8 of the Raft paper, one message at a time, on five
// real nodes. It shows why a leader may not commit an entry from an earlier term
// by counting copies, even when that entry is on a majority.
//
// Nothing here is random: only the node named in a step is ticked, and every
// message is delivered by hand or thrown away. docs/acceptance/figure8.md draws
// the logs after each step. Each leader's no-op stands in for the figure's entry.
func TestFigure8(t *testing.T) {
	// (d) S1 crashes before its term 4 entry spreads. S5's last entry is from term
	// 3, newer than anything on S2, S3 or S4, so all three vote for it. S5 then
	// overwrites index 2 on all of them. That is only safe if nobody committed it.
	t.Run("d overwrites the uncommitted entry", func(t *testing.T) {
		s := newScripted(t)
		s.upToC()
		s.crash(s1)
		s.restart(s5)
		s.timeout(s5) // term 4: everyone already voted for S1
		s.lose()
		s.win(s5, s2, s3, s4) // term 5
		s.settle()
		s.heartbeat(s5)
		s.settle()
		for _, id := range []NodeID{s2, s3, s4, s5} {
			s.wantDisk("d", id, 1, 3, 5)
		}
		if c := s.nodes[s5].Status().CommitIndex; c != 3 {
			t.Fatalf("S5 committed through %d, want 3", c)
		}
	})

	// (e) S1 gets its term 4 entry onto a majority before crashing. That commits
	// everything through index 3, and S5 can no longer win.
	t.Run("e commits through the current term", func(t *testing.T) {
		s := newScripted(t)
		s.upToC()
		s.heartbeat(s1)
		s.exchange(s1, s2)
		s.exchange(s1, s3)
		s.lose()
		s.wantDisk("e", s1, 1, 2, 4)
		s.wantDisk("e", s2, 1, 2, 4)
		s.wantDisk("e", s3, 1, 2, 4)
		s.wantDisk("e", s4, 1)
		s.wantDisk("e", s5, 1, 3)
		if c := s.nodes[s1].Status().CommitIndex; c != 3 {
			t.Fatalf("S1 committed through %d, want 3", c)
		}

		s.crash(s1)
		s.restart(s5)
		s.timeout(s5) // term 4
		s.lose()
		s.timeout(s5) // term 5
		for _, v := range []NodeID{s2, s3, s4} {
			s.exchange(s5, v)
		}
		if st := s.nodes[s5].Status(); st.Role == Leader {
			t.Fatalf("S5 won with an out-of-date log: %+v", st)
		}
		s.wantDisk("e", s2, 1, 2, 4)
		s.wantDisk("e", s3, 1, 2, 4)
	})
}
