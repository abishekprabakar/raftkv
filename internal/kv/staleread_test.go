package kv

import (
	"errors"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// The five servers of the stale-read script.
const (
	s1 raft.NodeID = iota + 1
	s2
	s3
	s4
	s5
)

// script drives a five-node kv cluster by hand. Only the node a step names is
// ticked, and messages are delivered oldest first, so nothing here is random.
type script struct {
	*cluster
	side map[raft.NodeID]int // a message between two different sides is lost
}

func newScript(t *testing.T) *script {
	t.Helper()
	return &script{cluster: newCluster(t, 5, 0, memOpener()), side: map[raft.NodeID]int{}}
}

// split cuts the listed nodes off from everyone else.
func (sc *script) split(ids ...raft.NodeID) {
	for _, id := range ids {
		sc.side[id] = 1
	}
}

func (sc *script) heal() { clear(sc.side) }

// settle delivers messages oldest first until nothing is left in flight.
func (sc *script) settle() {
	sc.t.Helper()
	for len(sc.queue) > 0 {
		m := sc.queue[0]
		sc.queue = sc.queue[1:]
		if sc.side[m.from] != sc.side[m.to] {
			continue // lost in the partition
		}
		if err := sc.servers[m.to].Receive(m.from, m.msg); err != nil {
			sc.t.Fatal(err)
		}
	}
}

// tick advances one node's clock, and then delivers whatever that caused.
func (sc *script) tick(id raft.NodeID) {
	sc.t.Helper()
	if err := sc.servers[id].Tick(); err != nil {
		sc.t.Fatal(err)
	}
	sc.settle()
}

// elect ticks one node, and only that node, until it stands for election. The
// votes and the new leader's first messages are delivered as part of each tick.
func (sc *script) elect(id raft.NodeID) {
	sc.t.Helper()
	term := sc.servers[id].Status().Term
	for i := 0; i < 100 && sc.servers[id].Status().Term == term; i++ {
		sc.tick(id)
	}
	if st := sc.servers[id].Status(); st.Role != raft.Leader {
		sc.t.Fatalf("S%d did not win an election: %+v", id, st)
	}
}

// spread ticks a leader, and only the leader, until every node it can reach has
// applied everything it has committed. Followers learn the commit index from the
// leader's next message, so this takes at least one heartbeat.
func (sc *script) spread(leader raft.NodeID) {
	sc.t.Helper()
	target := sc.servers[leader].Status().CommitIndex
	for i := 0; i < 100; i++ {
		behind := false
		for _, id := range sc.ids {
			if sc.side[id] == sc.side[leader] && sc.servers[id].Status().LastApplied < target {
				behind = true
			}
		}
		if !behind {
			return
		}
		sc.tick(leader)
	}
	sc.t.Fatalf("S%d's commits did not reach its side of the network", leader)
}

// write puts key=value through a leader, and fails unless the client hears that
// it succeeded.
func (sc *script) write(id raft.NodeID, seq uint64, key, value string) {
	sc.t.Helper()
	heard, result := false, error(nil)
	cmd := Command{ClientID: 1, Seq: seq, Kind: Put, Key: []byte(key), Value: []byte(value)}
	if err := sc.servers[id].Submit(cmd, func(err error) { heard, result = true, err }); err != nil {
		sc.t.Fatal(err)
	}
	sc.settle()
	if !heard || result != nil {
		sc.t.Fatalf("the write of %s=%s through S%d was not acknowledged (heard %v, error %v)", key, value, id, heard, result)
	}
}

type readResult struct {
	done  bool
	value string
	err   error
}

// startRead asks a node for key through ReadIndex, the way a client would, and
// delivers whatever messages that sends.
func (sc *script) startRead(id raft.NodeID, key string) *readResult {
	sc.t.Helper()
	r := &readResult{}
	done := func(v []byte, _ bool, err error) { *r = readResult{true, string(v), err} }
	if err := sc.servers[id].Read([]byte(key), done); err != nil {
		sc.t.Fatal(err)
	}
	sc.settle()
	return r
}

// wantStored checks a node's own copy of k, which is what a plain leader read
// would return.
func (sc *script) wantStored(step string, id raft.NodeID, want string) {
	sc.t.Helper()
	v, _, err := sc.servers[id].Store().Get([]byte("k"))
	if err != nil {
		sc.t.Fatal(err)
	}
	if string(v) != want {
		sc.t.Fatalf("step (%s): S%d stores k = %q, want %q", step, id, v, want)
	}
}

// TestPartitionedLeaderRefusesStaleRead shows why a leader has to confirm it still
// leads before answering a read, in five scripted steps. docs/acceptance/stale-read.md
// lays them out. The M3 simulator hit this case in 16 of 2,000 random schedules;
// here it happens on every run.
func TestPartitionedLeaderRefusesStaleRead(t *testing.T) {
	sc := newScript(t)

	// (a) S1 leads term 1, and k=old is committed and applied on all five.
	sc.elect(s1)
	sc.write(s1, 1, "k", "old")
	sc.spread(s1)
	for _, id := range sc.ids {
		sc.wantStored("a", id, "old")
	}

	// (b) The network splits into {S1, S2} and {S3, S4, S5}. S3 wins term 2 with
	// votes from S4 and S5, commits k=new, and the client hears that it succeeded.
	sc.split(s1, s2)
	sc.elect(s3)
	sc.write(s3, 2, "k", "new")
	sc.spread(s3)
	if st := sc.servers[s3].Status(); st.Term != 2 {
		t.Fatalf("step (b): S3 should lead term 2: %+v", st)
	}
	// The trap is set. Nothing has reached S1, so it still believes it leads term
	// 1, and its own store still says old. A leader that answered from its local
	// state would now return a value older than one already acknowledged.
	if st := sc.servers[s1].Status(); st.Role != raft.Leader || st.Term != 1 {
		t.Fatalf("step (b): S1 should still believe it leads term 1: %+v", st)
	}
	sc.wantStored("b", s1, "old")
	sc.wantStored("b", s3, "new")

	// (c) A client reads k through S1. S1's round of messages reaches only S2, and
	// S2 answers, because it also has heard nothing of term 2. Two of five is not a
	// majority, so S1 has no proof it still leads, and must not answer.
	r := sc.startRead(s1, "k")
	if r.done && r.err == nil {
		t.Fatalf("step (c): S1 served k = %q after k = %q was acknowledged in term 2", r.value, "new")
	}
	if r.done {
		t.Fatalf("step (c): the read ended before S1 could have known anything: %v", r.err)
	}
	if st := sc.servers[s2].Status(); st.Term != 1 || st.Leader != s1 {
		t.Fatalf("step (c): S2 should still follow S1 in term 1: %+v", st)
	}

	// (d) Only S1's clock moves. Its heartbeats reach S2 and nobody else, so the
	// read never gets a majority. It expires and is refused, with no value.
	for i := 0; i < 100 && !r.done; i++ {
		sc.tick(s1)
	}
	switch {
	case !r.done:
		t.Fatal("step (d): the read was never answered or refused")
	case r.err == nil:
		t.Fatalf("step (d): S1 served k = %q after k = %q was acknowledged in term 2", r.value, "new")
	case !errors.Is(r.err, ErrRetry):
		t.Fatalf("step (d): got %v, want ErrRetry", r.err)
	}

	// (e) The network heals. S3's next heartbeat tells S1 about term 2, and S1
	// steps down. The current value comes from the real leader.
	sc.heal()
	sc.spread(s3)
	if st := sc.servers[s1].Status(); st.Role != raft.Follower || st.Term != 2 || st.Leader != s3 {
		t.Fatalf("step (e): S1 should follow S3 in term 2: %+v", st)
	}
	sc.wantStored("e", s1, "new")
	if r := sc.startRead(s3, "k"); !r.done || r.err != nil || r.value != "new" {
		t.Fatalf("step (e): reading through S3 gave %+v, want new", *r)
	}
}
