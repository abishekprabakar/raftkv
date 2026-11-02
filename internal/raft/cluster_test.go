package raft

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"testing"
)

// cluster runs real Nodes against each other on a small deterministic network.
// There are no goroutines and no real time: the seed fixes the order of every
// tick and delivery, so a failure names a seed that replays it exactly. The
// fault injection in M2 builds on the same idea. Here the only faults are
// isolating a node and restarting one.
type cluster struct {
	t        *testing.T
	seed     uint64
	rnd      *rand.Rand
	ids      []NodeID
	nodes    map[NodeID]*Node
	stores   map[NodeID]Storage
	machines map[NodeID]*checkedMachine
	queue    []delivery
	isolated map[NodeID]bool
	restarts uint64

	maxEntries int // MaxEntriesPerMessage for every node; 0 means the default

	// Facts every node must agree on, recorded as they are first observed.
	leaderOfTerm map[uint64]NodeID
	entryAt      map[uint64]Entry
}

type delivery struct {
	from, to NodeID
	msg      Message
}

type clusterTransport struct {
	c    *cluster
	from NodeID
}

func (t clusterTransport) Send(to NodeID, msg Message) {
	t.c.queue = append(t.c.queue, delivery{t.from, to, msg})
}

// checkedMachine records what one node applied, and fails the test the moment
// two nodes apply different entries at an index (State Machine Safety).
type checkedMachine struct {
	c       *cluster
	id      NodeID
	applied []Entry
}

func (m *checkedMachine) Apply(e Entry) {
	if e.Index != uint64(len(m.applied))+1 {
		m.c.t.Fatalf("seed %d: node %d applied index %d after %d", m.c.seed, m.id, e.Index, len(m.applied))
	}
	if prev, ok := m.c.entryAt[e.Index]; ok && (prev.Term != e.Term || !bytes.Equal(prev.Data, e.Data)) {
		m.c.t.Fatalf("seed %d: node %d applied %+v at index %d, another node applied %+v", m.c.seed, m.id, e, e.Index, prev)
	}
	m.c.entryAt[e.Index] = e
	m.applied = append(m.applied, e)
}

// Position starts at zero: a test machine is rebuilt from the log after a restart.
func (m *checkedMachine) Position() (uint64, uint64) {
	if len(m.applied) == 0 {
		return 0, 0
	}
	e := m.applied[len(m.applied)-1]
	return e.Index, e.Term
}

func (m *checkedMachine) Snapshot() []byte { panic("cluster tests do not compact") }

func (m *checkedMachine) Restore(uint64, uint64, []byte) { panic("cluster tests do not compact") }

func (m *checkedMachine) commands() []string {
	var out []string
	for _, e := range m.applied {
		if e.Kind == EntryCommand {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func newCluster(t *testing.T, seed uint64, size int, open func(NodeID) Storage) *cluster {
	t.Helper()
	return buildCluster(t, seed, size, 0, open)
}

func buildCluster(t *testing.T, seed uint64, size, maxEntries int, open func(NodeID) Storage) *cluster {
	t.Helper()
	c := &cluster{
		t: t, seed: seed, rnd: rand.New(rand.NewPCG(seed, 0xC1)), maxEntries: maxEntries,
		nodes: map[NodeID]*Node{}, stores: map[NodeID]Storage{}, machines: map[NodeID]*checkedMachine{},
		isolated: map[NodeID]bool{}, leaderOfTerm: map[uint64]NodeID{}, entryAt: map[uint64]Entry{},
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, NodeID(i))
	}
	for _, id := range c.ids {
		c.stores[id] = open(id)
		c.start(id)
	}
	return c
}

func memCluster(t *testing.T, seed uint64, size int) *cluster {
	t.Helper()
	return newCluster(t, seed, size, func(NodeID) Storage { return NewMemStorage() })
}

// start builds a node from its storage, with an empty state machine. Each start
// gets a new seed, so a restarted node does not repeat its old timeouts.
func (c *cluster) start(id NodeID) {
	c.t.Helper()
	c.restarts++
	c.machines[id] = &checkedMachine{c: c, id: id}
	n, err := NewNode(Config{
		ID: id, Peers: c.ids, Seed: c.seed*1000 + c.restarts, MaxEntriesPerMessage: c.maxEntries,
		Storage: c.stores[id], Transport: clusterTransport{c, id}, StateMachine: c.machines[id],
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = n
}

// checkLeaders enforces Election Safety: at most one leader per term.
func (c *cluster) checkLeaders() {
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		if st.Role != Leader {
			continue
		}
		if other, ok := c.leaderOfTerm[st.Term]; ok && other != id {
			c.t.Fatalf("seed %d: nodes %d and %d both led term %d", c.seed, other, id, st.Term)
		}
		c.leaderOfTerm[st.Term] = id
	}
}

// tick advances every node's clock in a random order, then delivers everything
// in flight in a random order.
func (c *cluster) tick() {
	c.t.Helper()
	for _, i := range c.rnd.Perm(len(c.ids)) {
		if err := c.nodes[c.ids[i]].Tick(); err != nil {
			c.t.Fatal(err)
		}
		c.checkLeaders()
	}
	c.drain()
}

func (c *cluster) drain() {
	c.t.Helper()
	for len(c.queue) > 0 {
		i := c.rnd.IntN(len(c.queue))
		d := c.queue[i]
		c.queue[i] = c.queue[len(c.queue)-1]
		c.queue = c.queue[:len(c.queue)-1]
		if c.isolated[d.from] || c.isolated[d.to] {
			continue
		}
		if err := c.nodes[d.to].Receive(d.from, d.msg); err != nil {
			c.t.Fatal(err)
		}
		c.checkLeaders()
	}
}

// leader returns the reachable node leading the newest term, or None.
func (c *cluster) leader() NodeID {
	best, bestTerm := None, uint64(0)
	for _, id := range c.ids {
		st := c.nodes[id].Status()
		if st.Role == Leader && !c.isolated[id] && st.Term > bestTerm {
			best, bestTerm = id, st.Term
		}
	}
	return best
}

const maxTicks = 1000

func (c *cluster) waitLeader() NodeID {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		if l := c.leader(); l != None {
			return l
		}
		c.tick()
	}
	c.t.Fatalf("seed %d: no leader after %d ticks", c.seed, maxTicks)
	return None
}

// waitApplied runs until every reachable node has applied through index.
func (c *cluster) waitApplied(index uint64) {
	c.t.Helper()
	for i := 0; i < maxTicks; i++ {
		done := true
		for _, id := range c.ids {
			if !c.isolated[id] && c.nodes[id].Status().LastApplied < index {
				done = false
			}
		}
		if done {
			return
		}
		c.tick()
	}
	c.t.Fatalf("seed %d: entry %d not applied everywhere after %d ticks", c.seed, index, maxTicks)
}

// commit proposes a command through the leader and waits until all reachable
// nodes have applied it.
func (c *cluster) commit(cmd string) uint64 {
	c.t.Helper()
	index, _, err := c.nodes[c.waitLeader()].Propose([]byte(cmd))
	if err != nil {
		c.t.Fatalf("seed %d: propose %q: %v", c.seed, cmd, err)
	}
	c.waitApplied(index)
	return index
}

func (c *cluster) checkCommands(want ...string) {
	c.t.Helper()
	for _, id := range c.ids {
		if got := c.machines[id].commands(); fmt.Sprint(got) != fmt.Sprint(want) {
			c.t.Fatalf("seed %d: node %d applied %v, want %v", c.seed, id, got, want)
		}
	}
}

func TestElectsOneLeader(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			for seed := uint64(1); seed <= 200; seed++ {
				c := memCluster(t, seed, size)
				leader := c.waitLeader()
				// Stay stable: with no faults the leader must not be challenged.
				term := c.nodes[leader].Status().Term
				for i := 0; i < 50; i++ {
					c.tick()
				}
				if st := c.nodes[leader].Status(); st.Role != Leader || st.Term != term {
					t.Fatalf("seed %d: leader %d lost its place with no faults: %+v", seed, leader, st)
				}
			}
		})
	}
}

func TestReplicatesToAllNodes(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		c := memCluster(t, seed, 3)
		leader := c.nodes[c.waitLeader()]
		var want []string
		var last uint64
		for i := 0; i < 100; i++ {
			cmd := fmt.Sprintf("cmd-%d", i)
			want = append(want, cmd)
			index, _, err := leader.Propose([]byte(cmd))
			if err != nil {
				t.Fatal(err)
			}
			last = index
			if i%7 == 0 { // let several proposals pile up between deliveries
				c.tick()
			}
		}
		c.waitApplied(last)
		c.checkCommands(want...)
	}
}

// The old leader is cut off holding an entry it can never commit. The rest
// elect a new leader and move on, and when the old leader returns, its entry is
// discarded while everything committed survives.
func TestLeaderFailover(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		c := memCluster(t, seed, 3)
		c.commit("before")

		old := c.leader()
		c.isolated[old] = true
		if _, _, err := c.nodes[old].Propose([]byte("orphan")); err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		last := c.commit("after")
		if c.leader() == old {
			t.Fatalf("seed %d: isolated node still reported as leader", seed)
		}

		c.isolated[old] = false
		c.waitApplied(last)
		c.checkCommands("before", "after")
		for _, e := range c.nodes[old].log.entries {
			if string(e.Data) == "orphan" {
				t.Fatalf("seed %d: orphan entry survived at index %d", seed, e.Index)
			}
		}
		if st := c.nodes[old].Status(); st.Role == Leader {
			t.Fatalf("seed %d: old leader did not step down: %+v", seed, st)
		}
	}
}

func TestRestartKeepsCommittedEntries(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		c := memCluster(t, seed, 3)
		var want []string
		var last uint64
		add := func() {
			t.Helper()
			cmd := fmt.Sprintf("cmd-%d", len(want))
			want = append(want, cmd)
			last = c.commit(cmd)
		}
		for i := 0; i < 10; i++ {
			add()
		}
		for _, id := range c.ids {
			c.start(id) // the old Node is simply abandoned, as a crash would
			c.waitApplied(last)
			add()
		}
		c.checkCommands(want...)
	}
}

func TestClusterOnFileStorage(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		dirs := map[NodeID]string{}
		open := func(id NodeID) Storage {
			t.Helper()
			if dirs[id] == "" {
				dirs[id] = t.TempDir()
			}
			s, err := OpenFileStorage(dirs[id])
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		c := newCluster(t, seed, 3, open)
		var want []string
		var last uint64
		for i := 0; i < 20; i++ {
			want = append(want, fmt.Sprintf("cmd-%d", i))
			last = c.commit(want[i])
		}
		c.queue = nil // everything in flight dies with the processes
		for _, id := range c.ids {
			if err := c.stores[id].(*FileStorage).Close(); err != nil {
				t.Fatal(err)
			}
			c.stores[id] = open(id)
			c.start(id)
		}
		c.waitApplied(last)
		want = append(want, "after-restart")
		c.commit("after-restart")
		c.checkCommands(want...)
		for _, id := range c.ids {
			if err := c.stores[id].(*FileStorage).Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
