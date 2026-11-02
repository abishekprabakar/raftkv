package kv

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// cluster runs real Servers on a small deterministic network, for tests that need
// an exact story rather than random faults. Random faults live in internal/raft/sim.
type cluster struct {
	t        *testing.T
	rnd      *rand.Rand
	ids      []raft.NodeID
	servers  map[raft.NodeID]*Server
	storage  map[raft.NodeID]raft.Storage
	backends map[raft.NodeID]Backend
	queue    []message
	cut      map[raft.NodeID]bool
	every    uint64
	restarts uint64
	onDisk   func(raft.NodeID) (raft.Storage, Backend)
}

type message struct {
	from, to raft.NodeID
	msg      raft.Message
}

type clusterLink struct {
	c    *cluster
	from raft.NodeID
}

func (l clusterLink) Send(to raft.NodeID, msg raft.Message) {
	l.c.queue = append(l.c.queue, message{l.from, to, msg})
}

func newCluster(t *testing.T, size int, every uint64, open func(raft.NodeID) (raft.Storage, Backend)) *cluster {
	t.Helper()
	c := &cluster{
		t: t, rnd: rand.New(rand.NewPCG(1, 2)), every: every, onDisk: open,
		servers: map[raft.NodeID]*Server{}, storage: map[raft.NodeID]raft.Storage{}, backends: map[raft.NodeID]Backend{},
		cut: map[raft.NodeID]bool{},
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, raft.NodeID(i))
	}
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

func memOpener() func(raft.NodeID) (raft.Storage, Backend) {
	return func(raft.NodeID) (raft.Storage, Backend) { return raft.NewMemStorage(), NewMemBackend() }
}

func (c *cluster) start(id raft.NodeID) {
	c.t.Helper()
	c.storage[id], c.backends[id] = c.onDisk(id)
	c.restarts++
	s, err := NewServer(Config{
		Raft: raft.Config{
			ID: id, Peers: c.ids, Seed: c.restarts,
			Storage: c.storage[id], Transport: clusterLink{c, id},
		},
		Backend: c.backends[id], SnapshotEvery: c.every,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.servers[id] = s
}

// step advances every node's clock, then delivers everything in flight.
func (c *cluster) step() {
	c.t.Helper()
	for _, id := range c.ids {
		if err := c.servers[id].Tick(); err != nil {
			c.t.Fatal(err)
		}
	}
	c.deliver()
}

func (c *cluster) deliver() {
	c.t.Helper()
	for len(c.queue) > 0 {
		i := c.rnd.IntN(len(c.queue))
		m := c.queue[i]
		c.queue[i] = c.queue[len(c.queue)-1]
		c.queue = c.queue[:len(c.queue)-1]
		if c.cut[m.from] || c.cut[m.to] {
			continue
		}
		if err := c.servers[m.to].Receive(m.from, m.msg); err != nil {
			c.t.Fatal(err)
		}
	}
}

const maxSteps = 1000

func (c *cluster) leader() raft.NodeID {
	c.t.Helper()
	for i := 0; i < maxSteps; i++ {
		var best raft.NodeID
		var term uint64
		for _, id := range c.ids {
			if st := c.servers[id].Status(); !c.cut[id] && st.Role == raft.Leader && st.Term > term {
				best, term = id, st.Term
			}
		}
		if best != raft.None {
			return best
		}
		c.step()
	}
	c.t.Fatal("no leader")
	return raft.None
}

// put writes through the leader and returns once it is applied there.
func (c *cluster) put(key, value string, seq uint64) {
	c.t.Helper()
	for i := 0; i < maxSteps; i++ {
		result := make(chan error, 1)
		cmd := Command{ClientID: 1, Seq: seq, Kind: Put, Key: []byte(key), Value: []byte(value)}
		if err := c.servers[c.leader()].Submit(cmd, func(err error) { result <- err }); err != nil {
			c.t.Fatal(err)
		}
		for j := 0; j < 50 && len(result) == 0; j++ {
			c.step()
		}
		if len(result) > 0 && <-result == nil {
			return
		}
	}
	c.t.Fatalf("write of %s never succeeded", key)
}

// read does a ReadIndex read through one node, and says whether it finished.
func (c *cluster) read(id raft.NodeID, key string) (value string, err error, finished bool) {
	c.t.Helper()
	done := func(v []byte, _ bool, e error) { value, err, finished = string(v), e, true }
	if e := c.servers[id].Read([]byte(key), done); e != nil {
		c.t.Fatal(e)
	}
	for i := 0; i < 50 && !finished; i++ {
		c.step()
	}
	return value, err, finished
}

// settle runs until every reachable node has applied everything.
func (c *cluster) settle() {
	c.t.Helper()
	for i := 0; i < maxSteps; i++ {
		target := c.servers[c.leader()].Status().CommitIndex
		done := true
		for _, id := range c.ids {
			if !c.cut[id] && c.servers[id].Status().LastApplied < target {
				done = false
			}
		}
		if done {
			return
		}
		c.step()
	}
	c.t.Fatal("cluster did not settle")
}

// partitioned returns a settled 3-node cluster whose leader holds k=old and is cut
// off from the other two, which then elect a new leader and commit k=new.
func partitioned(t *testing.T) (*cluster, raft.NodeID, raft.NodeID) {
	t.Helper()
	c := newCluster(t, 3, 0, memOpener())
	c.put("k", "old", 1)
	c.settle()
	stale := c.leader()
	c.cut[stale] = true
	c.queue = nil
	fresh := c.leader()
	c.put("k", "new", 2)
	if fresh == stale {
		t.Fatal("the partition should have forced a new leader")
	}
	return c, stale, fresh
}

// The reason a plain leader read is unsafe. The old leader was never told it lost
// its position, so it still calls itself leader and still holds the old value. The
// write of k=new has already been acknowledged, so a client that saw that write
// finish, and then reads from here, gets time travel.
func TestPlainLeaderReadIsStale(t *testing.T) {
	c, stale, fresh := partitioned(t)
	st := c.servers[stale].Status()
	if st.Role != raft.Leader {
		t.Fatalf("the cut-off node should still believe it leads: %+v", st)
	}
	if c.servers[fresh].Status().Term <= st.Term {
		t.Fatal("a newer term should exist")
	}
	// A plain leader read: the node believes it leads, so it answers from local state.
	v, _, err := c.servers[stale].Store().Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "old" {
		t.Fatalf("got %q: this test exists to show the stale read", v)
	}
}

// The same setup with ReadIndex: the stale leader can't reach a majority, so it
// never serves the read, and the read is refused when it expires.
func TestReadIndexRefusesDeposedLeader(t *testing.T) {
	c, stale, fresh := partitioned(t)
	value, err, finished := c.read(stale, "k")
	if finished && err == nil {
		t.Fatalf("a deposed leader served a read: %q", value)
	}
	if !finished {
		t.Fatal("the read should have been refused by now, not left waiting")
	}
	if !errors.Is(err, ErrRetry) {
		t.Fatalf("got %v", err)
	}
	if v, err, ok := c.read(fresh, "k"); !ok || err != nil || v != "new" {
		t.Fatalf("the real leader read %q %v %v", v, err, ok)
	}
}

func TestReadSeesAcknowledgedWrite(t *testing.T) {
	c := newCluster(t, 3, 0, memOpener())
	c.put("k", "v", 1)
	if v, err, ok := c.read(c.leader(), "k"); !ok || err != nil || v != "v" {
		t.Fatalf("read %q %v %v", v, err, ok)
	}
}

func TestReadFromFollowerNamesTheLeader(t *testing.T) {
	c := newCluster(t, 3, 0, memOpener())
	c.put("k", "v", 1)
	c.settle()
	leader := c.leader()
	var follower raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			follower = id
		}
	}
	c.step() // the commit index reaches followers on the next message
	if _, err, ok := c.read(follower, "k"); !ok || !errors.Is(err, ErrNotLeader) {
		t.Fatalf("got %v %v", err, ok)
	}
	if got := c.servers[follower].Leader(); got != leader {
		t.Fatalf("follower points at %d, leader is %d", got, leader)
	}
}

// A retried write must take effect once, even if the first try committed and the
// client never heard. Here the retry goes to a different leader.
func TestRetryAcrossLeaderChangeAppliesOnce(t *testing.T) {
	c := newCluster(t, 3, 0, memOpener())
	first := c.leader()
	c.step()
	cmd := Command{ClientID: 1, Seq: 1, Kind: Append, Key: []byte("k"), Value: []byte("x")}
	if err := c.servers[first].Submit(cmd, func(error) {}); err != nil {
		t.Fatal(err)
	}
	c.settle() // it commits everywhere, then the leader dies before the client hears
	c.cut[first] = true
	c.queue = nil

	second := c.leader()
	done := false
	if err := c.servers[second].Submit(cmd, func(err error) { done = err == nil }); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50 && !done; i++ {
		c.step()
	}
	if !done {
		t.Fatal("the retry was not acknowledged")
	}
	if v, err, ok := c.read(second, "k"); !ok || err != nil || v != "x" {
		t.Fatalf("read %q %v %v: a retried append must apply once", v, err, ok)
	}
}

// R3: real files, the real engine, and a follower behind the leader's log.
func TestLaggingFollowerInstallsSnapshot(t *testing.T) {
	root := t.TempDir()
	open := func(id raft.NodeID) (raft.Storage, Backend) {
		dir := filepath.Join(root, fmt.Sprintf("node-%d", id))
		s, err := raft.OpenFileStorage(filepath.Join(dir, "raft"))
		if err != nil {
			t.Fatal(err)
		}
		b, err := OpenLSM(filepath.Join(dir, "kv"))
		if err != nil {
			t.Fatal(err)
		}
		return s, b
	}
	c := newCluster(t, 3, 10, open)
	defer func() {
		for _, id := range c.ids {
			c.storage[id].(*raft.FileStorage).Close()
			c.backends[id].Close()
		}
	}()
	c.put("before", "x", 1)
	c.settle()
	leader := c.leader()
	var lagging raft.NodeID
	for _, id := range c.ids {
		if id != leader {
			lagging = id
		}
	}
	c.cut[lagging] = true
	const writes = 40
	for i := 0; i < writes; i++ {
		c.put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), uint64(i+2))
	}
	c.settle()
	st := c.servers[leader].Status()
	if st.SnapshotIndex <= c.servers[lagging].Status().LastLogIndex {
		t.Fatalf("the leader kept the entries the follower needs: %+v", st)
	}

	c.cut[lagging] = false
	c.settle()
	got := c.servers[lagging].Status()
	if got.SnapshotIndex == 0 || got.LastApplied < st.SnapshotIndex {
		t.Fatalf("follower did not install a snapshot: %+v", got)
	}
	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("k%d", i)
		if v, _, _ := c.servers[lagging].Store().Get([]byte(key)); string(v) != fmt.Sprintf("v%d", i) {
			t.Fatalf("follower has %s = %q", key, v)
		}
	}

	// It survives a restart, from real files.
	c.storage[lagging].(*raft.FileStorage).Close()
	c.backends[lagging].Close()
	c.start(lagging)
	if v, _, _ := c.servers[lagging].Store().Get([]byte("k0")); string(v) != "v0" {
		t.Fatalf("k0 after restart = %q", v)
	}
	if st := c.servers[lagging].Status(); st.LastApplied < got.SnapshotIndex {
		t.Fatalf("restart went back before the snapshot: %+v", st)
	}
	c.put("after", "y", writes+2)
	c.settle()
	if v, _, _ := c.servers[lagging].Store().Get([]byte("after")); string(v) != "y" {
		t.Fatalf("after = %q", v)
	}
}

func TestCompactionKeepsRecentEntries(t *testing.T) {
	c := newCluster(t, 1, 0, memOpener())
	s := c.servers[1]
	s.cfg.SnapshotEvery = 5
	s.cfg.TrailingEntries = 3
	c.leader()
	for i := 0; i < 20; i++ {
		c.put("k", "v", uint64(i+1))
		c.step()
	}
	st := s.Status()
	if st.SnapshotIndex == 0 {
		t.Fatalf("no compaction: %+v", st)
	}
	if st.LastApplied-st.SnapshotIndex < 3 {
		t.Fatalf("trailing entries were not kept: %+v", st)
	}
}
