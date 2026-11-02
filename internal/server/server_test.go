package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/abishekprabakar/raftkv/internal/client"
	"github.com/abishekprabakar/raftkv/internal/pb"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// requireEveryFieldSet fails if any field of v, or of the first element of any
// slice of structs in v, is its zero value. A field added to a Raft message later
// then breaks the fixture below until it is set, and once it is set, the round
// trip fails until the encoding carries it too.
func requireEveryFieldSet(t *testing.T, v reflect.Value) {
	t.Helper()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		name := v.Type().Name() + "." + v.Type().Field(i).Name
		if f.IsZero() || (f.Kind() == reflect.Slice && f.Len() == 0) {
			t.Fatalf("%s is not set in the fixture", name)
		}
		if f.Kind() == reflect.Slice && f.Type().Elem().Kind() == reflect.Struct {
			requireEveryFieldSet(t, f.Index(0))
		}
	}
}

// G1. Every message, with every field set, survives protobuf encoding unchanged.
func TestMessagesRoundTrip(t *testing.T) {
	entries := []raft.Entry{
		{Index: 7, Term: 3, Kind: raft.EntryNoop, Data: []byte("first")}, // nonzero Kind, for the field check
		{Index: 8, Term: 3, Kind: raft.EntryCommand, Data: []byte("second")},
	}
	msgs := []raft.Message{
		raft.RequestVoteArgs{Term: 1, CandidateID: 2, LastLogIndex: 3, LastLogTerm: 4},
		raft.RequestVoteReply{Term: 5, VoteGranted: true},
		raft.AppendEntriesArgs{Term: 6, LeaderID: 7, PrevLogIndex: 8, PrevLogTerm: 9, Entries: entries, LeaderCommit: 10, ReadSeq: 11},
		raft.AppendEntriesReply{Term: 12, Success: true, ReadSeq: 13, MatchIndex: 14, ConflictTerm: 15, ConflictIndex: 16},
		raft.InstallSnapshotArgs{Term: 17, LeaderID: 18, LastIncludedIndex: 19, LastIncludedTerm: 20, Data: []byte("snapshot"), ReadSeq: 21},
		raft.InstallSnapshotReply{Term: 22, MatchIndex: 23, ReadSeq: 24},
	}
	for _, msg := range msgs {
		requireEveryFieldSet(t, reflect.ValueOf(msg))
		wire, err := proto.Marshal(toEnvelope(4, 5, msg))
		if err != nil {
			t.Fatal(err)
		}
		var env pb.Envelope
		if err := proto.Unmarshal(wire, &env); err != nil {
			t.Fatal(err)
		}
		got, err := fromEnvelope(&env)
		if err != nil {
			t.Fatal(err)
		}
		if env.From != 4 || env.To != 5 || !reflect.DeepEqual(got, msg) {
			t.Errorf("%T changed on the way: sent %+v from 4 to 5, got %+v from %d to %d", msg, msg, got, env.From, env.To)
		}
	}
	// A heartbeat has no entries, and must arrive with none, not an empty slice.
	hb := raft.AppendEntriesArgs{Term: 1, LeaderID: 2}
	if got, _ := fromEnvelope(toEnvelope(2, 1, hb)); !reflect.DeepEqual(got, hb) {
		t.Errorf("heartbeat changed: %+v", got)
	}
	// An envelope from the network with nothing in it is an error, not a panic.
	if _, err := fromEnvelope(&pb.Envelope{}); err == nil {
		t.Error("an empty envelope was accepted")
	}
}

// testCluster runs real nodes in this process, on 127.0.0.1, over real gRPC and
// real files. Only the clock is faster than in production.
type testCluster struct {
	t             *testing.T
	peers         map[raft.NodeID]string
	dirs          map[raft.NodeID]string
	nodes         map[raft.NodeID]*Server
	conns         map[raft.NodeID]*grpc.ClientConn
	snapshotEvery uint64
}

func startCluster(t *testing.T, size int, snapshotEvery uint64) *testCluster {
	t.Helper()
	c := &testCluster{
		t: t, peers: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{},
		nodes: map[raft.NodeID]*Server{}, conns: map[raft.NodeID]*grpc.ClientConn{}, snapshotEvery: snapshotEvery,
	}
	// Bind every port first, so every node can be given every address.
	listeners := map[raft.NodeID]net.Listener{}
	for i := 1; i <= size; i++ {
		id := raft.NodeID(i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id], c.peers[id] = lis, lis.Addr().String()
		c.dirs[id] = filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", id))
	}
	for id, lis := range listeners {
		c.start(id, lis)
	}
	t.Cleanup(func() {
		for _, conn := range c.conns {
			conn.Close()
		}
		for _, s := range c.nodes {
			s.Stop()
		}
	})
	return c
}

func (c *testCluster) start(id raft.NodeID, lis net.Listener) {
	c.t.Helper()
	s, err := Start(Config{
		ID: id, Peers: c.peers, DataDir: c.dirs[id], Listener: lis,
		// 5 ms ticks: elections time out after 250 to 500 ms. Faster than that,
		// and a busy machine running every package's tests at once can delay a
		// heartbeat enough to start an election nobody asked for.
		Tick: 5 * time.Millisecond, ElectionTicks: 50, HeartbeatTicks: 5,
		SnapshotEvery: c.snapshotEvery, TrailingEntries: 5,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id] = s
}

func (c *testCluster) stop(id raft.NodeID) {
	c.t.Helper()
	if err := c.nodes[id].Stop(); err != nil {
		c.t.Fatal(err)
	}
	delete(c.nodes, id)
}

// restart starts a stopped node again from its files, on its old address.
func (c *testCluster) restart(id raft.NodeID) {
	c.t.Helper()
	lis, err := net.Listen("tcp", c.peers[id])
	if err != nil {
		c.t.Fatal(err)
	}
	c.start(id, lis)
}

func (c *testCluster) status(id raft.NodeID) (*pb.StatusResponse, error) {
	conn, ok := c.conns[id]
	if !ok {
		var err error
		if conn, err = grpc.NewClient(c.peers[id], grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
			return nil, err
		}
		c.conns[id] = conn
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return pb.NewKVClient(conn).Status(ctx, &pb.StatusRequest{})
}

// waitFor polls cond until it holds, or fails the test after 10 s.
func (c *testCluster) waitFor(what string, cond func() bool) {
	c.t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	c.t.Fatalf("timed out waiting until %s", what)
}

// leader waits until every running node names the same leader in the same term.
func (c *testCluster) leader() raft.NodeID {
	c.t.Helper()
	var leader raft.NodeID
	c.waitFor("the nodes agree on a leader", func() bool {
		var term uint64
		leader = raft.None
		for id := range c.nodes {
			st, err := c.status(id)
			if err != nil || st.LeaderId == 0 || (leader != raft.None && (raft.NodeID(st.LeaderId) != leader || st.Term != term)) {
				return false
			}
			leader, term = raft.NodeID(st.LeaderId), st.Term
		}
		return leader != raft.None
	})
	return leader
}

func (c *testCluster) client(ids ...raft.NodeID) *client.Client {
	c.t.Helper()
	var addrs []string
	for _, id := range ids {
		addrs = append(addrs, c.peers[id])
	}
	cl, err := client.New(addrs, client.Options{})
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { cl.Close() })
	return cl
}

func ctx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func mustGet(t *testing.T, cl *client.Client, key, want string) {
	t.Helper()
	v, found, err := cl.Get(ctx(t), []byte(key))
	if err != nil || !found || string(v) != want {
		t.Fatalf("get %s = %q, found %v, err %v; want %q", key, v, found, err, want)
	}
}

// G2. For each node in turn, a client that knows only that node writes a key, and
// every key reads back through every node.
func TestClusterOverGRPC(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			c := startCluster(t, size, 0)
			c.leader()
			for id := range c.nodes {
				key, value := fmt.Sprintf("key-%d", id), fmt.Sprintf("value-%d", id)
				if err := c.client(id).Put(ctx(t), []byte(key), []byte(value)); err != nil {
					t.Fatalf("put through node %d: %v", id, err)
				}
			}
			for reader := range c.nodes {
				cl := c.client(reader)
				for id := range c.nodes {
					mustGet(t, cl, fmt.Sprintf("key-%d", id), fmt.Sprintf("value-%d", id))
				}
			}
		})
	}
}

// G3. A client that knows only a follower gets one redirect, to the leader.
func TestClientFollowsRedirect(t *testing.T) {
	c := startCluster(t, 3, 0)
	leader := c.leader()
	follower := leader%3 + 1
	var log []string
	cl, err := client.New([]string{c.peers[follower]}, client.Options{
		Log: func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Put(ctx(t), []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if cl.Redirects != 1 || len(log) != 1 || !strings.Contains(log[0], c.peers[leader]) {
		t.Fatalf("%d redirects, log %q; want one, to the leader at %s", cl.Redirects, log, c.peers[leader])
	}
	mustGet(t, cl, "k", "v")
}

// G4. Stop the leader: writes go on under a new one, and the old leader restarts
// from its files and catches up. The log is compacted often, so it has to catch
// up through InstallSnapshot, which sends the store over gRPC in one message.
func TestWritesContinueAfterLeaderStops(t *testing.T) {
	c := startCluster(t, 3, 20)
	cl := c.client(1, 2, 3)
	for i := 0; i < 10; i++ {
		if err := cl.Put(ctx(t), []byte(fmt.Sprintf("before-%d", i)), []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	old := c.leader()
	st, err := c.status(old)
	if err != nil {
		t.Fatal(err)
	}
	c.stop(old)

	for i := 0; i < 60; i++ {
		if err := cl.Put(ctx(t), []byte(fmt.Sprintf("after-%d", i)), []byte("y")); err != nil {
			t.Fatalf("write %d after the leader stopped: %v", i, err)
		}
	}
	leader := c.leader()
	if leader == old {
		t.Fatal("the stopped node is still the leader")
	}
	now, err := c.status(leader)
	if err != nil {
		t.Fatal(err)
	}
	if now.SnapshotIndex <= st.LastLogIndex {
		t.Fatalf("the leader kept every entry the old leader is missing (snapshot at %d, old log ends at %d), so this wouldn't test InstallSnapshot", now.SnapshotIndex, st.LastLogIndex)
	}

	c.restart(old)
	c.waitFor("the old leader catches up", func() bool {
		got, err := c.status(old)
		return err == nil && got.LastApplied >= now.CommitIndex
	})
	if got, _ := c.status(old); got.SnapshotIndex <= st.LastLogIndex {
		t.Fatalf("the old leader caught up without a snapshot: %+v", got)
	}
	mustGet(t, cl, "before-0", "x")
	mustGet(t, cl, "after-59", "y")
}

// G5. The same write sent twice, as a client does when it never heard back,
// applies once. An append shows it: applied twice, the value would be "xx".
func TestRetriedWriteAppliesOnce(t *testing.T) {
	c := startCluster(t, 3, 0)
	leader := c.leader()
	if _, err := c.status(leader); err != nil {
		t.Fatal(err)
	}
	kv := pb.NewKVClient(c.conns[leader])
	req := &pb.WriteRequest{ClientId: 42, Seq: 1, Op: pb.Op_OP_APPEND, Key: []byte("k"), Value: []byte("x")}
	for try := 1; try <= 2; try++ {
		resp, err := kv.Write(ctx(t), req)
		if err != nil || resp.NotLeader != nil {
			t.Fatalf("try %d: %v %v", try, resp, err)
		}
	}
	mustGet(t, c.client(leader), "k", "x")
}

// A write the store could misread is refused before it reaches the log.
func TestBadWritesAreRefused(t *testing.T) {
	c := startCluster(t, 1, 0)
	c.leader()
	if _, err := c.status(1); err != nil {
		t.Fatal(err)
	}
	kv := pb.NewKVClient(c.conns[1])
	for _, req := range []*pb.WriteRequest{
		{ClientId: 1, Seq: 1, Key: []byte("k")},                                               // no op
		{ClientId: 1, Seq: 0, Op: pb.Op_OP_PUT, Key: []byte("k")},                             // seq 0 would be dropped as a repeat
		{ClientId: 0, Seq: 1, Op: pb.Op_OP_PUT, Key: []byte("k")},                             // no client
		{ClientId: 1, Seq: 1, Op: pb.Op_OP_PUT},                                               // no key
		{ClientId: 1, Seq: 1, Op: pb.Op_OP_PUT, Key: []byte("k"), Value: make([]byte, 1<<20)}, // too large
	} {
		if _, err := kv.Write(ctx(t), req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%v: got %v, want InvalidArgument", req, err)
		}
	}
}
