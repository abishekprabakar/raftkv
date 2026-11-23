package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// probe asks a node's health endpoint the way the kubelet does, over HTTP.
func probe(t *testing.T, s *Server, path string) (int, Health) {
	t.Helper()
	srv := httptest.NewServer(s.HealthHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var h Health
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, h
}

// H1. One node of three, started alone, can never elect a leader. It is alive, so
// restarting it would be wrong, and it is not ready, so nothing should be sent to it.
func TestLonelyNodeIsLiveButNotReady(t *testing.T) {
	c := &testCluster{t: t, peers: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*Server{}}
	listeners := map[raft.NodeID]net.Listener{}
	for i := 1; i <= 3; i++ {
		id := raft.NodeID(i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id], c.peers[id] = lis, lis.Addr().String()
		c.dirs[id] = filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", id))
	}
	// Nodes 2 and 3 never start: their ports refuse connections.
	listeners[2].Close()
	listeners[3].Close()
	c.start(1, listeners[1])
	t.Cleanup(func() { c.nodes[1].Stop() })

	// Long enough for several election timeouts (250 to 500 ms each here).
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if code, h := probe(t, c.nodes[1], "/livez"); code != http.StatusOK || !h.OK {
			t.Fatalf("/livez = %d %+v; a node with no leader is still alive", code, h)
		}
		if code, h := probe(t, c.nodes[1], "/readyz"); code != http.StatusServiceUnavailable || h.OK {
			t.Fatalf("/readyz = %d %+v; a node that can't reach a majority must not be ready", code, h)
		}
	}
	_, h := probe(t, c.nodes[1], "/readyz")
	if h.Role == "leader" || h.Reason == "" {
		t.Errorf("a lone node reported %+v", h)
	}
}

// H2. Every node of a healthy cluster turns ready once a leader is elected.
func TestClusterBecomesReady(t *testing.T) {
	c := startCluster(t, 3, 0)
	leader := c.leader()
	for id, s := range c.nodes {
		c.waitFor(fmt.Sprintf("node %d is ready", id), func() bool {
			code, h := probe(t, s, "/readyz")
			return code == http.StatusOK && h.OK
		})
	}
	_, h := probe(t, c.nodes[leader], "/readyz")
	if h.Role != "leader" || h.Leader != uint64(leader) {
		t.Errorf("the leader's /readyz says %+v", h)
	}
	cl := c.client(leader)
	if err := cl.Put(ctx(t), []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, cl, "k", "v")
	// Followers stay ready after a write: they have applied what the leader committed.
	for id, s := range c.nodes {
		c.waitFor(fmt.Sprintf("node %d is ready after the write", id), func() bool {
			_, h := probe(t, s, "/readyz")
			return h.OK && h.LastApplied >= 2 // the leader's no-op and the put
		})
	}
}

// H3. The rule for a node that is not the leader.
func TestFollowerBehindIsNotReady(t *testing.T) {
	const allowance = 64
	follower := func(applied uint64) raft.Status {
		return raft.Status{ID: 2, Role: raft.Follower, Term: 7, Leader: 1, CommitIndex: applied, LastApplied: applied}
	}
	leaderAt := func(commit uint64) leaderView { return leaderView{role: "leader", term: 7, commit: commit} }
	cases := []struct {
		name   string
		st     raft.Status
		leader leaderView
		want   bool
	}{
		{"caught up", follower(1000), leaderAt(1000), true},
		{"one entry behind", follower(999), leaderAt(1000), true},
		{"behind by exactly the allowance", follower(1000 - allowance), leaderAt(1000), true},
		{"behind by one more than the allowance", follower(1000 - allowance - 1), leaderAt(1000), false},
		// A restarted node partway through catching up has applied everything it
		// has committed, so its own indexes look fine. Only the leader's commit
		// index shows how far it still has to go.
		{"restarted and far behind", follower(64), leaderAt(50_000), false},
		// What the second acceptance run caught: the node has applied 3 entries,
		// and the last message it accepted said the leader had committed 3,
		// because that message had waited in a queue while 300 more were
		// committed. Asked now, the leader says 303.
		{"restarted, and the last message it accepted was an old one", follower(3), leaderAt(303), false},
		{"the leader did not answer", follower(1000), leaderView{err: errors.New("connection refused")}, false},
		{"the node it follows has moved to a newer term", follower(1000), leaderView{role: "follower", term: 8, commit: 1000}, false},
		{"the node it follows no longer leads", follower(1000), leaderView{role: "candidate", term: 7, commit: 1000}, false},
		{"no leader", raft.Status{ID: 2, Role: raft.Follower, Term: 7, LastApplied: 1000}, leaderAt(1000), false},
		{"candidate", raft.Status{ID: 2, Role: raft.Candidate, Term: 8, LastApplied: 1000}, leaderAt(1000), false},
	}
	for _, tc := range cases {
		if got, reason := followerReady(tc.st, tc.leader, allowance); got != tc.want || reason == "" {
			t.Errorf("%s: ready = %v (%q), want %v", tc.name, got, reason, tc.want)
		}
	}
}

// H4. A leader that has lost its majority still believes it leads. It must stop
// being ready, and it must stay alive.
func TestLeaderWithoutMajorityIsNotReady(t *testing.T) {
	c := startCluster(t, 3, 0)
	leader := c.leader()
	s := c.nodes[leader]
	c.waitFor("the leader is ready", func() bool { _, h := probe(t, s, "/readyz"); return h.OK })

	for id := range c.peers {
		if id != leader {
			c.stop(id)
		}
	}
	c.waitFor("the leader stops being ready", func() bool {
		code, h := probe(t, s, "/readyz")
		return code == http.StatusServiceUnavailable && !h.OK
	})
	_, h := probe(t, s, "/readyz")
	if h.Role != "leader" {
		t.Logf("the node had already stepped down: %+v", h)
	}
	if code, h := probe(t, s, "/livez"); code != http.StatusOK || !h.OK {
		t.Errorf("/livez = %d %+v; losing the majority must not look like a dead process", code, h)
	}
}

// Regression test for a bug the second acceptance run caught. Readiness used to
// measure a follower against the commit index in the last AppendEntries it had
// accepted. While a follower is down, the leader's messages to it pile up in a
// queue, and when it comes back they arrive oldest first. The oldest carries the
// commit index from the moment the follower went down, which is exactly as far as
// the follower has applied, so for a moment it looked caught up with everything
// still to do. A probe that landed in that moment got "ready".
//
// Now a follower asks the leader for its commit index when the question is asked.
// This test stops a follower, makes it miss 300 writes, restarts it, and asks as
// fast as it can: the first "ready" must come with those writes applied.
func TestRestartedFollowerIsNotReadyUntilItHasCaughtUp(t *testing.T) {
	const missed = 300
	c := startCluster(t, 3, 0)
	leader := c.leader()
	var follower raft.NodeID
	for id := range c.peers {
		if id != leader {
			follower = id
			break
		}
	}
	cl := c.client(leader)
	c.stop(follower)
	for i := range missed {
		if err := cl.Put(ctx(t), []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	c.restart(follower)
	s := c.nodes[follower]
	var h Health
	for deadline := time.Now().Add(10 * time.Second); !h.OK; {
		if time.Now().After(deadline) {
			t.Fatalf("the restarted follower never became ready: %+v", h)
		}
		h = s.Ready(context.Background()) // no pause: the window was a few milliseconds wide
	}
	if h.LastApplied+s.cfg.ReadyLag < missed {
		t.Errorf("ready with applied %d (the leader's commit index reported as %d), after %d writes it missed", h.LastApplied, h.LeaderCommit, missed)
	}
}

// The health checks run through the loop like everything else, so they end when
// the node stops instead of hanging.
func TestHealthAfterStop(t *testing.T) {
	c := startCluster(t, 3, 0)
	leader := c.leader()
	s := c.nodes[leader]
	c.stop(leader)
	if h := s.Live(context.Background()); h.OK {
		t.Errorf("a stopped node is live: %+v", h)
	}
	if h := s.Ready(context.Background()); h.OK {
		t.Errorf("a stopped node is ready: %+v", h)
	}
}
