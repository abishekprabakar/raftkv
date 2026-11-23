package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/abishekprabakar/raftkv/internal/kv"
	"github.com/abishekprabakar/raftkv/internal/pb"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// Two questions an orchestrator asks about a node, and they must not be mixed up.
//
// Live: is this process working? If not, restart it. A node with no leader, or
// one cut off from its peers, is still working. Restarting it would fix nothing
// and would throw away a healthy process, so liveness looks only at the loop.
//
// Ready: is this node useful to the cluster right now? Kubernetes uses the answer
// three times. A Service only routes clients to ready pods, a rolling update only
// moves on to the next pod once the one it just replaced is ready, and a
// disruption budget only counts ready pods as healthy. If "ready" meant "the
// process is up", a rolling update would stop the second node while the first
// was still catching up, and a three-node cluster would be down to one node that
// has all the data. So ready has to mean what Raft means by it.

// healthTimeout bounds one health check. The kubelet's own timeout is longer, so
// it sees this node's answer and reason rather than a hang.
const healthTimeout = time.Second

// Health is one answer to /livez or /readyz.
type Health struct {
	OK bool `json:"ok"`
	// Reason says why, in words. It is what `kubectl describe pod` shows when a
	// probe fails.
	Reason string `json:"reason"`

	ID          uint64 `json:"id,omitempty"`
	Role        string `json:"role,omitempty"`
	Term        uint64 `json:"term,omitempty"`
	Leader      uint64 `json:"leader,omitempty"`
	CommitIndex uint64 `json:"commit_index,omitempty"`
	LastApplied uint64 `json:"last_applied,omitempty"`
	// LeaderCommit is the leader's commit index: its own for a leader, and for a
	// follower what the leader answered when this check asked it.
	LeaderCommit uint64 `json:"leader_commit,omitempty"`
}

// leaderView is what the leader said when a follower asked it for its Status.
type leaderView struct {
	err    error // the leader could not be asked
	role   string
	term   uint64
	commit uint64
}

// askLeader asks another node for its Status, over the connection this node
// already keeps open to it.
func (s *Server) askLeader(ctx context.Context, leader raft.NodeID) leaderView {
	p, ok := s.peers[leader]
	if !ok {
		return leaderView{err: fmt.Errorf("node %d is not a peer", leader)}
	}
	st, err := pb.NewKVClient(p.connection()).Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return leaderView{err: err}
	}
	return leaderView{role: st.Role, term: st.Term, commit: st.CommitIndex}
}

// followerReady is the readiness rule for a node that is not the leader: it
// follows a leader, that leader agrees it leads this term, and this node has
// applied what the leader has committed, give or take the allowance.
//
// The mark to measure against has to come from the leader, and it has to be
// fresh. A follower's own commit index can't serve: it is min(the leader's
// commit index, the end of the entries received so far), so a follower in the
// middle of a long catch-up has applied everything it has committed and looks up
// to date. The commit index inside the last AppendEntries can't serve either,
// though it is the leader's own number, and the first version of this rule used
// it. While a follower is down, the leader's messages to it wait in a queue, and
// when it returns they arrive oldest first. The oldest carries the commit index
// from the moment the follower went down, which is exactly how far the follower
// has applied. For a few milliseconds a node with everything still to catch up
// on measured itself against a stale mark and called itself ready. So the leader
// is asked now, each time the question is.
func followerReady(st raft.Status, leader leaderView, allowance uint64) (bool, string) {
	switch {
	case st.Role == raft.Candidate:
		return false, fmt.Sprintf("candidate in term %d: no leader", st.Term)
	case st.Leader == raft.None:
		return false, fmt.Sprintf("no leader heard from in term %d", st.Term)
	case leader.err != nil:
		return false, fmt.Sprintf("node %d, the leader, did not answer: %v", st.Leader, leader.err)
	case leader.role != raft.Leader.String() || leader.term != st.Term:
		return false, fmt.Sprintf("node %d is followed as leader of term %d, and says it is %s in term %d", st.Leader, st.Term, leader.role, leader.term)
	case st.LastApplied+allowance < leader.commit:
		return false, fmt.Sprintf("catching up: applied %d, the leader has committed %d", st.LastApplied, leader.commit)
	}
	return true, fmt.Sprintf("following node %d", st.Leader)
}

// Live reports whether the loop takes a request in time.
func (s *Server) Live(ctx context.Context) Health {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	done := make(chan struct{}, 1)
	if err := s.do(ctx, func() error { done <- struct{}{}; return nil }); err != nil {
		return Health{Reason: "the node's loop did not take a request: " + err.Error()}
	}
	if _, err := await(ctx, s, done); err != nil {
		return Health{Reason: "the node's loop did not answer: " + err.Error()}
	}
	return Health{OK: true, Reason: "the node's loop answered"}
}

// Ready reports whether this node is useful to the cluster right now. See the
// comment at the top of this file, and docs/acceptance/k8s.md for the rule.
func (s *Server) Ready(ctx context.Context) Health {
	ctx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()
	viewed := make(chan raft.Status, 1) // buffered: the loop must never wait on a handler
	confirmed := make(chan error, 1)    // only a leader sends on this
	err := s.do(ctx, func() error {
		st := s.kv.Status()
		viewed <- st
		if st.Role != raft.Leader {
			return nil
		}
		// A leader proves itself the way it does for a read: a round of
		// messages answered by a majority. One that has been cut off still
		// believes it leads, and this is what tells the two apart.
		return s.kv.Confirm(func(err error) { confirmed <- err })
	})
	if err != nil {
		return Health{Reason: "the node's loop did not take a request: " + err.Error()}
	}
	st, err := await(ctx, s, viewed)
	if err != nil {
		return Health{Reason: "the node's loop did not answer: " + err.Error()}
	}
	h := Health{
		ID: uint64(st.ID), Role: st.Role.String(), Term: st.Term, Leader: uint64(st.Leader),
		CommitIndex: st.CommitIndex, LastApplied: st.LastApplied,
	}
	if st.Role != raft.Leader {
		// This node's applied index was read first and the leader is asked
		// second. Applied only grows, so if it was within the allowance of what
		// the leader reports afterwards, it still is.
		var leader leaderView
		if st.Role == raft.Follower && st.Leader != raft.None {
			leader = s.askLeader(ctx, st.Leader)
			h.LeaderCommit = leader.commit
		}
		h.OK, h.Reason = followerReady(st, leader, s.cfg.ReadyLag)
		return h
	}
	h.LeaderCommit = st.CommitIndex
	switch err, waitErr := await(ctx, s, confirmed); {
	case waitErr != nil:
		h.Reason = "leader, but no majority confirmed it within " + healthTimeout.String()
	case err == nil:
		h.OK, h.Reason = true, "leader, confirmed by a majority"
	case errors.Is(err, kv.ErrNotLeader):
		h.Reason = "lost the leadership while checking"
	default:
		// ErrRetry: nothing committed in this term yet, or the round expired.
		h.Reason = "leader, but not confirmed by a majority yet"
	}
	return h
}

// HealthHandler serves /livez and /readyz: 200 when the answer is yes, 503 when
// it is no, and the Health as JSON either way.
func (s *Server) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	serve := func(check func(context.Context) Health) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			h := check(r.Context())
			if !h.OK {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			json.NewEncoder(w).Encode(h)
		}
	}
	mux.HandleFunc("/livez", serve(s.Live))
	mux.HandleFunc("/readyz", serve(s.Ready))
	return mux
}
