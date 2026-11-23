package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/abishekprabakar/raftkv/internal/kv"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// Two questions an orchestrator asks about a node, and they must not be mixed up.
//
// Live: is this process working? If not, restart it. A node with no leader, or
// one cut off from its peers, is still working. Restarting it would fix nothing
// and would throw away a healthy process, so liveness looks only at the loop.
//
// Ready: is this node useful to the cluster right now? Kubernetes uses the answer
// twice. A Service only routes clients to ready pods, and a rolling update only
// moves on to the next pod once the one it just replaced is ready. If "ready"
// meant "the process is up", a rolling update would stop the second node while
// the first was still catching up, and a three-node cluster would be down to one
// node that has all the data. So ready has to mean what Raft means by it.

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
	// LeaderCommit is the commit index the leader last told this follower.
	LeaderCommit uint64 `json:"leader_commit,omitempty"`
}

// leaderSeen is what a follower last heard from its leader. Only the loop
// goroutine touches it.
type leaderSeen struct {
	term   uint64 // the term the message was accepted in
	commit uint64 // the highest commit index the leader has named in that term
}

// noteLeader runs on the loop after every message. A follower's own commit index
// can't say whether it is caught up: it is min(the leader's commit index, the end
// of the entries received so far), so a follower in the middle of a long catch-up
// has applied everything it has committed and still looks up to date. The
// leader's own commit index, which every AppendEntries carries, is the mark to
// measure against.
func (s *Server) noteLeader(msg raft.Message) {
	var term, commit uint64
	switch m := msg.(type) {
	case raft.AppendEntriesArgs:
		term, commit = m.Term, m.LeaderCommit
	case raft.InstallSnapshotArgs:
		term, commit = m.Term, m.LastIncludedIndex // a snapshot only holds committed entries
	default:
		return
	}
	// A message from an old term was refused, so it says nothing.
	if st := s.kv.Status(); st.Term != term || st.Role != raft.Follower {
		return
	}
	if s.seen.term != term {
		s.seen = leaderSeen{term: term}
	}
	// Messages can arrive out of order, and a commit index never goes back.
	s.seen.commit = max(s.seen.commit, commit)
}

// followerReady is the readiness rule for a node that is not the leader.
func followerReady(st raft.Status, seen leaderSeen, allowance uint64) (bool, string) {
	switch {
	case st.Role == raft.Candidate:
		return false, fmt.Sprintf("candidate in term %d: no leader", st.Term)
	case st.Leader == raft.None || seen.term != st.Term:
		return false, fmt.Sprintf("no leader heard from in term %d", st.Term)
	case st.LastApplied+allowance < seen.commit:
		return false, fmt.Sprintf("catching up: applied %d, the leader has committed %d", st.LastApplied, seen.commit)
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
	type view struct {
		st   raft.Status
		seen leaderSeen
	}
	viewed := make(chan view, 1)     // buffered: the loop must never wait on a handler
	confirmed := make(chan error, 1) // only a leader sends on this
	err := s.do(ctx, func() error {
		st := s.kv.Status()
		viewed <- view{st, s.seen}
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
	v, err := await(ctx, s, viewed)
	if err != nil {
		return Health{Reason: "the node's loop did not answer: " + err.Error()}
	}
	h := Health{
		ID: uint64(v.st.ID), Role: v.st.Role.String(), Term: v.st.Term, Leader: uint64(v.st.Leader),
		CommitIndex: v.st.CommitIndex, LastApplied: v.st.LastApplied, LeaderCommit: v.seen.commit,
	}
	if v.st.Role != raft.Leader {
		h.OK, h.Reason = followerReady(v.st, v.seen, s.cfg.ReadyLag)
		return h
	}
	h.LeaderCommit = v.st.CommitIndex
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
