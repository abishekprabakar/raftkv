package sim

import (
	"fmt"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// The checks, by the names used in docs/acceptance/M2.md and in reports.
const (
	CheckElectionSafety     = "election-safety"
	CheckLogMatching        = "log-matching"
	CheckLeaderCompleteness = "leader-completeness"
	CheckStateMachineSafety = "state-machine-safety"
	CheckLiveness           = "liveness"

	CheckVoteOnce        = "vote-once"
	CheckDurablePromise  = "durable-promise"
	CheckDurableApply    = "durable-apply"
	CheckCommitMajority  = "commit-majority"
	CheckCommitBound     = "commit-bound"
	CheckCommitMonotonic = "commit-monotonic"
	CheckTermMonotonic   = "term-monotonic"
	CheckStateOrder      = "state-order"
	CheckPanic           = "panic"
)

// Violation is the first check a schedule failed. A run stops there, because
// nothing after a broken invariant can be trusted.
type Violation struct {
	Check  string `json:"check"`
	AtUs   int64  `json:"at_us"`
	Node   uint64 `json:"node"`
	Detail string `json:"detail"`
}

type entryID struct{ index, term uint64 }

type voteKey struct {
	voter raft.NodeID
	term  uint64
}

type appliedRecord struct {
	term uint64
	hash uint64
}

// commitRecord remembers what was committed at an index and in which term that
// was first observed. A leader of an older term has no duty to hold it, because
// Leader Completeness only speaks about later terms.
type commitRecord struct {
	term     uint64
	hash     uint64
	observed uint64
}

// checker holds what is true of the whole schedule. Properties look at history
// rather than at the current moment, so a violation can't slip through because
// the evidence was later overwritten.
type checker struct {
	now            *int64
	nodes          int
	propertiesOnly bool
	violation      *Violation

	leaderOfTerm map[uint64]raft.NodeID
	prefixOf     map[entryID]uint64 // (index, term) -> hash of the log up to there
	appliedAt    map[uint64]appliedRecord
	committed    []commitRecord // committed[i] describes index i+1
	votes        map[voteKey]raft.NodeID
}

func newChecker(now *int64, nodes int, propertiesOnly bool) *checker {
	return &checker{
		now: now, nodes: nodes, propertiesOnly: propertiesOnly,
		leaderOfTerm: map[uint64]raft.NodeID{},
		prefixOf:     map[entryID]uint64{},
		appliedAt:    map[uint64]appliedRecord{},
		votes:        map[voteKey]raft.NodeID{},
	}
}

// fail records a broken property, and keeps only the first failure.
func (c *checker) fail(check string, node raft.NodeID, format string, args ...any) {
	if c.violation == nil {
		c.violation = &Violation{Check: check, AtUs: *c.now, Node: uint64(node), Detail: fmt.Sprintf(format, args...)}
	}
}

// monitor records a broken Figure 2 rule, unless monitors are switched off.
func (c *checker) monitor(check string, node raft.NodeID, format string, args ...any) {
	if !c.propertiesOnly {
		c.fail(check, node, format, args...)
	}
}

func (c *checker) quorum() int { return c.nodes/2 + 1 }

// sawLeader is Election Safety: at most one leader per term.
func (c *checker) sawLeader(id raft.NodeID, term uint64) {
	if other, ok := c.leaderOfTerm[term]; ok && other != id {
		c.fail(CheckElectionSafety, id, "nodes %d and %d both led term %d", other, id, term)
		return
	}
	c.leaderOfTerm[term] = id
}

// sawEntry is Log Matching. A running hash makes each new entry O(1): if any two
// logs ever disagree before an entry with the same index and term, the hashes
// of that prefix differ.
func (c *checker) sawEntry(id raft.NodeID, e raft.Entry, prefix uint64) {
	key := entryID{e.Index, e.Term}
	if h, ok := c.prefixOf[key]; ok && h != prefix {
		c.fail(CheckLogMatching, id, "index %d term %d has a different log before it than another copy", e.Index, e.Term)
		return
	}
	c.prefixOf[key] = prefix
}

// sawApply is State Machine Safety.
func (c *checker) sawApply(id raft.NodeID, e raft.Entry) {
	rec := appliedRecord{e.Term, entryHash(e)}
	if prev, ok := c.appliedAt[e.Index]; ok && prev != rec {
		c.fail(CheckStateMachineSafety, id, "index %d applied as term %d, but another apply had term %d", e.Index, e.Term, prev.term)
		return
	}
	c.appliedAt[e.Index] = rec
}

// sawGrant is vote-once.
func (c *checker) sawGrant(voter, candidate raft.NodeID, term uint64) {
	key := voteKey{voter, term}
	if prev, ok := c.votes[key]; ok && prev != candidate {
		c.monitor(CheckVoteOnce, voter, "granted term %d to both %d and %d", term, prev, candidate)
		return
	}
	c.votes[key] = candidate
}

// sawCommit is called for every index a node's commit index passes. An index
// seen for the first time must already be on a majority of disks (holders is
// the count), and every later sighting must agree with the first.
func (c *checker) sawCommit(id raft.NodeID, index, term, hash, nodeTerm uint64, holders int) {
	if index <= uint64(len(c.committed)) {
		rec := c.committed[index-1]
		if rec.term != term || rec.hash != hash {
			c.fail(CheckStateMachineSafety, id, "index %d committed as term %d, but another node committed term %d", index, term, rec.term)
		}
		return
	}
	if index != uint64(len(c.committed))+1 {
		panic(fmt.Sprintf("sim: commit of index %d skipped ahead of %d", index, len(c.committed)))
	}
	if holders < c.quorum() {
		c.monitor(CheckCommitMajority, id, "index %d committed with %d copies on disk, majority is %d", index, holders, c.quorum())
	}
	c.committed = append(c.committed, commitRecord{term, hash, nodeTerm})
}

// checkLeaderHolds is Leader Completeness for one leader: logTerm reads the
// leader's log, and returns 0 for an index it does not hold.
func (c *checker) checkLeaderHolds(id raft.NodeID, leaderTerm uint64, logTerm func(uint64) uint64) {
	for i, rec := range c.committed {
		index := uint64(i + 1)
		if rec.observed <= leaderTerm && logTerm(index) != rec.term {
			c.fail(CheckLeaderCompleteness, id, "leader of term %d has term %d at index %d, but term %d was committed there", leaderTerm, logTerm(index), index, rec.term)
			return
		}
	}
}

// checkLeaderHoldsOne is Leader Completeness for a newly committed index.
func (c *checker) checkLeaderHoldsOne(id raft.NodeID, leaderTerm, index uint64, logTerm func(uint64) uint64) {
	rec := c.committed[index-1]
	if rec.observed <= leaderTerm && logTerm(index) != rec.term {
		c.fail(CheckLeaderCompleteness, id, "leader of term %d has term %d at index %d, but term %d was committed there", leaderTerm, logTerm(index), index, rec.term)
	}
}
