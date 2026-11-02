package sim

import (
	"errors"
	"hash/fnv"

	"github.com/abishekprabakar/raftkv/internal/kv"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// errCrash is what Save returns when the simulated power goes out: the update
// never reached the disk and the node is gone.
var errCrash = errors.New("sim: crashed during save")

// disk is one node's durable Raft state. It lasts across crashes while the Node
// above it does not. It keeps a running hash of the log so Log Matching costs O(1)
// per entry, however long the log grows.
type disk struct {
	hard     raft.HardState
	snap     raft.Snapshot
	snapHash uint64
	entries  []raft.Entry // the log after snap
	hashes   []uint64     // hashes[i] covers the whole log through entries[i]
}

func (d *disk) lastIndex() uint64 { return d.snap.Index + uint64(len(d.entries)) }

// term returns the term at index, or 0 if the disk no longer knows it.
func (d *disk) term(index uint64) uint64 {
	switch {
	case index == d.snap.Index:
		return d.snap.Term
	case index < d.snap.Index || index > d.lastIndex():
		return 0
	}
	return d.entries[index-d.snap.Index-1].Term
}

// holds reports whether the disk has this entry. Anything before the snapshot was
// committed and is held by the state machine, so it counts.
func (d *disk) holds(index, term uint64) bool {
	return index < d.snap.Index || d.term(index) == term
}

func (d *disk) hashAt(index uint64) uint64 {
	if index == d.snap.Index {
		return d.snapHash
	}
	return d.hashes[index-d.snap.Index-1]
}

func (d *disk) add(e raft.Entry) uint64 {
	prev := d.snapHash
	if n := len(d.hashes); n > 0 {
		prev = d.hashes[n-1]
	}
	h := mix(mix(prev, e.Index), entryHash(e))
	d.entries = append(d.entries, e)
	d.hashes = append(d.hashes, h)
	return h
}

func (d *disk) truncate(from uint64) {
	keep := from - d.snap.Index - 1
	d.entries = d.entries[:keep]
	d.hashes = d.hashes[:keep]
}

func entryHash(e raft.Entry) uint64 {
	f := fnv.New64a()
	f.Write(e.Data)
	return mix(mix(e.Term, uint64(e.Kind)), f.Sum64())
}

// mix folds a value into a hash. The trace hash and the log hashes both use it,
// and it depends on nothing but its inputs.
func mix(h, v uint64) uint64 {
	h ^= v
	h *= 0x100000001b3
	h ^= h >> 29
	return h * 0xbf58476d1ce4e5b9
}

// host is the world around one node: its disk, its state machine's backend, and
// its link to the network. It implements raft.Storage and raft.Transport, which
// is how the simulator sees everything a node does.
type host struct {
	s       *Sim
	id      raft.NodeID
	server  *kv.Server // nil while crashed
	disk    disk
	backend kv.Backend

	incarnation int  // ticks scheduled for an earlier life are ignored
	dying       bool // this call ends in a crash; nothing else may leave
	crashKind   string

	// Reset on every restart.
	status     raft.Status
	seenCommit uint64
	applied    uint64
	seenLeader uint64 // the last term this node was checked as a new leader
}

// Load implements raft.Storage.
func (h *host) Load() (raft.Durable, error) {
	return raft.Durable{Hard: h.disk.hard, Snapshot: h.disk.snap, Entries: h.disk.entries}, nil
}

// Save implements raft.Storage. This is where a crash can fall inside a call:
// before the update is durable, or after it is durable but before any message
// has left. A Node that orders its work correctly can survive both.
func (h *host) Save(u raft.Update) error {
	s := h.s
	if s.chance(s.sched.CrashDuringSave) {
		h.dying, h.crashKind = true, crashDuringSave
		return errCrash
	}
	if u.HardState != nil {
		if u.HardState.CurrentTerm < h.disk.hard.CurrentTerm {
			s.check.monitor(CheckTermMonotonic, h.id, "term on disk went from %d to %d", h.disk.hard.CurrentTerm, u.HardState.CurrentTerm)
		}
		h.disk.hard = *u.HardState
	}
	if u.Snapshot != nil {
		hash := s.check.prefixOf[entryID{u.Snapshot.Index, u.Snapshot.Term}]
		h.disk = disk{hard: h.disk.hard, snap: *u.Snapshot, snapHash: hash}
	}
	if u.TruncateFrom != 0 {
		h.disk.truncate(u.TruncateFrom)
	}
	for _, e := range u.Entries {
		s.check.sawEntry(h.id, e, h.disk.add(e))
	}
	if s.chance(s.sched.CrashAfterSave) {
		h.dying, h.crashKind = true, crashAfterSave
	}
	return nil
}

// Send implements raft.Transport. Before a message leaves, everything it
// promises must already be on this node's disk.
func (h *host) Send(to raft.NodeID, msg raft.Message) {
	if h.dying {
		return
	}
	h.checkPromise(to, msg)
	h.s.send(h.id, to, msg)
}

func (h *host) checkPromise(to raft.NodeID, msg raft.Message) {
	c := h.s.check
	hard := h.disk.hard
	switch m := msg.(type) {
	case raft.RequestVoteArgs:
		if hard.CurrentTerm != m.Term || hard.VotedFor != h.id {
			c.monitor(CheckDurablePromise, h.id, "RequestVote for term %d with term %d, vote %d on disk", m.Term, hard.CurrentTerm, hard.VotedFor)
		}
	case raft.RequestVoteReply:
		if !m.VoteGranted {
			return
		}
		if hard.CurrentTerm != m.Term || hard.VotedFor != to {
			c.monitor(CheckDurablePromise, h.id, "vote for %d in term %d with term %d, vote %d on disk", to, m.Term, hard.CurrentTerm, hard.VotedFor)
		}
		c.sawGrant(h.id, to, m.Term)
	case raft.AppendEntriesArgs:
		last := m.PrevLogIndex + uint64(len(m.Entries))
		if hard.CurrentTerm < m.Term || h.disk.lastIndex() < last {
			c.monitor(CheckDurablePromise, h.id, "AppendEntries through %d in term %d with term %d, log to %d on disk", last, m.Term, hard.CurrentTerm, h.disk.lastIndex())
		}
	case raft.AppendEntriesReply:
		if m.Success && (hard.CurrentTerm < m.Term || h.disk.lastIndex() < m.MatchIndex) {
			c.monitor(CheckDurablePromise, h.id, "acknowledged through %d in term %d with term %d, log to %d on disk", m.MatchIndex, m.Term, hard.CurrentTerm, h.disk.lastIndex())
		}
	}
}

// onApply watches every entry reach the state machine.
func (h *host) onApply(e raft.Entry, duplicate bool) {
	s := h.s
	if e.Index != h.applied+1 {
		s.check.monitor(CheckStateOrder, h.id, "applied index %d after %d", e.Index, h.applied)
	}
	h.applied = e.Index
	if !h.disk.holds(e.Index, e.Term) {
		s.check.monitor(CheckDurableApply, h.id, "applied index %d of term %d with term %d on disk", e.Index, e.Term, h.disk.term(e.Index))
	}
	s.check.sawApply(h.id, e)
	if duplicate {
		s.stats.Deduped++
	}
}

func (h *host) onRestore(index uint64) {
	h.applied = index
	h.s.stats.SnapshotsInstalled++
	h.s.tracef("node %d restored a snapshot through %d", h.id, index)
}

func (h *host) onCompact(index uint64) {
	h.s.stats.Compactions++
	h.s.tracef("node %d compacted through %d", h.id, index)
}

// afterStep checks the node after one input has fully finished.
func (h *host) afterStep() {
	s, c := h.s, h.s.check
	st := h.server.Status()
	prev := h.status
	h.status = st

	if st.Term < prev.Term {
		c.monitor(CheckTermMonotonic, h.id, "term went from %d to %d", prev.Term, st.Term)
	}
	if st.CommitIndex < prev.CommitIndex {
		c.monitor(CheckCommitMonotonic, h.id, "commit index went from %d to %d", prev.CommitIndex, st.CommitIndex)
	}
	if st.LastApplied > st.CommitIndex || st.CommitIndex > st.LastLogIndex {
		c.monitor(CheckStateOrder, h.id, "applied %d, commit %d, last %d", st.LastApplied, st.CommitIndex, st.LastLogIndex)
	}
	if st.LastLogIndex != h.disk.lastIndex() || st.LastLogTerm != h.disk.term(h.disk.lastIndex()) {
		c.monitor(CheckStateOrder, h.id, "memory log ends at %d term %d, disk at %d term %d", st.LastLogIndex, st.LastLogTerm, h.disk.lastIndex(), h.disk.term(h.disk.lastIndex()))
	}

	if st.Role == raft.Leader {
		c.sawLeader(h.id, st.Term)
		if h.seenLeader != st.Term {
			h.seenLeader = st.Term
			s.stats.LeadersElected++
			c.checkLeaderHolds(h.id, st.Term, h.disk.holds)
		}
	}

	// The commit index can pass entries this node holds only in memory if a bug
	// let it. There is then nothing on disk to inspect, and the monitors above
	// have already reported it. Entries inside a snapshot arrived already committed
	// and left no entry to look at.
	last := min(st.CommitIndex, h.disk.lastIndex())
	for index := max(h.seenCommit, h.disk.snap.Index) + 1; index <= last; index++ {
		term := h.disk.term(index)
		c.sawCommit(h.id, index, term, h.disk.hashAt(index), st.Term, s.copiesOnDisk(index, term))
		for _, other := range s.hosts {
			if other.server != nil && other.status.Role == raft.Leader {
				c.checkLeaderHoldsOne(other.id, other.status.Term, index, other.disk.holds)
			}
		}
	}
	h.seenCommit = max(h.seenCommit, last)
}
