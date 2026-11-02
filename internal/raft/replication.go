package raft

import (
	"fmt"
	"slices"
)

// Log replication and commitment: sections 5.3 and 5.4 of the paper.

func (n *Node) appendAsLeader(kind EntryKind, data []byte) Entry {
	e := Entry{Index: n.log.lastIndex() + 1, Term: n.currentTerm, Kind: kind, Data: data}
	n.appendToLog(e)
	return e
}

func (n *Node) appendToLog(entries ...Entry) {
	n.log.append(entries...)
	n.unsaved = append(n.unsaved, entries...)
}

// broadcastAppend sends every peer what it is missing, or a heartbeat if it is
// missing nothing. A lost message needs no timer of its own, because the next
// heartbeat repeats it.
func (n *Node) broadcastAppend() {
	n.heartbeatElapsed = 0
	for _, p := range n.peers {
		n.sendAppend(p)
	}
}

func (n *Node) sendAppend(to NodeID) {
	prev := n.nextIndex[to] - 1
	n.send(to, AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  n.log.term(prev),
		Entries:      n.log.slice(prev+1, n.cfg.MaxEntriesPerMessage),
		LeaderCommit: n.commitIndex,
	})
}

func (n *Node) handleAppendEntries(from NodeID, m AppendEntriesArgs) {
	if m.Term < n.currentTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm})
		return
	}
	if n.role == Leader {
		panic(fmt.Sprintf("raft: node %d is leader of term %d and heard from leader %d of the same term", n.id, m.Term, m.LeaderID))
	}
	// This message is from the legitimate leader of the current term.
	n.role = Follower
	n.leaderID = m.LeaderID
	n.resetElectionTimer()

	// Consistency check: if the follower holds the entry just before these
	// ones, Log Matching says everything before that is identical too.
	if m.PrevLogIndex > n.log.lastIndex() {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, ConflictIndex: n.log.lastIndex() + 1})
		return
	}
	if t := n.log.term(m.PrevLogIndex); t != m.PrevLogTerm {
		n.send(from, AppendEntriesReply{Term: n.currentTerm, ConflictTerm: t, ConflictIndex: n.log.firstIndexOfTerm(m.PrevLogIndex)})
		return
	}

	// Cut the log only at a real conflict. A delayed copy of an old message may
	// carry fewer entries than the log already holds, and everything past its
	// end may be committed, so "replace everything after PrevLogIndex" would
	// lose committed entries.
	for i, e := range m.Entries {
		if e.Index <= n.log.lastIndex() {
			if n.log.term(e.Index) == e.Term {
				continue
			}
			n.truncateLog(e.Index)
		}
		n.appendToLog(m.Entries[i:]...)
		break
	}

	// Only entries up to lastNew are known to match the leader. Anything past
	// that in this log may be stale, even at an index the leader has committed.
	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if m.LeaderCommit > n.commitIndex {
		// Figure 2 says min(leaderCommit, lastNew). The max is needed because
		// with messages reordered, lastNew can be below a commit index learned
		// from a newer message, and commitIndex must never go backwards.
		n.commitIndex = max(n.commitIndex, min(m.LeaderCommit, lastNew))
	}
	n.send(from, AppendEntriesReply{Term: n.currentTerm, Success: true, MatchIndex: lastNew})
}

func (n *Node) truncateLog(index uint64) {
	if index <= n.commitIndex {
		panic(fmt.Sprintf("raft: node %d truncating committed index %d (commit %d)", n.id, index, n.commitIndex))
	}
	n.log.truncateFrom(index)
	n.truncateFrom = index
}

func (n *Node) handleAppendEntriesReply(from NodeID, m AppendEntriesReply) {
	// A reply from an earlier term says nothing about the current log.
	if n.role != Leader || m.Term != n.currentTerm {
		return
	}
	if m.Success {
		// Replies can arrive out of order, so these only ever move forward.
		if m.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = m.MatchIndex
			n.advanceCommit()
		}
		n.nextIndex[from] = max(n.nextIndex[from], n.matchIndex[from]+1)
		if n.nextIndex[from] <= n.log.lastIndex() {
			n.sendAppend(from)
		}
		return
	}
	// Rejected: back up and retry. If this log has entries from the follower's
	// conflicting term, continue just after them. Otherwise skip that whole term.
	next := m.ConflictIndex
	if m.ConflictTerm != 0 {
		if last := n.log.lastIndexOfTerm(m.ConflictTerm); last != 0 {
			next = last + 1
		}
	}
	// A late rejection can name a point this follower has already passed. Never
	// go back past what it has confirmed, and never forward on a rejection.
	n.nextIndex[from] = max(n.matchIndex[from]+1, min(next, n.nextIndex[from]))
	n.sendAppend(from)
}

// advanceCommit finds the highest index stored on a majority. Sorted ascending,
// the value quorum places from the end is held by at least quorum nodes.
//
// It commits that index only if the entry is from the current term. This is
// the Figure 8 rule: an old-term entry on a majority can still be overwritten
// by a future leader, because a candidate with a newer last term beats it. An
// entry from the current term cannot, and committing it commits everything
// before it through Log Matching. No search for a lower index is needed: terms
// never decrease along the log, so anything lower is not current-term either.
func (n *Node) advanceCommit() {
	match := make([]uint64, 0, len(n.peers)+1)
	match = append(match, n.log.lastIndex())
	for _, p := range n.peers {
		match = append(match, n.matchIndex[p])
	}
	slices.Sort(match)
	index := match[len(match)-n.quorum()]
	if index > n.commitIndex && n.log.term(index) == n.currentTerm {
		n.commitIndex = index
	}
}
