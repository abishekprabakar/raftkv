package raft

import "fmt"

// Log compaction and InstallSnapshot: section 7 and Figure 13 of the paper.
//
// The state machine is durable in its own right, so its contents are the
// snapshot. Raft keeps only a marker of where the log now starts, and asks the
// state machine for the actual bytes when a lagging follower needs them.

// maxRetainedBytes bounds the entries one snapshot record may carry. It sits well
// under the WAL's record limit, because a Save that failed for that reason would
// stop the node.
const maxRetainedBytes = 32 << 20

// CompactLog discards the log through index, which must be applied. The caller
// must already have made the state machine durable through index: after this the
// log can no longer rebuild it. If the entries kept after index would not fit in
// one record, compaction waits for a later call, by which time they will have
// been applied and can go too.
func (n *Node) CompactLog(index uint64) error {
	if n.failed != nil {
		return n.failed
	}
	if index <= n.log.snapIndex {
		return nil
	}
	if index > n.lastApplied {
		return fmt.Errorf("raft: compacting through %d, but only %d is applied", index, n.lastApplied)
	}
	if n.log.bytesAfter(index) > maxRetainedBytes {
		return nil
	}
	n.log.compactTo(index)
	n.snapshotDirty = true
	return n.finish()
}

// adoptStateMachinePosition runs at startup. The state machine only ever holds
// committed entries, so where it stands is safe to believe. Being behind the log
// is normal: the missing entries get applied again. Being behind the snapshot is
// not, because those entries are gone. Being ahead of the log, or disagreeing with
// it, means a snapshot was restored and the crash came before Save, in which case
// the node finishes the job now, as Figure 13 step 7 would have.
func (n *Node) adoptStateMachinePosition() error {
	index, term := n.cfg.StateMachine.Position()
	if index < n.log.snapIndex {
		return fmt.Errorf("raft: state machine at %d is behind the snapshot at %d", index, n.log.snapIndex)
	}
	n.lastApplied, n.commitIndex = index, index
	if index > n.log.lastIndex() || n.log.term(index) != term {
		n.log.reset(index, term)
		n.snapshotDirty = true
		return n.finish()
	}
	return nil
}

// sendSnapshot gives a peer the current state, because the log entries it needs
// are gone. Building one is expensive, so a peer gets at most one per election
// timeout. In between it gets an empty AppendEntries, which it will reject, but
// which keeps its election timer from expiring while the snapshot is on its way.
func (n *Node) sendSnapshot(to NodeID) {
	if sent, ok := n.snapshotSent[to]; ok && n.ticks-sent < n.cfg.ElectionTicks {
		n.send(to, AppendEntriesArgs{
			Term:         n.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: n.log.snapIndex,
			PrevLogTerm:  n.log.snapTerm,
			LeaderCommit: n.commitIndex,
			ReadSeq:      n.readSeq,
		})
		return
	}
	n.snapshotSent[to] = n.ticks
	// Position, not commitIndex: this call may have advanced the commit index
	// past what the state machine has applied so far.
	index := n.lastApplied
	n.send(to, InstallSnapshotArgs{
		Term:              n.currentTerm,
		LeaderID:          n.id,
		LastIncludedIndex: index,
		LastIncludedTerm:  n.log.term(index),
		Data:              n.cfg.StateMachine.Snapshot(),
		ReadSeq:           n.readSeq,
	})
}

func (n *Node) handleInstallSnapshot(from NodeID, m InstallSnapshotArgs) {
	if m.Term < n.currentTerm {
		n.send(from, InstallSnapshotReply{Term: n.currentTerm})
		return
	}
	n.followLeader(m.LeaderID)

	// A delayed or repeated snapshot must not move the state machine backwards.
	// Everything through commitIndex already matches the leader, so this node can
	// say so without doing anything.
	if m.LastIncludedIndex <= n.commitIndex {
		n.send(from, InstallSnapshotReply{Term: n.currentTerm, MatchIndex: m.LastIncludedIndex, ReadSeq: m.ReadSeq})
		return
	}

	// Restore before saving. If the crash comes between them, the state machine
	// holds committed data that the log has not caught up with, and
	// adoptStateMachinePosition recovers. The other order could leave a log that
	// says the entries are gone, with a state machine that never received them.
	n.cfg.StateMachine.Restore(m.LastIncludedIndex, m.LastIncludedTerm, m.Data)
	n.lastApplied, n.commitIndex = m.LastIncludedIndex, m.LastIncludedIndex

	// Figure 13 step 6: if the log already holds the last included entry, what
	// follows it is still good and nothing on disk has to change. Step 7: otherwise
	// the log disagrees with committed data, so all of it goes.
	if m.LastIncludedIndex > n.log.lastIndex() || n.log.term(m.LastIncludedIndex) != m.LastIncludedTerm {
		n.log.reset(m.LastIncludedIndex, m.LastIncludedTerm)
		n.snapshotDirty = true
	}
	n.send(from, InstallSnapshotReply{Term: n.currentTerm, MatchIndex: m.LastIncludedIndex, ReadSeq: m.ReadSeq})
}

func (n *Node) handleInstallSnapshotReply(from NodeID, m InstallSnapshotReply) {
	if n.role != Leader || m.Term != n.currentTerm {
		return
	}
	n.noteAck(from, m.ReadSeq)
	if m.MatchIndex <= n.matchIndex[from] {
		return
	}
	n.matchIndex[from] = m.MatchIndex
	n.advanceCommit()
	n.nextIndex[from] = max(n.nextIndex[from], m.MatchIndex+1)
	delete(n.snapshotSent, from)
	if n.nextIndex[from] <= n.log.lastIndex() {
		n.sendAppend(from)
	}
}
