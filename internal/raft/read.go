package raft

import "slices"

// Linearizable reads with ReadIndex: section 6.4 of the Raft dissertation.
//
// A leader can't answer a read from its own state just because it believes it is
// the leader. A newer leader may have been elected behind a partition and may have
// already committed writes this node has never seen, so a plain read could return
// stale data. Appending every read to the log would solve that, at the cost of an
// fsync per read. ReadIndex proves the same thing without touching the disk:
//
//  1. Note the commit index when the read arrives. The read must reflect it.
//  2. Check this leader has committed something in its own term. Until then its
//     commit index may lag behind what an earlier leader committed.
//  3. Exchange a round of messages with a majority, sent after the read arrived.
//     Any newer leader needed votes from a majority, and those voters would no
//     longer answer this one, so a majority answering in this term proves no newer
//     leader existed when the read arrived.
//  4. Wait until the state machine reaches the index from step 1, then serve it.

// readExpiryElections is how many election timeouts a read may wait before it is
// failed. A leader cut off from a majority never confirms anything, and without
// this its reads would wait until it hears of a new term, possibly forever.
const readExpiryElections = 2

type pendingRead struct {
	id    uint64
	index uint64 // the commit index when the read arrived
	seq   uint64 // confirmed once a majority has echoed this
	tick  int    // when it arrived
}

// ReadIndex starts a linearizable read. When the state machine can serve it, the
// node calls Config.Reads.ReadReady(id) from inside one of its input methods, or
// ReadFailed(id) if it can't. An error means the read never started.
func (n *Node) ReadIndex(id uint64) error {
	if n.failed != nil {
		return n.failed
	}
	if n.role != Leader {
		return ErrNotLeader
	}
	if n.log.term(n.commitIndex) != n.currentTerm {
		return ErrNotReady
	}
	n.readSeq++
	n.reads = append(n.reads, pendingRead{id: id, index: n.commitIndex, seq: n.readSeq, tick: n.ticks})
	n.broadcastAppend()
	return n.finish()
}

// noteAck records that a peer answered this leader, in this term, after it had
// seen read sequence number seq. A rejection counts too: it was still sent by a
// node that accepts this leader.
func (n *Node) noteAck(from NodeID, seq uint64) {
	n.ackedSeq[from] = max(n.ackedSeq[from], seq)
}

// confirmedSeq is the highest read sequence number a majority has acknowledged,
// counting this leader itself.
func (n *Node) confirmedSeq() uint64 {
	acks := make([]uint64, 0, len(n.peers)+1)
	acks = append(acks, n.readSeq)
	for _, p := range n.peers {
		acks = append(acks, n.ackedSeq[p])
	}
	slices.Sort(acks)
	return acks[len(acks)-n.quorum()]
}

// deliverReads releases reads in arrival order, after the apply in finish. The
// check on lastApplied is step 4. Apply runs inside this package's calls, so here
// it always holds, but it is the rule that makes the read correct.
func (n *Node) deliverReads() {
	for _, id := range n.failedReads {
		n.cfg.Reads.ReadFailed(id)
	}
	n.failedReads = nil
	if len(n.reads) == 0 {
		return
	}
	confirmed := n.confirmedSeq()
	for len(n.reads) > 0 {
		r := n.reads[0]
		if r.seq > confirmed || r.index > n.lastApplied {
			return
		}
		n.reads = n.reads[1:]
		n.cfg.Reads.ReadReady(r.id)
	}
}

func (n *Node) failAllReads() {
	for _, r := range n.reads {
		n.failedReads = append(n.failedReads, r.id)
	}
	n.reads = nil
}

func (n *Node) expireReads() {
	limit := readExpiryElections * n.cfg.ElectionTicks
	for len(n.reads) > 0 && n.ticks-n.reads[0].tick >= limit {
		n.failedReads = append(n.failedReads, n.reads[0].id)
		n.reads = n.reads[1:]
	}
}
