package raft

// Leader election: sections 5.2 and 5.4.1 of the paper.

// startElection converts to candidate: new term, vote for self, ask everyone.
// The election timer restarts, so a split vote ends in a fresh election with a
// fresh random timeout. Nothing retransmits RequestVote: if a request is lost,
// the next timeout simply starts another election.
func (n *Node) startElection() {
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.hardDirty = true
	n.leaderID = None
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimer()
	if len(n.votes) >= n.quorum() { // a one-node cluster has already won
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		n.send(p, RequestVoteArgs{
			Term:         n.currentTerm,
			CandidateID:  n.id,
			LastLogIndex: n.log.lastIndex(),
			LastLogTerm:  n.log.lastTerm(),
		})
	}
}

// stepDown moves to a newer term as a follower. The election timer keeps
// running unless this node was leader, because Figure 2 resets it only for a
// granted vote or the current leader. If any higher-term message reset it, a
// candidate that can never win could hold off every election forever.
func (n *Node) stepDown(term uint64) {
	if n.role == Leader {
		n.resetElectionTimer()
	}
	n.role = Follower
	n.leaderID = None
	n.currentTerm = term
	n.votedFor = None
	n.hardDirty = true
}

func (n *Node) handleRequestVote(from NodeID, m RequestVoteArgs) {
	// A higher term was already handled in Receive, so m.Term is at most
	// currentTerm here. Granting again to the same candidate is harmless and
	// necessary: the first reply may have been lost.
	grant := m.Term == n.currentTerm &&
		(n.votedFor == None || n.votedFor == m.CandidateID) &&
		n.candidateLogUpToDate(m.LastLogIndex, m.LastLogTerm)
	if grant {
		if n.votedFor == None {
			n.votedFor = m.CandidateID
			n.hardDirty = true
		}
		n.resetElectionTimer()
	}
	n.send(from, RequestVoteReply{Term: n.currentTerm, VoteGranted: grant})
}

// candidateLogUpToDate is the election restriction (section 5.4.1): the later
// last term wins, and on equal terms the longer log wins. A committed entry is
// on a majority, and a winner needs a majority of votes, so at least one voter
// has that entry and will refuse any candidate without it. This is what
// guarantees every new leader already holds all committed entries.
func (n *Node) candidateLogUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != n.log.lastTerm() {
		return lastTerm > n.log.lastTerm()
	}
	return lastIndex >= n.log.lastIndex()
}

func (n *Node) handleRequestVoteReply(from NodeID, m RequestVoteReply) {
	// A reply from an earlier term says nothing about this election. votes is
	// a set, so a repeated reply cannot count twice.
	if n.role != Candidate || m.Term != n.currentTerm || !m.VoteGranted {
		return
	}
	n.votes[from] = true
	if len(n.votes) >= n.quorum() {
		n.becomeLeader()
	}
}

// becomeLeader takes over and appends a no-op entry from the new term (section
// 8). A leader may only commit entries from its own term by counting replicas
// (see advanceCommit), so without a no-op, entries inherited from earlier terms
// would stay uncommitted until the next client command, possibly forever. M3's
// reads also need the leader to have committed something in its own term.
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	next := n.log.lastIndex() + 1
	for _, p := range n.peers {
		n.nextIndex[p] = next
		n.matchIndex[p] = 0
	}
	n.appendAsLeader(EntryNoop, nil)
	n.advanceCommit()
	n.broadcastAppend()
}
