package sim

import (
	"fmt"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

type link struct{ from, to raft.NodeID }

// network holds the state that outlives one message: which links are cut, and
// what has been sent and delivered on each link.
type network struct {
	cut       map[link]bool
	sent      map[link]uint64
	delivered map[link]uint64 // the highest sequence number delivered so far
}

func newNetwork() network {
	return network{cut: map[link]bool{}, sent: map[link]uint64{}, delivered: map[link]uint64{}}
}

func (n *network) heal() { clear(n.cut) }

// send queues msg for delivery, deciding its fate now. Each copy gets its own
// random delay, and that is all reordering is: a later message on the same link
// that draws a shorter delay overtakes an earlier one.
func (s *Sim) send(from, to raft.NodeID, msg raft.Message) {
	s.stats.Sent++
	l := link{from, to}
	s.links.sent[l]++
	seq := s.links.sent[l]

	copies := 1
	if s.chance(s.sched.DupRate) {
		copies = 2
		s.stats.Duplicated++
	}
	for i := 0; i < copies; i++ {
		if s.chance(s.sched.DropRate) {
			s.stats.Dropped++
			s.tracef("drop %d->%d %s", from, to, describe(msg))
			continue
		}
		delay := between(s.rnd, s.sched.MinDelay, s.sched.MaxDelay)
		if s.quiet {
			delay = between(s.rnd, s.sched.MinDelay, quietMaxDelay)
		}
		if s.chance(s.sched.LongDelayRate) {
			delay = between(s.rnd, s.sched.MaxDelay, s.sched.LongDelayMax)
		}
		if delay > int64(s.sched.ElectionTicks)*TickLength {
			s.stats.LongDelayed++
		}
		s.schedule(delay, event{kind: evDeliver, host: s.hosts[to-1], from: from, msg: msg, linkSeq: seq})
	}
}

// deliver runs when a message's delay is up. Cuts are checked here rather than
// at send time, so a partition also catches messages already on the wire.
func (s *Sim) deliver(e *event) {
	h := e.host
	l := link{e.from, h.id}
	switch {
	case s.links.cut[l]:
		s.stats.DroppedByCut++
		return
	case h.server == nil:
		s.stats.DroppedAtDown++
		return
	}
	s.stats.Delivered++
	if e.linkSeq < s.links.delivered[l] {
		s.stats.Reordered++
	}
	s.links.delivered[l] = max(s.links.delivered[l], e.linkSeq)
	s.tracef("deliver %d->%d %s", e.from, h.id, describe(e.msg))

	before := h.status.CommitIndex
	s.call(h, func() error { return h.server.Receive(e.from, e.msg) })

	// Receiver rule 5 of AppendEntries: however a message moves the commit
	// index, it can't move it past what the message itself verified.
	if m, ok := e.msg.(raft.AppendEntriesArgs); ok && h.server != nil {
		lastNew := m.PrevLogIndex + uint64(len(m.Entries))
		if allowed := max(before, min(m.LeaderCommit, lastNew)); h.status.CommitIndex > allowed {
			s.check.monitor(CheckCommitBound, h.id, "commit index reached %d, but this message allowed %d", h.status.CommitIndex, allowed)
		}
	}
}

// startPartition cuts some links. Only one partition runs at a time.
func (s *Sim) startPartition() {
	n := len(s.ids)
	s.stats.Partitions++
	cutBoth := func(a, b raft.NodeID) {
		s.links.cut[link{a, b}] = true
		s.links.cut[link{b, a}] = true
	}
	if s.chance(s.sched.OneWayRate) {
		// One-way cuts are the nasty case: a node hears others, but they can't
		// hear it, so timeouts and terms keep moving on one side only.
		s.stats.OneWayCuts++
		cuts := 1 + s.rnd.IntN(n)
		for i := 0; i < cuts; i++ {
			a, b := s.ids[s.rnd.IntN(n)], s.ids[s.rnd.IntN(n)]
			if a != b {
				s.links.cut[link{a, b}] = true
			}
		}
		s.tracef("one-way cuts: %d", cuts)
	} else {
		order := s.rnd.Perm(n)
		size := 1 + s.rnd.IntN(max(n-1, 1))
		for _, i := range order[:size] {
			for _, j := range order[size:] {
				cutBoth(s.ids[i], s.ids[j])
			}
		}
		s.tracef("partition: %d of %d nodes cut off", size, n)
	}
	s.schedule(between(s.rnd, 1, s.sched.PartitionLen), event{kind: evHeal})
}

func describe(msg raft.Message) string {
	switch m := msg.(type) {
	case raft.RequestVoteArgs:
		return fmt.Sprintf("RequestVote term %d last %d/%d", m.Term, m.LastLogIndex, m.LastLogTerm)
	case raft.RequestVoteReply:
		return fmt.Sprintf("VoteReply term %d granted %v", m.Term, m.VoteGranted)
	case raft.AppendEntriesArgs:
		return fmt.Sprintf("Append term %d prev %d/%d entries %d commit %d", m.Term, m.PrevLogIndex, m.PrevLogTerm, len(m.Entries), m.LeaderCommit)
	case raft.AppendEntriesReply:
		return fmt.Sprintf("AppendReply term %d ok %v match %d conflict %d/%d", m.Term, m.Success, m.MatchIndex, m.ConflictIndex, m.ConflictTerm)
	case raft.InstallSnapshotArgs:
		return fmt.Sprintf("InstallSnapshot term %d through %d/%d, %d bytes", m.Term, m.LastIncludedIndex, m.LastIncludedTerm, len(m.Data))
	case raft.InstallSnapshotReply:
		return fmt.Sprintf("SnapshotReply term %d match %d", m.Term, m.MatchIndex)
	}
	return fmt.Sprintf("%T", msg)
}
