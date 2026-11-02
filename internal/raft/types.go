package raft

// NodeID identifies a server. Zero is reserved for "nobody", so Figure 2's
// null votedFor is just None.
type NodeID uint64

// None means no node: nobody voted for, or no known leader.
const None NodeID = 0

// EntryKind says what a log entry is for.
type EntryKind uint8

const (
	// EntryCommand carries data for the state machine.
	EntryCommand EntryKind = iota
	// EntryNoop is appended by each new leader. It carries no data; see
	// becomeLeader for why it exists.
	EntryNoop
)

// Entry is one log entry. Entries are immutable once created, so slices of them
// can be shared with the transport.
type Entry struct {
	Index uint64
	Term  uint64
	Kind  EntryKind
	Data  []byte
}

// HardState is the part of Figure 2's persistent state that is not the log.
type HardState struct {
	CurrentTerm uint64
	VotedFor    NodeID
}

// Snapshot marks where the log begins: everything through Index has been
// discarded and is held by the state machine instead. Raft stores only this
// marker. The state machine's own durable state is the snapshot's contents, which
// avoids writing the whole data set a second time on every compaction.
type Snapshot struct {
	Index uint64
	Term  uint64
}

// Message is one of the messages nodes exchange. The paper describes RPCs, but
// here a request and its reply are separate one-way messages, because
// a real network can lose, delay, repeat or reorder either half. Nothing in
// this package assumes a reply matches a particular request.
type Message interface {
	// term is the sender's currentTerm, which every message carries.
	term() uint64
}

// RequestVoteArgs asks for a vote (Figure 2, RequestVote RPC).
type RequestVoteArgs struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply answers RequestVoteArgs.
type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesArgs replicates entries, and is the heartbeat when Entries is
// empty (Figure 2, AppendEntries RPC).
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
	// ReadSeq is the leader's newest read sequence number. A follower echoes it,
	// which proves it still accepted this leader after the read arrived. See
	// ReadIndex.
	ReadSeq uint64
}

// AppendEntriesReply answers AppendEntriesArgs.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
	ReadSeq uint64 // echoed from the request
	// MatchIndex is set on success: the follower's log matches the leader's up
	// to here. Figure 2's leader knows this from the RPC it sent. Here the reply
	// is a separate message that can arrive late or twice, so it has to say.
	MatchIndex uint64
	// ConflictTerm and ConflictIndex are set on failure and let the leader skip
	// back a whole term at a time instead of one entry per round trip (the
	// optimization at the end of section 5.3). ConflictTerm is the follower's
	// term at PrevLogIndex, or 0 if its log is too short. ConflictIndex is the
	// first index of that term, or the follower's last index plus one.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs gives a follower the leader's whole state machine, for a
// follower that needs entries the leader has already discarded (Figure 13). It is
// sent in one message rather than in Figure 13's chunks, which keeps the follower
// simple at the cost of holding a whole snapshot in memory.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
	ReadSeq           uint64
}

// InstallSnapshotReply answers InstallSnapshotArgs.
type InstallSnapshotReply struct {
	Term       uint64
	MatchIndex uint64 // the follower's log now matches the leader's through here
	ReadSeq    uint64
}

func (m RequestVoteArgs) term() uint64      { return m.Term }
func (m RequestVoteReply) term() uint64     { return m.Term }
func (m AppendEntriesArgs) term() uint64    { return m.Term }
func (m AppendEntriesReply) term() uint64   { return m.Term }
func (m InstallSnapshotArgs) term() uint64  { return m.Term }
func (m InstallSnapshotReply) term() uint64 { return m.Term }

// Transport carries messages between nodes.
type Transport interface {
	// Send hands msg to the network and must not block. It promises nothing:
	// the message may be lost, delayed, repeated or reordered.
	Send(to NodeID, msg Message)
}

// StateMachine receives committed entries in log order.
type StateMachine interface {
	// Apply applies one committed entry.
	Apply(Entry)
	// Position returns the index and term of the last entry the state machine
	// holds. NewNode asks once at startup and continues applying from there, which
	// may be behind what was applied before a crash, if the state machine lost
	// writes it had not made durable.
	Position() (index, term uint64)
	// Snapshot returns the state machine's contents as of its Position.
	Snapshot() []byte
	// Restore replaces all contents with a snapshot, and must be durable and
	// atomic before it returns.
	Restore(index, term uint64, data []byte)
}

// ReadListener hears the outcome of each ReadIndex call.
type ReadListener interface {
	// ReadReady means the state machine may now serve this read.
	ReadReady(id uint64)
	// ReadFailed means this node could not confirm the read. Try again.
	ReadFailed(id uint64)
}
