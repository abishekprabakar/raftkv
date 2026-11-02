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

// Message is one of the four messages nodes exchange. Figure 2 describes two
// RPCs, but here a request and its reply are separate one-way messages, because
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
}

// AppendEntriesReply answers AppendEntriesArgs.
type AppendEntriesReply struct {
	Term    uint64
	Success bool
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

func (m RequestVoteArgs) term() uint64    { return m.Term }
func (m RequestVoteReply) term() uint64   { return m.Term }
func (m AppendEntriesArgs) term() uint64  { return m.Term }
func (m AppendEntriesReply) term() uint64 { return m.Term }

// Transport carries messages between nodes.
type Transport interface {
	// Send hands msg to the network and must not block. It promises nothing:
	// the message may be lost, delayed, repeated or reordered.
	Send(to NodeID, msg Message)
}

// StateMachine receives committed entries in log order. After a restart it
// receives them again from the start of the log, so it must be rebuilt from
// empty. Snapshots in M3 change that.
type StateMachine interface {
	Apply(Entry)
}
