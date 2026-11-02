// Package raft implements the Raft consensus algorithm, following Figure 2 of
// "In Search of an Understandable Consensus Algorithm" by Ongaro and Ousterhout.
//
// A Node is a plain state machine with no goroutines, clock or network of its
// own. Time arrives as calls to Tick, messages as calls to Receive and client
// commands as calls to Propose, one at a time. This is what makes a whole
// cluster deterministic in simulation: given the same seed and the same order
// of calls, nodes do exactly the same thing, so any failure can be replayed.
//
// Every call ends in finish, the only place messages leave a node. It saves
// first and sends second, so "persist before replying" holds for every message
// by construction rather than by remembering it in each handler.
//
// Not implemented yet: snapshots and log compaction (M3), and membership
// changes. Pre-vote is not in Figure 2 and is deliberately left out, so a
// partitioned node returns with a high term and forces an election.
package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

// Role is a node's position in the protocol.
type Role int

const (
	// Follower answers requests and waits for a leader.
	Follower Role = iota
	// Candidate is trying to become leader.
	Candidate
	// Leader handles commands and replicates the log.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

var (
	// ErrNotLeader is returned by Propose on a node that is not the leader.
	// Status().Leader names the leader when this node knows it.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrEntryTooLarge is returned by Propose for data over Config.MaxEntryBytes.
	ErrEntryTooLarge = errors.New("raft: entry too large")
)

// Config configures a Node. Zero values select the defaults listed.
type Config struct {
	ID NodeID
	// Peers lists every member of the cluster, including ID.
	Peers []NodeID
	// ElectionTicks is the shortest election timeout. Each timeout is drawn at
	// random from [ElectionTicks, 2*ElectionTicks), so two nodes rarely time out
	// together and split the vote. Default 10.
	ElectionTicks int
	// HeartbeatTicks is how often a leader sends AppendEntries with nothing else
	// to send. It must be well under ElectionTicks, or a healthy leader would be
	// deposed by its own followers' timers. Default 2.
	HeartbeatTicks int
	// MaxEntriesPerMessage bounds one AppendEntries. Default 64.
	MaxEntriesPerMessage int
	// MaxEntryBytes bounds Propose. Together with MaxEntriesPerMessage it keeps
	// one Save well under the WAL's record limit, because a Save that failed
	// for that reason would stop the node. Default 256 KiB.
	MaxEntryBytes int
	// Seed drives the election timeouts. Combined with ID, so nodes differ.
	Seed uint64

	Storage      Storage
	Transport    Transport
	StateMachine StateMachine
}

func (c Config) withDefaults() Config {
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&c.ElectionTicks, 10)
	def(&c.HeartbeatTicks, 2)
	def(&c.MaxEntriesPerMessage, 64)
	def(&c.MaxEntryBytes, 256<<10)
	return c
}

type outgoing struct {
	to  NodeID
	msg Message
}

// Node is one Raft server. It is not safe for concurrent use: the caller makes
// one call at a time, which is the point of the design. See the package comment.
type Node struct {
	cfg   Config
	id    NodeID
	peers []NodeID // every member except this node, in a fixed order
	rnd   *rand.Rand

	// Persistent state on all servers (Figure 2).
	currentTerm uint64
	votedFor    NodeID
	log         raftLog

	// Volatile state on all servers (Figure 2).
	commitIndex uint64
	lastApplied uint64

	// Volatile state on leaders, reinitialized after election (Figure 2).
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	role             Role
	leaderID         NodeID
	votes            map[NodeID]bool // who granted this candidate a vote
	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int

	// Work collected during the current call for finish to carry out.
	hardDirty    bool
	truncateFrom uint64
	unsaved      []Entry
	outbox       []outgoing

	failed error // set once Storage fails; the node then refuses all input
}

// NewNode restores a node from cfg.Storage. It always starts as a follower with
// commitIndex 0: both are volatile, and the leader teaches it the commit index.
func NewNode(cfg Config) (*Node, error) {
	cfg = cfg.withDefaults()
	hard, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:         cfg,
		id:          cfg.ID,
		rnd:         rand.New(rand.NewPCG(cfg.Seed, uint64(cfg.ID))),
		currentTerm: hard.CurrentTerm,
		votedFor:    hard.VotedFor,
		nextIndex:   map[NodeID]uint64{},
		matchIndex:  map[NodeID]uint64{},
	}
	found := false
	for _, p := range cfg.Peers {
		if p == cfg.ID {
			found = true
		} else {
			n.peers = append(n.peers, p)
		}
	}
	if !found {
		return nil, fmt.Errorf("raft: node %d is not in Peers %v", cfg.ID, cfg.Peers)
	}
	n.log.append(entries...)
	n.resetElectionTimer()
	return n, nil
}

// quorum is the smallest majority of the cluster.
func (n *Node) quorum() int { return (len(n.peers)+1)/2 + 1 }

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTicks + n.rnd.IntN(n.cfg.ElectionTicks)
}

// ---- inputs ----------------------------------------------------------------

// Tick advances the node's logical clock by one tick.
func (n *Node) Tick() error {
	if n.failed != nil {
		return n.failed
	}
	if n.role == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTicks {
			n.broadcastAppend()
		}
	} else {
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.startElection()
		}
	}
	return n.finish()
}

// Receive handles a message from another node.
func (n *Node) Receive(from NodeID, msg Message) error {
	if n.failed != nil {
		return n.failed
	}
	// Rules for all servers: a higher term means this node is out of date,
	// whatever the message is and whatever role the node holds.
	if msg.term() > n.currentTerm {
		n.stepDown(msg.term())
	}
	switch m := msg.(type) {
	case RequestVoteArgs:
		n.handleRequestVote(from, m)
	case RequestVoteReply:
		n.handleRequestVoteReply(from, m)
	case AppendEntriesArgs:
		n.handleAppendEntries(from, m)
	case AppendEntriesReply:
		n.handleAppendEntriesReply(from, m)
	default:
		panic(fmt.Sprintf("raft: unknown message %T", msg))
	}
	return n.finish()
}

// Propose appends a command to the leader's log and starts replicating it. It
// returns the entry's index and term. Success does not mean the command is
// committed: a leader can lose its position before that. The caller learns the
// outcome from what the state machine applies at that index.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if n.failed != nil {
		return 0, 0, n.failed
	}
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if len(data) > n.cfg.MaxEntryBytes {
		return 0, 0, ErrEntryTooLarge
	}
	e := n.appendAsLeader(EntryCommand, data)
	n.advanceCommit() // in a one-node cluster this entry is already a majority
	n.broadcastAppend()
	if err := n.finish(); err != nil {
		return 0, 0, err
	}
	return e.Index, e.Term, nil
}

// finish ends every input. Its order is the core safety rule of this package.
//
// Save first: a reply is a promise ("I voted for you", "I stored this entry"),
// and a promise made before the state is durable can be forgotten in a crash.
// The node could then vote twice in a term, or a leader could count an entry
// toward a majority that has already vanished.
//
// Apply second: the commit index may have advanced during this input, covering
// entries saved just now. Applying an entry this node could still lose would
// let the state machine run ahead of what the node can recover.
//
// Send last, so nothing leaves the node unless everything above succeeded. If
// Save fails the node's memory is ahead of its disk and nothing it says can be
// trusted, so it stops permanently.
func (n *Node) finish() error {
	u := Update{TruncateFrom: n.truncateFrom, Entries: n.unsaved}
	if n.hardDirty {
		u.HardState = &HardState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor}
	}
	if !u.empty() {
		if err := n.cfg.Storage.Save(u); err != nil {
			n.failed = fmt.Errorf("raft: node %d stopped, save failed: %w", n.id, err)
			n.outbox = nil
			return n.failed
		}
	}
	n.hardDirty, n.truncateFrom, n.unsaved = false, 0, nil

	n.applyCommitted()

	for _, o := range n.outbox {
		n.cfg.Transport.Send(o.to, o.msg)
	}
	n.outbox = nil
	return nil
}

func (n *Node) send(to NodeID, msg Message) {
	n.outbox = append(n.outbox, outgoing{to, msg})
}

func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		n.cfg.StateMachine.Apply(n.log.entry(n.lastApplied))
	}
}

// Status is a snapshot of a node's state.
type Status struct {
	ID           NodeID
	Role         Role
	Term         uint64
	VotedFor     NodeID
	Leader       NodeID
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

// Status returns the node's current state.
func (n *Node) Status() Status {
	return Status{
		ID: n.id, Role: n.role, Term: n.currentTerm, VotedFor: n.votedFor, Leader: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastLogIndex: n.log.lastIndex(), LastLogTerm: n.log.lastTerm(),
	}
}
