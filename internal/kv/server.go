package kv

import (
	"errors"
	"fmt"
	"slices"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

var (
	// ErrNotLeader means this node can't serve the request. Server.Leader names
	// the leader when it is known.
	ErrNotLeader = errors.New("kv: not the leader")
	// ErrUnknown means this node lost track of a write, which may or may not have
	// committed. The client should retry it under the same Seq, and the session
	// makes that safe.
	ErrUnknown = errors.New("kv: outcome unknown, retry")
	// ErrRetry means a read could not be confirmed. Nothing was read.
	ErrRetry = errors.New("kv: read not confirmed, retry")
)

// Hooks let a test watch what a server does. Any of them may be nil.
type Hooks struct {
	Applied   func(e raft.Entry, duplicate bool)
	Restored  func(index uint64)
	Compacted func(index uint64)
}

// Config configures a Server.
type Config struct {
	Raft    raft.Config // the fields for state machine and reads are filled in here
	Backend Backend
	// SnapshotEvery compacts the log each time this many entries have been applied
	// since the last compaction. Zero means never.
	SnapshotEvery uint64
	// TrailingEntries is how many applied entries to keep after a compaction, so
	// that a follower only slightly behind doesn't need a whole snapshot.
	TrailingEntries uint64
	Hooks           Hooks
}

type waiter struct {
	client, seq uint64
	done        func(error)
}

type pendingRead struct {
	key  []byte
	done func(value []byte, found bool, err error)
	// confirmOnly marks a Confirm: the same ReadIndex round, and no key to read.
	confirmOnly bool
}

// Server joins a Raft node to a Store. Like Node, it expects one call at a time.
// Callbacks run inside those calls.
type Server struct {
	cfg      Config
	node     *raft.Node
	store    *Store
	waiters  map[uint64]waiter // by log index
	reads    map[uint64]pendingRead
	nextRead uint64
}

// NewServer opens a store on cfg.Backend and starts a Raft node on it.
func NewServer(cfg Config) (*Server, error) {
	store, err := OpenStore(cfg.Backend)
	if err != nil {
		return nil, err
	}
	s := &Server{cfg: cfg, store: store, waiters: map[uint64]waiter{}, reads: map[uint64]pendingRead{}}
	rc := cfg.Raft
	rc.StateMachine, rc.Reads = s, s
	if s.node, err = raft.NewNode(rc); err != nil {
		return nil, err
	}
	return s, nil
}

// Status returns the Raft node's state.
func (s *Server) Status() raft.Status { return s.node.Status() }

// Store returns the state machine.
func (s *Server) Store() *Store { return s.store }

// Leader returns the node this server believes leads, or raft.None.
func (s *Server) Leader() raft.NodeID { return s.node.Status().Leader }

// Tick advances the clock and does periodic work.
func (s *Server) Tick() error {
	if err := s.node.Tick(); err != nil {
		return err
	}
	if s.node.Status().Role != raft.Leader {
		s.failWaiters()
	}
	return s.maybeCompact()
}

// Receive handles a message from another node.
func (s *Server) Receive(from raft.NodeID, msg raft.Message) error {
	return s.node.Receive(from, msg)
}

// Submit proposes a write. done runs with nil once it is applied, or with an error
// saying what to do: ErrNotLeader, ErrUnknown or raft.ErrEntryTooLarge. The returned
// error means the node itself has failed.
func (s *Server) Submit(cmd Command, done func(error)) error {
	index, _, err := s.node.Propose(encodeCommand(cmd))
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		done(ErrNotLeader)
		return nil
	case errors.Is(err, raft.ErrEntryTooLarge):
		done(err)
		return nil
	case err != nil:
		return err
	}
	if w, ok := s.waiters[index]; ok {
		w.done(ErrUnknown) // an entry from a lost leadership at this same index
	}
	s.waiters[index] = waiter{cmd.ClientID, cmd.Seq, done}
	// A one-node cluster has already applied the entry inside Propose, before
	// there was a waiter to tell.
	if st := s.node.Status(); st.LastApplied >= index {
		delete(s.waiters, index)
		done(nil)
	}
	return nil
}

// Read starts a linearizable read of key. done runs when the read is confirmed or
// has failed. The returned error means the node itself has failed.
func (s *Server) Read(key []byte, done func(value []byte, found bool, err error)) error {
	return s.read(pendingRead{key: key, done: done})
}

// Confirm asks the question a read asks, without reading anything: is this node
// the leader, and does a majority still accept it right now? done runs with nil
// once the ReadIndex round completes, and with ErrNotLeader or ErrRetry if it
// can't. A health check uses it to tell a working leader from one that only
// believes it leads. The returned error means the node itself has failed.
func (s *Server) Confirm(done func(error)) error {
	return s.read(pendingRead{confirmOnly: true, done: func(_ []byte, _ bool, err error) { done(err) }})
}

func (s *Server) read(r pendingRead) error {
	s.nextRead++
	id := s.nextRead
	s.reads[id] = r
	done := r.done
	err := s.node.ReadIndex(id)
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		delete(s.reads, id)
		done(nil, false, ErrNotLeader)
		return nil
	case errors.Is(err, raft.ErrNotReady):
		delete(s.reads, id)
		done(nil, false, ErrRetry)
		return nil
	}
	return err
}

// ReadReady implements raft.ReadListener: leadership is confirmed and the store
// has applied everything the read must see.
func (s *Server) ReadReady(id uint64) {
	r := s.reads[id]
	delete(s.reads, id)
	if r.confirmOnly {
		r.done(nil, false, nil)
		return
	}
	value, found, err := s.store.Get(r.key)
	if err != nil {
		panic(fmt.Sprintf("kv: reading %q: %v", r.key, err))
	}
	r.done(value, found, nil)
}

// ReadFailed implements raft.ReadListener.
func (s *Server) ReadFailed(id uint64) {
	r := s.reads[id]
	delete(s.reads, id)
	r.done(nil, false, ErrRetry)
}

// Apply implements raft.StateMachine.
func (s *Server) Apply(e raft.Entry) {
	cmd, duplicate := s.store.Apply(e)
	if h := s.cfg.Hooks.Applied; h != nil {
		h(e, duplicate)
	}
	w, ok := s.waiters[e.Index]
	if !ok {
		return
	}
	delete(s.waiters, e.Index)
	// A different entry at this index means this write lost its place to a newer
	// leader. It might still commit later from another node's log, so its outcome
	// is unknown, not failed.
	if e.Kind == raft.EntryCommand && cmd.ClientID == w.client && cmd.Seq == w.seq {
		w.done(nil)
	} else {
		w.done(ErrUnknown)
	}
}

// Position implements raft.StateMachine.
func (s *Server) Position() (index, term uint64) { return s.store.Position() }

// Snapshot implements raft.StateMachine.
func (s *Server) Snapshot() []byte { return s.store.Snapshot() }

// Restore implements raft.StateMachine.
func (s *Server) Restore(index, term uint64, data []byte) {
	s.store.Restore(index, term, data)
	s.failWaiters()
	if h := s.cfg.Hooks.Restored; h != nil {
		h(index)
	}
}

// failWaiters tells every waiting writer the outcome is unknown. Indexes are
// visited in order, so callbacks run in the same order on every run.
func (s *Server) failWaiters() {
	if len(s.waiters) == 0 {
		return
	}
	indexes := make([]uint64, 0, len(s.waiters))
	for i := range s.waiters {
		indexes = append(indexes, i)
	}
	slices.Sort(indexes)
	for _, i := range indexes {
		w := s.waiters[i]
		delete(s.waiters, i)
		w.done(ErrUnknown)
	}
}

// maybeCompact cuts the log once enough has been applied. The flush comes first:
// after the cut, the log can no longer rebuild what the store might lose.
func (s *Server) maybeCompact() error {
	every := s.cfg.SnapshotEvery
	st := s.node.Status()
	if every == 0 || st.LastApplied < st.SnapshotIndex+every+s.cfg.TrailingEntries {
		return nil
	}
	index := st.LastApplied - s.cfg.TrailingEntries
	if err := s.store.Flush(); err != nil {
		return err
	}
	if err := s.node.CompactLog(index); err != nil {
		return err
	}
	if h := s.cfg.Hooks.Compacted; h != nil {
		h(index)
	}
	return nil
}
