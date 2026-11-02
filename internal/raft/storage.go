package raft

import (
	"errors"
	"fmt"
)

// Update is one atomic change to the durable state: optionally a new HardState,
// then either a new Snapshot or a truncation, then appended entries. A term
// change, a vote and new log entries caused by one message become one Save and
// therefore one fsync.
type Update struct {
	HardState *HardState // nil if unchanged
	// Snapshot, if set, replaces the whole log: afterwards it is this snapshot
	// followed by Entries. HardState must be set as well, so that storage can
	// discard everything older and start again from this one update.
	Snapshot     *Snapshot
	TruncateFrom uint64  // delete this index and all later ones; 0 means none
	Entries      []Entry // appended after the truncation
}

func (u Update) empty() bool {
	return u.HardState == nil && u.Snapshot == nil && u.TruncateFrom == 0 && len(u.Entries) == 0
}

// Durable is everything Storage holds.
type Durable struct {
	Hard     HardState
	Snapshot Snapshot
	Entries  []Entry // the log after Snapshot
}

// apply is the meaning of an Update, shared by both implementations.
func (d *Durable) apply(u Update) error {
	if u.HardState != nil {
		d.Hard = *u.HardState
	}
	if u.Snapshot != nil {
		d.Snapshot, d.Entries = *u.Snapshot, nil
	}
	last := func() uint64 { return d.Snapshot.Index + uint64(len(d.Entries)) }
	if u.TruncateFrom != 0 {
		if u.TruncateFrom <= d.Snapshot.Index || u.TruncateFrom > last()+1 {
			return fmt.Errorf("raft: truncate from %d in log (%d, %d]", u.TruncateFrom, d.Snapshot.Index, last())
		}
		d.Entries = d.Entries[:u.TruncateFrom-d.Snapshot.Index-1]
	}
	for _, e := range u.Entries {
		if e.Index != last()+1 {
			return fmt.Errorf("raft: entry %d after log ending at %d", e.Index, last())
		}
		d.Entries = append(d.Entries, e)
	}
	return nil
}

// Storage is the durable copy of Figure 2's persistent state.
type Storage interface {
	// Load returns everything from every Save that returned nil.
	Load() (Durable, error)
	// Save returns only after the update is durable, and the whole update
	// survives a crash or none of it does. The safety of the protocol rests
	// on this: a node's promises are worthless if it can forget them.
	Save(Update) error
}

// MemStorage keeps durable state in memory. It models a disk that never loses a
// completed Save: to simulate a crash, build a new Node from the same
// MemStorage. FailNext makes the next Save fail, as a full disk would.
type MemStorage struct {
	state    Durable
	FailNext error
}

// NewMemStorage returns empty storage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// Load implements Storage.
func (s *MemStorage) Load() (Durable, error) {
	d := s.state
	d.Entries = append([]Entry(nil), s.state.Entries...)
	return d, nil
}

// Save implements Storage.
func (s *MemStorage) Save(u Update) error {
	if err := s.FailNext; err != nil {
		s.FailNext = nil
		return err
	}
	if u.empty() {
		return errors.New("raft: empty update")
	}
	return s.state.apply(u)
}
