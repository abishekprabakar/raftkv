package raft

import (
	"errors"
	"fmt"
)

// Update is one atomic change to the durable state: optionally a new HardState,
// then a truncation, then appended entries. A term change, a vote and new log
// entries caused by one message become one Save and therefore one fsync.
type Update struct {
	HardState    *HardState // nil if unchanged
	TruncateFrom uint64     // delete this index and all later ones; 0 means none
	Entries      []Entry    // appended after the truncation
}

func (u Update) empty() bool {
	return u.HardState == nil && u.TruncateFrom == 0 && len(u.Entries) == 0
}

// Storage is the durable copy of Figure 2's persistent state.
type Storage interface {
	// Load returns everything from every Save that returned nil.
	Load() (HardState, []Entry, error)
	// Save returns only after the update is durable, and the whole update
	// survives a crash or none of it does. The safety of the protocol rests
	// on this: a node's promises are worthless if it can forget them.
	Save(Update) error
}

// applyUpdate is the meaning of an Update, shared by both implementations.
func applyUpdate(hard *HardState, entries []Entry, u Update) ([]Entry, error) {
	if u.HardState != nil {
		*hard = *u.HardState
	}
	if u.TruncateFrom != 0 {
		if u.TruncateFrom > uint64(len(entries))+1 {
			return nil, fmt.Errorf("raft: truncate from %d in log of %d entries", u.TruncateFrom, len(entries))
		}
		entries = entries[:u.TruncateFrom-1]
	}
	for _, e := range u.Entries {
		if e.Index != uint64(len(entries))+1 {
			return nil, fmt.Errorf("raft: entry %d after log of %d entries", e.Index, len(entries))
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// MemStorage keeps durable state in memory. It models a disk that never loses a
// completed Save: to simulate a crash, build a new Node from the same
// MemStorage. FailNext makes the next Save fail, as a full disk would.
type MemStorage struct {
	hard     HardState
	entries  []Entry
	FailNext error
}

// NewMemStorage returns empty storage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

// Load implements Storage.
func (s *MemStorage) Load() (HardState, []Entry, error) {
	return s.hard, append([]Entry(nil), s.entries...), nil
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
	entries, err := applyUpdate(&s.hard, s.entries, u)
	if err != nil {
		return err
	}
	s.entries = entries
	return nil
}
