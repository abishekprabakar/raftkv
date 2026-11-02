package raft

import "fmt"

// raftLog is the in-memory copy of the log. Storage holds the durable copy,
// which is only read at startup. All index arithmetic lives here, so M3 can
// discard old entries for snapshots without touching the protocol code.
//
// Raft numbers entries from 1. Index 0 means "before the log", and it has term
// 0, which is what makes the consistency check work for the very first entry.
type raftLog struct {
	entries []Entry // entries[i] holds index i+1
}

func (l *raftLog) lastIndex() uint64 { return uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 { return l.term(l.lastIndex()) }

// term returns the term of the entry at index, which must exist or be 0.
func (l *raftLog) term(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	return l.entry(index).Term
}

func (l *raftLog) entry(index uint64) Entry {
	if index == 0 || index > l.lastIndex() {
		panic(fmt.Sprintf("raft: index %d outside log of %d entries", index, l.lastIndex()))
	}
	return l.entries[index-1]
}

func (l *raftLog) append(entries ...Entry) {
	for _, e := range entries {
		if e.Index != l.lastIndex()+1 {
			panic(fmt.Sprintf("raft: appending index %d after %d", e.Index, l.lastIndex()))
		}
		l.entries = append(l.entries, e)
	}
}

// truncateFrom deletes index and everything after it. Capping the slice forces
// the next append to reallocate, so a slice already handed to the transport can
// never have its entries overwritten in place.
func (l *raftLog) truncateFrom(index uint64) {
	l.entries = l.entries[: index-1 : index-1]
}

// slice returns a copy of up to limit entries starting at index.
func (l *raftLog) slice(index uint64, limit int) []Entry {
	if index > l.lastIndex() {
		return nil
	}
	rest := l.entries[index-1:]
	if len(rest) > limit {
		rest = rest[:limit]
	}
	return append([]Entry(nil), rest...)
}

// firstIndexOfTerm returns the first index holding the same term as index.
func (l *raftLog) firstIndexOfTerm(index uint64) uint64 {
	term := l.term(index)
	for index > 1 && l.term(index-1) == term {
		index--
	}
	return index
}

// lastIndexOfTerm returns the last index with exactly this term, or 0 if the
// log has none. Terms never decrease along the log, so it searches backwards
// and stops as soon as it passes the term.
func (l *raftLog) lastIndexOfTerm(term uint64) uint64 {
	for i := l.lastIndex(); i > 0; i-- {
		switch t := l.term(i); {
		case t == term:
			return i
		case t < term:
			return 0
		}
	}
	return 0
}
