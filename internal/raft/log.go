package raft

import "fmt"

// raftLog is the in-memory copy of the log. Storage holds the durable copy, which
// is only read at startup. All index arithmetic lives here.
//
// Raft numbers entries from 1. Index 0 means "before the log", and it has term 0,
// which is what makes the consistency check work for the very first entry. A
// snapshot moves that starting point: entries through snapIndex are gone, but
// snapIndex keeps its term, because the entry after it has to check against it.
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry // entries[i] holds index snapIndex+i+1
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 { return l.term(l.lastIndex()) }

// term returns the term at index, which must be held or be snapIndex.
func (l *raftLog) term(index uint64) uint64 {
	if index == l.snapIndex {
		return l.snapTerm
	}
	return l.entry(index).Term
}

func (l *raftLog) entry(index uint64) Entry {
	if index <= l.snapIndex || index > l.lastIndex() {
		panic(fmt.Sprintf("raft: index %d outside log (%d, %d]", index, l.snapIndex, l.lastIndex()))
	}
	return l.entries[index-l.snapIndex-1]
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
	keep := index - l.snapIndex - 1
	l.entries = l.entries[:keep:keep]
}

// slice returns a copy of up to limit entries starting at index.
func (l *raftLog) slice(index uint64, limit int) []Entry {
	if index > l.lastIndex() {
		return nil
	}
	rest := l.entries[index-l.snapIndex-1:]
	if len(rest) > limit {
		rest = rest[:limit]
	}
	return append([]Entry(nil), rest...)
}

// tail returns a copy of every entry after the snapshot.
func (l *raftLog) tail() []Entry { return append([]Entry(nil), l.entries...) }

// compactTo discards everything through index. The entries are copied, so the
// memory of the discarded ones can be reclaimed.
func (l *raftLog) compactTo(index uint64) {
	term := l.term(index)
	l.entries = append([]Entry(nil), l.entries[index-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = index, term
}

// reset discards the whole log and starts again after a snapshot.
func (l *raftLog) reset(index, term uint64) {
	l.entries = nil
	l.snapIndex, l.snapTerm = index, term
}

// bytesAfter totals the entry data after index.
func (l *raftLog) bytesAfter(index uint64) int {
	n := 0
	for _, e := range l.entries[index-l.snapIndex:] {
		n += len(e.Data)
	}
	return n
}

// firstIndexOfTerm returns the first held index with the same term as index.
func (l *raftLog) firstIndexOfTerm(index uint64) uint64 {
	term := l.term(index)
	for index > l.snapIndex+1 && l.term(index-1) == term {
		index--
	}
	return index
}

// lastIndexOfTerm returns the last index with exactly this term, or 0 if the log
// has none. Terms never decrease along the log, so it searches backwards and
// stops as soon as it passes the term.
func (l *raftLog) lastIndexOfTerm(term uint64) uint64 {
	for i := l.lastIndex(); i > l.snapIndex; i-- {
		switch t := l.term(i); {
		case t == term:
			return i
		case t < term:
			return 0
		}
	}
	if l.snapTerm == term {
		return l.snapIndex
	}
	return 0
}
