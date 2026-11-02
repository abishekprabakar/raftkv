package engine

import (
	"bytes"
	"container/heap"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
	"github.com/abishekprabakar/raftkv/internal/storage/memtable"
	"github.com/abishekprabakar/raftkv/internal/storage/sstable"
)

// internalIter is the common shape of memtable, table and level iterators.
// Key and Value are valid only until the next positioning call.
type internalIter interface {
	SeekToFirst()
	Seek(key []byte)
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Err() error
}

// memIter adapts a memtable iterator.
type memIter struct{ *memtable.Iterator }

func (m memIter) Err() error { return nil }

// tableIter adapts an SSTable iterator.
type tableIter struct{ *sstable.Iterator }

func (t tableIter) SeekToFirst()    { t.Iterator.SeekToFirst() }
func (t tableIter) Seek(key []byte) { t.Iterator.Seek(key) }

// levelIter walks the non-overlapping files of one level (n >= 1) in order,
// opening one table iterator at a time.
type levelIter struct {
	files []*fileMeta
	idx   int
	cur   *sstable.Iterator
	err   error
}

func newLevelIter(files []*fileMeta) *levelIter { return &levelIter{files: files, idx: -1} }

func (l *levelIter) open(i int) bool {
	if i >= len(l.files) {
		l.cur, l.idx = nil, len(l.files)
		return false
	}
	l.idx = i
	l.cur = l.files[i].reader.Iter()
	return true
}

func (l *levelIter) SeekToFirst() {
	l.err = nil
	if !l.open(0) {
		return
	}
	l.err = l.cur.SeekToFirst()
	l.skipEmpty()
}

func (l *levelIter) Seek(key []byte) {
	l.err = nil
	// First file whose largest key is >= key.
	lo, hi := 0, len(l.files)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ikey.Compare(l.files[mid].props.Largest, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if !l.open(lo) {
		return
	}
	l.err = l.cur.Seek(key)
	l.skipEmpty()
}

// skipEmpty advances to the next file while the current one is exhausted.
func (l *levelIter) skipEmpty() {
	for l.err == nil && l.cur != nil && !l.cur.Valid() {
		if l.cur.Err() != nil {
			l.err = l.cur.Err()
			return
		}
		if !l.open(l.idx + 1) {
			return
		}
		l.err = l.cur.SeekToFirst()
	}
}

func (l *levelIter) Valid() bool { return l.err == nil && l.cur != nil && l.cur.Valid() }

func (l *levelIter) Next() {
	l.cur.Next()
	l.skipEmpty()
}

func (l *levelIter) Key() []byte   { return l.cur.Key() }
func (l *levelIter) Value() []byte { return l.cur.Value() }
func (l *levelIter) Err() error {
	if l.err != nil {
		return l.err
	}
	if l.cur != nil {
		return l.cur.Err()
	}
	return nil
}

// mergeIter performs a k-way merge of internal iterators using a min-heap
// keyed by ikey.Compare. Ties are broken by child index, so children must be
// supplied newest-first for the engine's dedup logic to see the newest
// version of a key first.
type mergeIter struct {
	children []internalIter
	h        mergeHeap
	err      error
}

type mergeHeap struct {
	items []int // indexes into children
	its   []internalIter
}

func (h mergeHeap) Len() int { return len(h.items) }
func (h mergeHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	c := ikey.Compare(h.its[a].Key(), h.its[b].Key())
	if c != 0 {
		return c < 0
	}
	return a < b
}
func (h mergeHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *mergeHeap) Push(x any)   { h.items = append(h.items, x.(int)) }
func (h *mergeHeap) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}

func newMergeIter(children []internalIter) *mergeIter {
	return &mergeIter{children: children, h: mergeHeap{its: children}}
}

func (m *mergeIter) rebuild() {
	m.h.items = m.h.items[:0]
	m.err = nil
	for i, c := range m.children {
		if c.Err() != nil {
			m.err = c.Err()
			return
		}
		if c.Valid() {
			m.h.items = append(m.h.items, i)
		}
	}
	heap.Init(&m.h)
}

func (m *mergeIter) SeekToFirst() {
	for _, c := range m.children {
		c.SeekToFirst()
	}
	m.rebuild()
}

func (m *mergeIter) Seek(key []byte) {
	for _, c := range m.children {
		c.Seek(key)
	}
	m.rebuild()
}

func (m *mergeIter) Valid() bool { return m.err == nil && len(m.h.items) > 0 }

func (m *mergeIter) Next() {
	top := m.h.items[0]
	c := m.children[top]
	c.Next()
	if c.Err() != nil {
		m.err = c.Err()
		return
	}
	if c.Valid() {
		heap.Fix(&m.h, 0)
	} else {
		heap.Pop(&m.h)
	}
}

func (m *mergeIter) Key() []byte   { return m.children[m.h.items[0]].Key() }
func (m *mergeIter) Value() []byte { return m.children[m.h.items[0]].Value() }
func (m *mergeIter) Err() error    { return m.err }

// Iterator is a user-facing range iterator. It observes a consistent
// snapshot: the durable sequence number at creation time, the memtables and
// the version that were current then.
type Iterator struct {
	db      *DB
	merge   *mergeIter
	snap    uint64
	end     []byte
	key     []byte
	value   []byte
	lastKey []byte
	valid   bool
	started bool
	ver     *version
	err     error
}

// Scan returns an iterator over user keys in [start, end). A nil start
// begins at the first key; a nil end runs to the last key. Call Next to
// position it, and Close when done.
func (db *DB) Scan(start, end []byte) *Iterator {
	snap := db.visible.Load()
	db.mu.Lock()
	mem := db.mem
	imm := db.imm
	v := db.current
	v.ref()
	db.mu.Unlock()

	children := []internalIter{memIter{mem.Iter()}}
	for i := len(imm) - 1; i >= 0; i-- {
		children = append(children, memIter{imm[i].table.Iter()})
	}
	for _, f := range v.levels[0] {
		children = append(children, tableIter{f.reader.Iter()})
	}
	for n := 1; n < len(v.levels); n++ {
		if len(v.levels[n]) > 0 {
			children = append(children, newLevelIter(v.levels[n]))
		}
	}
	it := &Iterator{db: db, merge: newMergeIter(children), snap: snap, ver: v}
	if end != nil {
		it.end = append([]byte(nil), end...)
	}
	if start == nil {
		it.merge.SeekToFirst()
	} else {
		it.merge.Seek(ikey.Encode(nil, start, ikey.MaxSeq, ikey.KindValue))
	}
	return it
}

// Next advances to the next live key. It returns false at the end or on
// error.
func (it *Iterator) Next() bool {
	if it.err != nil || it.merge == nil {
		return false
	}
	if it.started {
		it.merge.Next()
	}
	it.started = true
	for it.merge.Valid() {
		k := it.merge.Key()
		user, seq, kind := ikey.Split(k)
		switch {
		case seq > it.snap:
			// Not yet durable when the iterator was created.
		case bytes.Equal(user, it.lastKey):
			// An older version of a key already emitted or deleted.
		default:
			it.lastKey = append(it.lastKey[:0], user...)
			if it.end != nil && bytes.Compare(user, it.end) >= 0 {
				it.valid = false
				return false
			}
			if kind == ikey.KindValue {
				it.key = append(it.key[:0], user...)
				it.value = append(it.value[:0], it.merge.Value()...)
				it.valid = true
				return true
			}
		}
		it.merge.Next()
	}
	it.err = it.merge.Err()
	it.valid = false
	return false
}

// Key returns the current user key. Valid until the next call to Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value. Valid until the next call to Next.
func (it *Iterator) Value() []byte { return it.value }

// Err reports any error hit while iterating.
func (it *Iterator) Err() error { return it.err }

// Close releases the snapshot.
func (it *Iterator) Close() error {
	if it.ver != nil {
		it.ver.unref()
		it.ver = nil
	}
	it.merge = nil
	return it.err
}
