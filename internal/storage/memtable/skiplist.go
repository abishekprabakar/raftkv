// Package memtable implements an in-memory sorted table backed by a
// skiplist that supports one concurrent writer and any number of readers
// without locks on the read path.
//
// Keys are internal keys (see package ikey) and are ordered with
// ikey.Compare, so all versions of a user key are adjacent, newest first.
// Nodes are never removed or mutated once linked; a reader that observes a
// node at level i therefore always finds a consistent chain below it.
package memtable

import (
	"math/rand/v2"
	"sync/atomic"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
)

const (
	maxHeight = 12
	branching = 4
)

type node struct {
	key   []byte
	value []byte
	next  []atomic.Pointer[node] // len == height of this node
}

// Table is a skiplist memtable.
type Table struct {
	head   *node
	height atomic.Int32
	size   atomic.Int64
	count  atomic.Int64
	rnd    *rand.Rand
}

// New returns an empty table.
func New() *Table {
	t := &Table{head: &node{next: make([]atomic.Pointer[node], maxHeight)}, rnd: rand.New(rand.NewPCG(0x5eed, 0xf00d))}
	t.height.Store(1)
	return t
}

// Size returns the approximate number of bytes of keys and values stored.
func (t *Table) Size() int64 { return t.size.Load() }

// Count returns the number of entries.
func (t *Table) Count() int64 { return t.count.Load() }

func (t *Table) randomHeight() int {
	h := 1
	for h < maxHeight && t.rnd.IntN(branching) == 0 {
		h++
	}
	return h
}

// findGreaterOrEqual returns the first node with key >= key, recording the
// predecessor at each level in prev when prev is non-nil.
func (t *Table) findGreaterOrEqual(key []byte, prev *[maxHeight]*node) *node {
	x := t.head
	level := int(t.height.Load()) - 1
	for {
		next := x.next[level].Load()
		if next != nil && ikey.Compare(next.key, key) < 0 {
			x = next
			continue
		}
		if prev != nil {
			prev[level] = x
		}
		if level == 0 {
			return next
		}
		level--
	}
}

// Put inserts key with value. Only one goroutine may call Put at a time;
// readers may run concurrently. Keys must be unique (each internal key
// carries a distinct sequence number).
func (t *Table) Put(key, value []byte) {
	var prev [maxHeight]*node
	t.findGreaterOrEqual(key, &prev)
	h := t.randomHeight()
	cur := int(t.height.Load())
	if h > cur {
		for i := cur; i < h; i++ {
			prev[i] = t.head
		}
		t.height.Store(int32(h))
	}
	n := &node{key: key, value: value, next: make([]atomic.Pointer[node], h)}
	for i := 0; i < h; i++ {
		n.next[i].Store(prev[i].next[i].Load())
	}
	// Publish bottom-up so a reader that finds the node at any level can
	// always descend through fully linked lower levels.
	for i := 0; i < h; i++ {
		prev[i].next[i].Store(n)
	}
	t.size.Add(int64(len(key) + len(value) + 24))
	t.count.Add(1)
}

// Seek returns the first entry whose key is >= key.
func (t *Table) Seek(key []byte) (k, v []byte, ok bool) {
	n := t.findGreaterOrEqual(key, nil)
	if n == nil {
		return nil, nil, false
	}
	return n.key, n.value, true
}

// Iterator walks a table in key order. It stays valid while the table is
// written to concurrently; newly inserted keys may or may not be observed.
type Iterator struct {
	t *Table
	n *node
}

// Iter returns an unpositioned iterator.
func (t *Table) Iter() *Iterator { return &Iterator{t: t} }

// SeekToFirst positions at the smallest key.
func (it *Iterator) SeekToFirst() { it.n = it.t.head.next[0].Load() }

// Seek positions at the first key >= key.
func (it *Iterator) Seek(key []byte) { it.n = it.t.findGreaterOrEqual(key, nil) }

// Valid reports whether the iterator points at an entry.
func (it *Iterator) Valid() bool { return it.n != nil }

// Next advances to the following entry.
func (it *Iterator) Next() { it.n = it.n.next[0].Load() }

// Key returns the current internal key.
func (it *Iterator) Key() []byte { return it.n.key }

// Value returns the current value.
func (it *Iterator) Value() []byte { return it.n.value }
