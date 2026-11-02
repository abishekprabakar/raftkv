package engine

import (
	"encoding/binary"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
)

type batchOp struct {
	kind  ikey.Kind
	key   []byte
	value []byte
}

// Batch collects writes that must take effect together. It copies nothing: the
// caller keeps keys and values unchanged until Write returns.
type Batch struct {
	ops []batchOp
}

// Put adds a write of key.
func (b *Batch) Put(key, value []byte) {
	b.ops = append(b.ops, batchOp{ikey.KindValue, key, value})
}

// Delete adds a deletion of key.
func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, batchOp{ikey.KindDelete, key, nil})
}

// Len returns the number of ops.
func (b *Batch) Len() int { return len(b.ops) }

// encodedBytes is an upper bound on the size of the WAL record.
func (b *Batch) encodedBytes() int {
	n := recordHeader
	for _, op := range b.ops {
		n += 1 + 2*binary.MaxVarintLen32 + len(op.key) + len(op.value)
	}
	return n
}
