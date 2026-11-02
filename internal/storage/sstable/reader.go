package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
)

// Stats counts physical work done by readers that share it.
type Stats struct {
	BlockReads     atomic.Int64 // pread calls for data blocks
	BloomChecks    atomic.Int64
	BloomNegatives atomic.Int64 // lookups the filter ruled out
}

type indexEntry struct {
	lastKey []byte
	off     uint64
	size    uint64
}

// Reader serves point lookups and scans over one table.
type Reader struct {
	f     *os.File
	size  int64
	index []indexEntry
	bloom []byte
	props Properties
	stats *Stats
}

// Open reads the footer, index, bloom filter and properties into memory.
// Data blocks are read on demand with pread.
func Open(path string, stats *Stats) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r := &Reader{f: f, stats: stats}
	if err := r.init(); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable %s: %w", path, err)
	}
	return r, nil
}

func (r *Reader) init() error {
	info, err := r.f.Stat()
	if err != nil {
		return err
	}
	r.size = info.Size()
	if r.size < footerSize {
		return ErrCorrupt
	}
	fb := make([]byte, footerSize)
	if _, err := r.f.ReadAt(fb, r.size-footerSize); err != nil {
		return err
	}
	ft, err := decodeFooter(fb)
	if err != nil {
		return err
	}
	idx, err := r.readBlock(ft.indexOff, ft.indexLen)
	if err != nil {
		return err
	}
	d := &decoder{b: idx}
	for len(d.b) > 0 && d.err == nil {
		k := d.bytes()
		off := d.uvarint()
		size := d.uvarint()
		r.index = append(r.index, indexEntry{lastKey: k, off: off, size: size})
	}
	if d.err != nil {
		return d.err
	}
	if ft.bloomLen > 0 {
		if r.bloom, err = r.readBlock(ft.bloomOff, ft.bloomLen); err != nil {
			return err
		}
	}
	pb, err := r.readBlock(ft.propsOff, ft.propsLen)
	if err != nil {
		return err
	}
	r.props, err = decodeProperties(pb)
	return err
}

// readBlock preads and checksums a block. It does not count toward
// Stats.BlockReads; callers that read data blocks do that.
func (r *Reader) readBlock(off, n uint64) ([]byte, error) {
	if off+n > uint64(r.size) {
		return nil, ErrCorrupt
	}
	buf := make([]byte, n)
	if _, err := r.f.ReadAt(buf, int64(off)); err != nil {
		return nil, err
	}
	return checkBlock(buf)
}

// Properties returns the table properties.
func (r *Reader) Properties() Properties { return r.props }

// Close releases the file descriptor.
func (r *Reader) Close() error { return r.f.Close() }

// MayContain consults the bloom filter for a user key. Tables written
// without a filter always return true.
func (r *Reader) MayContain(userKey []byte) bool {
	if r.bloom == nil {
		return true
	}
	r.stats.BloomChecks.Add(1)
	if BloomMayContain(r.bloom, userKey) {
		return true
	}
	r.stats.BloomNegatives.Add(1)
	return false
}

// blockIndexFor returns the index of the first block whose last key is >=
// key, or len(index) if none.
func (r *Reader) blockIndexFor(key []byte) int {
	lo, hi := 0, len(r.index)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if ikey.Compare(r.index[mid].lastKey, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// Get returns the first entry with internal key >= key, if it exists in
// this table. Exactly one data block is read on the happy path.
func (r *Reader) Get(key []byte) (k, v []byte, found bool, err error) {
	it := r.Iter()
	if err := it.Seek(key); err != nil {
		return nil, nil, false, err
	}
	if !it.Valid() {
		return nil, nil, false, nil
	}
	return it.Key(), it.Value(), true, nil
}

// Iterator walks a table block by block.
type Iterator struct {
	r     *Reader
	bi    int    // current block index
	block []byte // decoded block (without crc)
	pos   int    // offset of the current entry within block
	key   []byte
	value []byte
	valid bool
	err   error
}

// Iter returns an unpositioned iterator.
func (r *Reader) Iter() *Iterator { return &Iterator{r: r, bi: -1} }

func (it *Iterator) loadBlock(bi int) error {
	e := it.r.index[bi]
	it.r.stats.BlockReads.Add(1)
	b, err := it.r.readBlock(e.off, e.size)
	if err != nil {
		return err
	}
	it.block, it.bi, it.pos = b, bi, 0
	return nil
}

// decodeAt decodes the entry at pos into key/value and advances pos.
func (it *Iterator) decodeNext() bool {
	if it.pos >= len(it.block) {
		return false
	}
	b := it.block[it.pos:]
	kl, n1 := binary.Uvarint(b)
	vl, n2 := binary.Uvarint(b[n1:])
	if n1 <= 0 || n2 <= 0 || uint64(len(b)) < uint64(n1+n2)+kl+vl {
		it.err = ErrCorrupt
		return false
	}
	start := n1 + n2
	it.key = b[start : start+int(kl)]
	it.value = b[start+int(kl) : start+int(kl)+int(vl)]
	it.pos += start + int(kl) + int(vl)
	return true
}

// SeekToFirst positions at the first entry.
func (it *Iterator) SeekToFirst() error {
	it.valid, it.err = false, nil
	if len(it.r.index) == 0 {
		return nil
	}
	if err := it.loadBlock(0); err != nil {
		it.err = err
		return err
	}
	it.valid = it.decodeNext()
	return it.err
}

// Seek positions at the first entry with key >= target.
func (it *Iterator) Seek(target []byte) error {
	it.valid, it.err = false, nil
	bi := it.r.blockIndexFor(target)
	if bi >= len(it.r.index) {
		return nil
	}
	if bi != it.bi || it.block == nil {
		if err := it.loadBlock(bi); err != nil {
			it.err = err
			return err
		}
	} else {
		it.pos = 0
	}
	for it.decodeNext() {
		if ikey.Compare(it.key, target) >= 0 {
			it.valid = true
			return nil
		}
	}
	// The block's last key is >= target, so we cannot run off its end
	// unless the index is inconsistent with the data.
	if it.err == nil {
		it.err = ErrCorrupt
	}
	return it.err
}

// Next advances to the following entry, crossing block boundaries.
func (it *Iterator) Next() {
	if !it.valid {
		return
	}
	if it.decodeNext() {
		return
	}
	if it.err != nil || it.bi+1 >= len(it.r.index) {
		it.valid = false
		return
	}
	if err := it.loadBlock(it.bi + 1); err != nil {
		it.err, it.valid = err, false
		return
	}
	it.valid = it.decodeNext()
}

// Valid reports whether the iterator is positioned at an entry.
func (it *Iterator) Valid() bool { return it.valid && it.err == nil }

// Key returns the current internal key. It is valid until the next call.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value. It is valid until the next call.
func (it *Iterator) Value() []byte { return it.value }

// Err returns any error encountered.
func (it *Iterator) Err() error { return it.err }

// Overlaps reports whether the table's user-key range intersects
// [smallest, largest] (inclusive; nil largest means unbounded).
func Overlaps(p Properties, smallest, largest []byte) bool {
	if smallest != nil && bytes.Compare(ikey.UserKey(p.Largest), smallest) < 0 {
		return false
	}
	if largest != nil && bytes.Compare(ikey.UserKey(p.Smallest), largest) > 0 {
		return false
	}
	return true
}
