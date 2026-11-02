// Package sstable reads and writes immutable sorted string tables.
//
// File layout (all integers little-endian unless noted):
//
//	+--------------------+
//	| data block 0       |  entries, then crc32c u32
//	| data block 1       |
//	| ...                |
//	| index block        |  one entry per data block, then crc32c
//	| bloom block        |  bloom filter bytes, then crc32c
//	| properties block   |  file statistics, then crc32c
//	| footer (64 bytes)  |
//	+--------------------+
//
// Data block entry: uvarint(keylen) uvarint(valuelen) key value, where key
// is an internal key. Blocks are closed once they exceed the target block
// size. Index entry: uvarint(keylen) key uvarint(offset) uvarint(size) where
// key is the last internal key of the block, so a point lookup binary
// searches the index for the first block whose last key is >= the search
// key and reads exactly one block. The bloom filter is built over user keys.
//
// Footer: index off u64, index len u64, bloom off u64, bloom len u64,
// props off u64, props len u64, reserved u64, magic u64.
package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	footerSize = 64
	magic      = 0x7261667472616b76 // "raftrakv"
	crcSize    = 4
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	// ErrCorrupt indicates a checksum or structural failure.
	ErrCorrupt = errors.New("sstable: corrupt")
)

type footer struct {
	indexOff, indexLen uint64
	bloomOff, bloomLen uint64
	propsOff, propsLen uint64
}

func (f footer) encode() []byte {
	b := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(b[0:], f.indexOff)
	binary.LittleEndian.PutUint64(b[8:], f.indexLen)
	binary.LittleEndian.PutUint64(b[16:], f.bloomOff)
	binary.LittleEndian.PutUint64(b[24:], f.bloomLen)
	binary.LittleEndian.PutUint64(b[32:], f.propsOff)
	binary.LittleEndian.PutUint64(b[40:], f.propsLen)
	binary.LittleEndian.PutUint64(b[56:], magic)
	return b
}

func decodeFooter(b []byte) (footer, error) {
	if len(b) != footerSize || binary.LittleEndian.Uint64(b[56:]) != magic {
		return footer{}, errors.New("sstable: bad footer")
	}
	return footer{
		indexOff: binary.LittleEndian.Uint64(b[0:]),
		indexLen: binary.LittleEndian.Uint64(b[8:]),
		bloomOff: binary.LittleEndian.Uint64(b[16:]),
		bloomLen: binary.LittleEndian.Uint64(b[24:]),
		propsOff: binary.LittleEndian.Uint64(b[32:]),
		propsLen: binary.LittleEndian.Uint64(b[40:]),
	}, nil
}

// Properties describe the contents of a table and are also stored in the
// engine manifest so the engine can plan compactions without opening files.
type Properties struct {
	NumEntries uint64
	MinSeq     uint64
	MaxSeq     uint64
	Smallest   []byte // smallest internal key
	Largest    []byte // largest internal key
	DataBytes  uint64
	NumBlocks  uint64
}

func (p Properties) encode() []byte {
	b := binary.AppendUvarint(nil, p.NumEntries)
	b = binary.AppendUvarint(b, p.MinSeq)
	b = binary.AppendUvarint(b, p.MaxSeq)
	b = binary.AppendUvarint(b, p.DataBytes)
	b = binary.AppendUvarint(b, p.NumBlocks)
	b = appendBytes(b, p.Smallest)
	b = appendBytes(b, p.Largest)
	return b
}

func decodeProperties(b []byte) (Properties, error) {
	var p Properties
	var err error
	rd := &decoder{b: b}
	p.NumEntries = rd.uvarint()
	p.MinSeq = rd.uvarint()
	p.MaxSeq = rd.uvarint()
	p.DataBytes = rd.uvarint()
	p.NumBlocks = rd.uvarint()
	p.Smallest = rd.bytes()
	p.Largest = rd.bytes()
	if rd.err != nil {
		err = ErrCorrupt
	}
	return p, err
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = ErrCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil || uint64(len(d.b)) < n {
		d.err = ErrCorrupt
		return nil
	}
	v := d.b[:n]
	d.b = d.b[n:]
	return v
}

// blockWithCRC appends a crc32c trailer to data.
func blockWithCRC(data []byte) []byte {
	return binary.LittleEndian.AppendUint32(data, crc32.Checksum(data, castagnoli))
}

// checkBlock verifies and strips the crc32c trailer.
func checkBlock(b []byte) ([]byte, error) {
	if len(b) < crcSize {
		return nil, ErrCorrupt
	}
	data, sum := b[:len(b)-crcSize], binary.LittleEndian.Uint32(b[len(b)-crcSize:])
	if crc32.Checksum(data, castagnoli) != sum {
		return nil, ErrCorrupt
	}
	return data, nil
}
