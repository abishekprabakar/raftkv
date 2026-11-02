// Package ikey defines the internal key encoding shared by the memtable,
// SSTables and the engine.
//
// An internal key is the user key followed by an 8-byte big-endian trailer
// holding (sequence << 8 | kind). Internal keys sort by user key ascending,
// then by trailer descending, so the newest version of a key comes first.
// That ordering lets a point lookup for key K at snapshot S seek to
// Encode(K, S, 0xFF) and take the first entry whose user key equals K.
package ikey

import (
	"bytes"
	"encoding/binary"
)

// Kind is the type of an entry.
type Kind uint8

const (
	// KindDelete marks a tombstone.
	KindDelete Kind = 0
	// KindValue marks a live value.
	KindValue Kind = 1
	// kindSeek is used only in search keys so they sort before every real
	// entry carrying the same sequence number.
	kindSeek Kind = 0xFF
)

// TrailerSize is the size in bytes of the trailer appended to the user key.
const TrailerSize = 8

// MaxSeq is the largest representable sequence number (56 bits).
const MaxSeq uint64 = 1<<56 - 1

// Encode appends the internal key for (user, seq, kind) to dst.
func Encode(dst, user []byte, seq uint64, kind Kind) []byte {
	dst = append(dst, user...)
	return binary.BigEndian.AppendUint64(dst, seq<<8|uint64(kind))
}

// SearchKey returns the internal key that sorts immediately before every
// version of user with sequence <= seq.
func SearchKey(dst, user []byte, seq uint64) []byte {
	return Encode(dst, user, seq, kindSeek)
}

// UserKey returns the user key portion of an internal key.
func UserKey(ik []byte) []byte { return ik[:len(ik)-TrailerSize] }

// Split decodes an internal key.
func Split(ik []byte) (user []byte, seq uint64, kind Kind) {
	n := len(ik) - TrailerSize
	t := binary.BigEndian.Uint64(ik[n:])
	return ik[:n], t >> 8, Kind(t & 0xFF)
}

// Trailer returns the raw 8-byte trailer value.
func Trailer(ik []byte) uint64 {
	return binary.BigEndian.Uint64(ik[len(ik)-TrailerSize:])
}

// Compare orders internal keys: user key ascending, then trailer descending.
func Compare(a, b []byte) int {
	ua, ub := UserKey(a), UserKey(b)
	if c := bytes.Compare(ua, ub); c != 0 {
		return c
	}
	ta, tb := Trailer(a), Trailer(b)
	switch {
	case ta > tb:
		return -1
	case ta < tb:
		return 1
	}
	return 0
}
