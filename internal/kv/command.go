// Package kv is a replicated key-value store. Clients send commands to the Raft
// leader, committed commands are applied to a storage backend, and reads go
// through ReadIndex.
//
// Three things matter here beyond the plain map:
//
//   - Exactly-once writes. A client retries when it gets no answer, but the first
//     try may have succeeded. Each client numbers its writes, and the store keeps
//     the highest number it has applied per client, so a repeat is dropped. That
//     table is replicated state: it changes in the same atomic batch as the write,
//     goes into snapshots, and comes out the same on every node.
//   - Durable position. The store records which log entry it last applied, also in
//     the same batch, so after a crash Raft replays exactly what was lost.
//   - Snapshots. The store's durable contents are the snapshot. Compaction flushes
//     them and discards the log, and bytes are only built when a lagging follower
//     needs them.
package kv

import (
	"encoding/binary"
	"errors"
)

// Kind is what a command does to a key.
type Kind uint8

const (
	// Put sets a key.
	Put Kind = iota + 1
	// Delete removes a key.
	Delete
	// Append adds to a key's value. It is the operation that makes a duplicate
	// visible: unlike Put, applying it twice gives a different result.
	Append
)

// Command is one client write. Together ClientID and Seq name it, so a retry of
// the same write carries the same pair. A client must not send Seq+1 until Seq has
// succeeded, because the session only remembers the highest number.
type Command struct {
	ClientID uint64
	Seq      uint64
	Kind     Kind
	Key      []byte
	Value    []byte
}

var errBadCommand = errors.New("kv: malformed command")

func encodeCommand(c Command) []byte {
	buf := make([]byte, 0, 32+len(c.Key)+len(c.Value))
	buf = append(buf, byte(c.Kind))
	buf = binary.AppendUvarint(buf, c.ClientID)
	buf = binary.AppendUvarint(buf, c.Seq)
	buf = binary.AppendUvarint(buf, uint64(len(c.Key)))
	buf = append(buf, c.Key...)
	return append(buf, c.Value...)
}

func decodeCommand(b []byte) (Command, error) {
	if len(b) < 1 {
		return Command{}, errBadCommand
	}
	c := Command{Kind: Kind(b[0])}
	b = b[1:]
	for _, v := range []*uint64{&c.ClientID, &c.Seq} {
		x, n := binary.Uvarint(b)
		if n <= 0 {
			return Command{}, errBadCommand
		}
		*v = x
		b = b[n:]
	}
	keyLen, n := binary.Uvarint(b)
	if n <= 0 || keyLen > uint64(len(b)-n) {
		return Command{}, errBadCommand
	}
	b = b[n:]
	c.Key, c.Value = b[:keyLen], b[keyLen:]
	return c, nil
}
