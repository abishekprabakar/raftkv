package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// Keys in the backend. A one-byte prefix keeps the three kinds of data apart, so
// that no user key can collide with a session or with the position.
const (
	prefixData    = 'd'
	prefixSession = 's'
)

var positionKey = []byte("p")

func dataKey(key []byte) []byte { return append([]byte{prefixData}, key...) }

func sessionKey(client uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{prefixSession}, client)
}

// Store is the replicated state machine. It is not safe for concurrent use.
type Store struct {
	backend  Backend
	index    uint64
	term     uint64
	sessions map[uint64]uint64 // client -> highest Seq applied; a copy of the backend's
}

// OpenStore loads a store's position and session table from its backend.
func OpenStore(b Backend) (*Store, error) {
	s := &Store{backend: b}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) load() error {
	s.index, s.term = 0, 0
	s.sessions = map[uint64]uint64{}
	v, found, err := s.backend.Get(positionKey)
	if err != nil {
		return err
	}
	if found {
		if len(v) != 16 {
			return errors.New("kv: bad position record")
		}
		s.index = binary.BigEndian.Uint64(v)
		s.term = binary.BigEndian.Uint64(v[8:])
	}
	start := []byte{prefixSession}
	end := []byte{prefixSession + 1}
	return s.backend.Scan(start, end, func(key, value []byte) error {
		if len(key) != 9 || len(value) != 8 {
			return errors.New("kv: bad session record")
		}
		s.sessions[binary.BigEndian.Uint64(key[1:])] = binary.BigEndian.Uint64(value)
		return nil
	})
}

func positionWrite(index, term uint64) Write {
	v := binary.BigEndian.AppendUint64(nil, index)
	return Write{Key: positionKey, Value: binary.BigEndian.AppendUint64(v, term)}
}

// Position returns the last entry the store holds.
func (s *Store) Position() (index, term uint64) { return s.index, s.term }

// Apply applies one committed entry. It returns the command, if the entry held
// one, and whether a session dropped it as a repeat.
//
// Everything an entry changes goes into one batch: the data, the session and the
// position. If they could come apart in a crash, the store could hold a write
// without remembering it, and applying the entry again would repeat it.
func (s *Store) Apply(e raft.Entry) (cmd Command, duplicate bool) {
	batch := []Write{positionWrite(e.Index, e.Term)}
	if e.Kind == raft.EntryCommand {
		var err error
		if cmd, err = decodeCommand(e.Data); err != nil {
			panic(fmt.Sprintf("kv: committed entry %d: %v", e.Index, err))
		}
		if cmd.Seq <= s.sessions[cmd.ClientID] {
			duplicate = true
		} else {
			batch = append(batch, s.dataWrite(cmd), Write{Key: sessionKey(cmd.ClientID), Value: binary.BigEndian.AppendUint64(nil, cmd.Seq)})
			s.sessions[cmd.ClientID] = cmd.Seq
		}
	}
	if err := s.backend.Apply(batch); err != nil {
		panic(fmt.Sprintf("kv: applying entry %d: %v", e.Index, err))
	}
	s.index, s.term = e.Index, e.Term
	return cmd, duplicate
}

func (s *Store) dataWrite(cmd Command) Write {
	key := dataKey(cmd.Key)
	switch cmd.Kind {
	case Put:
		return Write{Key: key, Value: cmd.Value}
	case Delete:
		return Write{Key: key, Delete: true}
	case Append:
		old, _, err := s.backend.Get(key)
		if err != nil {
			panic(fmt.Sprintf("kv: reading %q: %v", cmd.Key, err))
		}
		return Write{Key: key, Value: append(append([]byte(nil), old...), cmd.Value...)}
	}
	panic(fmt.Sprintf("kv: unknown command kind %d", cmd.Kind))
}

// Get reads a user key from the local state.
func (s *Store) Get(key []byte) (value []byte, found bool, err error) {
	return s.backend.Get(dataKey(key))
}

// Flush makes everything applied so far durable, which is what allows the Raft log
// through this position to be discarded.
func (s *Store) Flush() error { return s.backend.Flush() }

// Snapshot format: every key and value in the backend except the position, then a
// checksum. Sessions are ordinary keys, so they travel with the data.
func (s *Store) Snapshot() []byte {
	var buf []byte
	err := s.backend.Scan(nil, nil, func(key, value []byte) error {
		if string(key) == string(positionKey) {
			return nil
		}
		buf = binary.AppendUvarint(buf, uint64(len(key)))
		buf = binary.AppendUvarint(buf, uint64(len(value)))
		buf = append(buf, key...)
		buf = append(buf, value...)
		return nil
	})
	if err != nil {
		panic(fmt.Sprintf("kv: building snapshot: %v", err))
	}
	return binary.BigEndian.AppendUint32(buf, crc32.ChecksumIEEE(buf))
}

func decodeSnapshot(data []byte) ([]Write, error) {
	if len(data) < 4 {
		return nil, errors.New("kv: snapshot too short")
	}
	body, sum := data[:len(data)-4], binary.BigEndian.Uint32(data[len(data)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return nil, errors.New("kv: snapshot checksum mismatch")
	}
	var contents []Write
	for len(body) > 0 {
		kl, n1 := binary.Uvarint(body)
		if n1 <= 0 {
			return nil, errors.New("kv: bad snapshot")
		}
		vl, n2 := binary.Uvarint(body[n1:])
		if n2 <= 0 {
			return nil, errors.New("kv: bad snapshot")
		}
		body = body[n1+n2:]
		if kl+vl > uint64(len(body)) {
			return nil, errors.New("kv: bad snapshot")
		}
		contents = append(contents, Write{Key: body[:kl], Value: body[kl : kl+vl]})
		body = body[kl+vl:]
	}
	return contents, nil
}

// Restore replaces the whole store with a snapshot, and is durable on return.
func (s *Store) Restore(index, term uint64, data []byte) {
	contents, err := decodeSnapshot(data)
	if err != nil {
		panic(fmt.Sprintf("kv: restoring snapshot at %d: %v", index, err))
	}
	if err := s.backend.Replace(append(contents, positionWrite(index, term))); err != nil {
		panic(fmt.Sprintf("kv: restoring snapshot at %d: %v", index, err))
	}
	if err := s.load(); err != nil {
		panic(fmt.Sprintf("kv: loading restored snapshot at %d: %v", index, err))
	}
}
