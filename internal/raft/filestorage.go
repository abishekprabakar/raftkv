package raft

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

// FileStorage keeps Raft's durable state in a write-ahead log, using the same
// wal package as the storage engine. Each Save is one WAL record, and a record
// is all-or-nothing because of its checksum, which gives Update its atomicity
// without any extra work. Truncating the log is a record too: nothing on disk
// is rewritten, and replay rebuilds the final state by applying records in
// order.
//
// A snapshot is what shrinks the file. Its Update holds the complete state, so it
// starts a fresh segment and every older segment can go. See Save.
type FileStorage struct {
	log   *wal.Log
	state Durable // the replayed state; only Load reads it
}

// OpenFileStorage opens or creates storage in dir. A torn tail from a crash is
// dropped, which is safe because the Save that wrote it never returned, so no
// reply depended on it. Any other damage is an error: a node that silently lost
// a vote or a log entry it had acknowledged could break Raft's safety, so it
// must refuse to start.
func OpenFileStorage(dir string) (*FileStorage, error) {
	l, err := wal.Open(dir, wal.Options{Mode: wal.GroupCommit})
	if err != nil {
		return nil, err
	}
	s := &FileStorage{log: l}
	err = l.Replay(func(_ uint64, payload []byte) error {
		u, err := decodeUpdate(payload)
		if err != nil {
			return err
		}
		return s.state.apply(u)
	})
	if err != nil {
		return nil, fmt.Errorf("raft: replay log: %w", err)
	}
	if err := l.Start(); err != nil {
		return nil, err
	}
	return s, nil
}

// Load implements Storage.
func (s *FileStorage) Load() (Durable, error) { return s.state, nil }

// Save implements Storage. It returns after the WAL's fsync.
//
// A snapshot update is written to a new segment and only then are older segments
// deleted. A crash before the new record is durable leaves the old log whole. A
// crash after it leaves old segments to replay, which is harmless because the
// snapshot record replaces whatever they built. Deleting newest first means any
// survivors are still a contiguous prefix of the old log, so replay never meets a
// gap.
func (s *FileStorage) Save(u Update) error {
	if u.Snapshot == nil {
		return s.log.Append(encodeUpdate(u)).Wait()
	}
	closed, err := s.log.Rotate()
	if err != nil {
		return err
	}
	if err := s.log.Append(encodeUpdate(u)).Wait(); err != nil {
		return err
	}
	segs, err := s.log.Segments()
	if err != nil {
		return err
	}
	for i := len(segs) - 1; i >= 0; i-- {
		if segs[i] <= closed {
			if err := s.log.Remove(segs[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close releases the log. Everything Saved is already durable.
func (s *FileStorage) Close() error { return s.log.Close() }

// ---- record encoding -------------------------------------------------------

// Update record: flags u8, then if flagged: term and votedFor uvarints, then
// snapshot index and term uvarints, then truncateFrom uvarint, then an entry
// count and the entries. Entry: index uvarint, term uvarint, kind u8, data length
// uvarint, data.
const (
	flagHardState = 1 << iota
	flagTruncate
	flagSnapshot
)

func encodeUpdate(u Update) []byte {
	var flags byte
	if u.HardState != nil {
		flags |= flagHardState
	}
	if u.TruncateFrom != 0 {
		flags |= flagTruncate
	}
	if u.Snapshot != nil {
		flags |= flagSnapshot
	}
	buf := []byte{flags}
	if u.HardState != nil {
		buf = binary.AppendUvarint(buf, u.HardState.CurrentTerm)
		buf = binary.AppendUvarint(buf, uint64(u.HardState.VotedFor))
	}
	if u.Snapshot != nil {
		buf = binary.AppendUvarint(buf, u.Snapshot.Index)
		buf = binary.AppendUvarint(buf, u.Snapshot.Term)
	}
	if u.TruncateFrom != 0 {
		buf = binary.AppendUvarint(buf, u.TruncateFrom)
	}
	buf = binary.AppendUvarint(buf, uint64(len(u.Entries)))
	for _, e := range u.Entries {
		buf = binary.AppendUvarint(buf, e.Index)
		buf = binary.AppendUvarint(buf, e.Term)
		buf = append(buf, byte(e.Kind))
		buf = binary.AppendUvarint(buf, uint64(len(e.Data)))
		buf = append(buf, e.Data...)
	}
	return buf
}

// recordReader decodes one record and remembers the first failure, so the
// decoding code reads as a plain sequence of fields.
type recordReader struct {
	b   []byte
	err error
}

var errBadRecord = errors.New("raft: malformed log record")

func (r *recordReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errBadRecord
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *recordReader) byte() byte {
	if r.err != nil || len(r.b) == 0 {
		r.err = errBadRecord
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

// bytes copies, because the WAL reuses its buffer between records.
func (r *recordReader) bytes(n uint64) []byte {
	if r.err != nil || n > uint64(len(r.b)) {
		r.err = errBadRecord
		return nil
	}
	v := append([]byte(nil), r.b[:n]...)
	r.b = r.b[n:]
	return v
}

func decodeUpdate(payload []byte) (Update, error) {
	r := &recordReader{b: payload}
	var u Update
	flags := r.byte()
	if flags&flagHardState != 0 {
		u.HardState = &HardState{CurrentTerm: r.uvarint(), VotedFor: NodeID(r.uvarint())}
	}
	if flags&flagSnapshot != 0 {
		u.Snapshot = &Snapshot{Index: r.uvarint(), Term: r.uvarint()}
	}
	if flags&flagTruncate != 0 {
		u.TruncateFrom = r.uvarint()
	}
	count := r.uvarint()
	for i := uint64(0); i < count && r.err == nil; i++ {
		e := Entry{Index: r.uvarint(), Term: r.uvarint(), Kind: EntryKind(r.byte())}
		e.Data = r.bytes(r.uvarint())
		u.Entries = append(u.Entries, e)
	}
	if r.err == nil && len(r.b) != 0 {
		r.err = errBadRecord
	}
	return u, r.err
}
