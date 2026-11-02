package kv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/abishekprabakar/raftkv/internal/storage/engine"
	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

// Write is one change in an atomic batch.
type Write struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Backend is where the store keeps its contents.
type Backend interface {
	// Get reads a key.
	Get(key []byte) (value []byte, found bool, err error)
	// Apply makes a batch take effect atomically. It need not be durable yet. A
	// crash may lose the newest batches, but only whole batches, and only from the
	// end: what survives is always a prefix of what was applied. The store's
	// recorded position depends on this, and it is why the WAL syncs each finished
	// segment even with fsync off.
	Apply(batch []Write) error
	// Flush makes everything applied so far durable.
	Flush() error
	// Scan calls fn for each key in [start, end) in order. A nil bound is open.
	Scan(start, end []byte, fn func(key, value []byte) error) error
	// Replace swaps the whole contents for these, durably and atomically.
	Replace(contents []Write) error
	// Close releases resources without flushing, as a crash would.
	Close() error
}

// ---- LSM ------------------------------------------------------------------------

// LSM keeps the store in the storage engine. The engine runs without fsync on its
// own WAL: Raft's log already makes every entry durable, so a write lost to a
// power cut is simply applied again from the log. Paying for a second fsync on
// every write would add nothing. Flush is the one durable point, and it is what
// has to happen before the Raft log may be cut.
//
// Each generation of the data lives in its own directory, and the CURRENT file
// names the live one. Replace builds a new generation beside the old one and
// switches with an atomic rename, so a crash leaves one whole generation or the
// other, never a mix.
type LSM struct {
	root string
	gen  uint64
	db   *engine.DB
}

const lsmCurrent = "CURRENT"

func lsmOptions() engine.Options {
	return engine.Options{SyncMode: wal.NoSync, NoFlushOnClose: true}
}

func (l *LSM) genDir(gen uint64) string {
	return filepath.Join(l.root, fmt.Sprintf("gen-%020d", gen))
}

// OpenLSM opens or creates a store under root, and removes any generation left
// behind by a crash.
func OpenLSM(root string) (*LSM, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	l := &LSM{root: root, gen: 1}
	data, err := os.ReadFile(filepath.Join(root, lsmCurrent))
	switch {
	case err == nil:
		if l.gen, err = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err != nil {
			return nil, fmt.Errorf("kv: bad %s: %w", lsmCurrent, err)
		}
	case !os.IsNotExist(err):
		return nil, err
	}
	dirs, err := filepath.Glob(filepath.Join(root, "gen-*"))
	if err != nil {
		return nil, err
	}
	for _, d := range dirs {
		if d != l.genDir(l.gen) {
			if err := os.RemoveAll(d); err != nil {
				return nil, err
			}
		}
	}
	if l.db, err = engine.Open(l.genDir(l.gen), lsmOptions()); err != nil {
		return nil, err
	}
	return l, nil
}

// Get implements Backend.
func (l *LSM) Get(key []byte) ([]byte, bool, error) {
	v, err := l.db.Get(key)
	if errors.Is(err, engine.ErrNotFound) {
		return nil, false, nil
	}
	return v, err == nil, err
}

// Apply implements Backend.
func (l *LSM) Apply(batch []Write) error {
	var b engine.Batch
	for _, w := range batch {
		if w.Delete {
			b.Delete(w.Key)
		} else {
			b.Put(w.Key, w.Value)
		}
	}
	return l.db.Write(&b)
}

// Flush implements Backend.
func (l *LSM) Flush() error { return l.db.Flush() }

// Scan implements Backend.
func (l *LSM) Scan(start, end []byte, fn func(key, value []byte) error) error {
	it := l.db.Scan(start, end)
	defer it.Close()
	for it.Next() {
		if err := fn(it.Key(), it.Value()); err != nil {
			return err
		}
	}
	return it.Err()
}

// Replace implements Backend.
func (l *LSM) Replace(contents []Write) error {
	const chunkBytes = 1 << 20
	gen := l.gen + 1
	dir := l.genDir(gen)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	db, err := engine.Open(dir, lsmOptions())
	if err != nil {
		return err
	}
	var b engine.Batch
	size := 0
	for _, w := range contents {
		b.Put(w.Key, w.Value)
		if size += len(w.Key) + len(w.Value); size >= chunkBytes {
			if err := db.Write(&b); err != nil {
				db.Close()
				return err
			}
			b, size = engine.Batch{}, 0
		}
	}
	if err := db.Write(&b); err != nil {
		db.Close()
		return err
	}
	// The new generation must be durable before CURRENT names it.
	if err := db.Flush(); err != nil {
		db.Close()
		return err
	}
	if err := writeFileAtomic(filepath.Join(l.root, lsmCurrent), []byte(strconv.FormatUint(gen, 10))); err != nil {
		db.Close()
		return err
	}
	old, oldDir := l.db, l.genDir(l.gen)
	l.db, l.gen = db, gen
	if err := old.Close(); err != nil {
		return err
	}
	return os.RemoveAll(oldDir)
}

// Close implements Backend.
func (l *LSM) Close() error { return l.db.Close() }

// writeFileAtomic replaces a file so that a crash leaves the old or new contents.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// ---- memory ---------------------------------------------------------------------

// MemBackend keeps the store in memory, with the durability of the LSM backend
// made explicit, so the simulator can test what a power cut does to it. Applied
// batches wait in a journal until Flush moves them to the durable map. Crash keeps
// only a prefix of the journal, which is what the engine's WAL can leave behind.
type MemBackend struct {
	durable map[string][]byte
	journal [][]Write
	live    map[string][]byte
}

// NewMemBackend returns an empty backend.
func NewMemBackend() *MemBackend {
	return &MemBackend{durable: map[string][]byte{}, live: map[string][]byte{}}
}

func applyTo(m map[string][]byte, batch []Write) {
	for _, w := range batch {
		if w.Delete {
			delete(m, string(w.Key))
		} else {
			m[string(w.Key)] = w.Value
		}
	}
}

// Unflushed returns the number of batches a crash could lose.
func (m *MemBackend) Unflushed() int { return len(m.journal) }

// Crash forgets everything but the durable contents and the first keep batches
// of the journal.
func (m *MemBackend) Crash(keep int) {
	for _, batch := range m.journal[:keep] {
		applyTo(m.durable, batch)
	}
	m.journal = nil
	m.live = make(map[string][]byte, len(m.durable))
	for k, v := range m.durable {
		m.live[k] = v
	}
}

// Get implements Backend.
func (m *MemBackend) Get(key []byte) ([]byte, bool, error) {
	v, ok := m.live[string(key)]
	return v, ok, nil
}

// Apply implements Backend.
func (m *MemBackend) Apply(batch []Write) error {
	applyTo(m.live, batch)
	m.journal = append(m.journal, batch)
	return nil
}

// Flush implements Backend.
func (m *MemBackend) Flush() error {
	for _, batch := range m.journal {
		applyTo(m.durable, batch)
	}
	m.journal = nil
	return nil
}

// Scan implements Backend.
func (m *MemBackend) Scan(start, end []byte, fn func(key, value []byte) error) error {
	keys := make([]string, 0, len(m.live))
	for k := range m.live {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if start != nil && bytes.Compare([]byte(k), start) < 0 {
			continue
		}
		if end != nil && bytes.Compare([]byte(k), end) >= 0 {
			continue
		}
		if err := fn([]byte(k), m.live[k]); err != nil {
			return err
		}
	}
	return nil
}

// Replace implements Backend.
func (m *MemBackend) Replace(contents []Write) error {
	m.journal = nil
	m.durable = make(map[string][]byte, len(contents))
	m.live = make(map[string][]byte, len(contents))
	applyTo(m.durable, contents)
	applyTo(m.live, contents)
	return nil
}

// Close implements Backend.
func (m *MemBackend) Close() error { return nil }
