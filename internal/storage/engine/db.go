// Package engine is the single-node LSM-tree storage engine behind raftkv.
//
// Write path: a writer takes writeMu, assigns the next sequence number,
// appends the record to the WAL (group committed) and inserts it into the
// skiplist memtable, then releases writeMu and waits for the WAL to report
// durability. Reads use only sequence numbers at or below the last durable
// sequence number ("visible"), so a value is never observable before it is
// on disk.
//
// Read path: memtable, then frozen memtables newest first, then L0 tables
// newest first, then one table per deeper level. Every table lookup is
// guarded by the table's key range and bloom filter.
//
// Background work: one goroutine flushes frozen memtables to L0 and runs
// leveled compactions. Versions are reference counted so readers never see
// a file disappear.
package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
	"github.com/abishekprabakar/raftkv/internal/storage/memtable"
	"github.com/abishekprabakar/raftkv/internal/storage/sstable"
	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

var (
	// ErrNotFound is returned by Get for missing or deleted keys.
	ErrNotFound = errors.New("engine: key not found")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("engine: closed")
	// ErrTooLarge is returned for a key and value too big for one WAL record.
	ErrTooLarge = errors.New("engine: key and value too large")
)

// recordHeader is the size of a WAL payload before the key and value: kind
// u8, seq u64, keylen u32.
const recordHeader = 13

// recordBatch in the first byte of a WAL payload marks an atomic batch, in place
// of the kind of a single write. See encodeBatch.
const recordBatch = 2

// testHookBeforeCapture lets a regression test run code just before a read
// captures its view of the database. It is always nil outside tests.
var testHookBeforeCapture func()

// immTable is a frozen memtable waiting to be flushed.
type immTable struct {
	table  *memtable.Table
	maxSeq uint64
	segs   []uint64 // WAL segments that become garbage once flushed
}

// DB is an LSM-tree key-value store.
type DB struct {
	opts Options
	dir  string
	lock *os.File
	wal  *wal.Log

	// writeMu serialises writers: sequence assignment, WAL append and
	// memtable insert happen under it so WAL order equals sequence order.
	writeMu sync.Mutex
	seq     uint64 // last assigned sequence; guarded by writeMu

	visible atomic.Uint64 // highest durable sequence
	closing atomic.Bool

	// mu guards everything below.
	mu         sync.Mutex
	cond       *sync.Cond
	mem        *memtable.Table
	imm        []*immTable
	current    *version
	nextFile   uint64
	flushedSeq uint64
	compactPtr [][]byte // per level: largest user key of the last compacted file
	bgBusy     bool
	bgErr      error
	closed     bool
	manualReq  bool // force compaction down to a single level
	idleReq    bool // a caller is waiting for the worker to run out of work

	wake       chan struct{}
	workerDone chan struct{}

	sstStats sstable.Stats
	counters counters
}

type counters struct {
	puts, deletes, gets, scans             atomic.Int64
	flushes, flushBytes                    atomic.Int64
	compactions, compactRead, compactWrite atomic.Int64
	trivialMoves, stalls                   atomic.Int64
	replayedRecords, replayedBytes         atomic.Int64
	recoveryNanos, replayNanos             atomic.Int64
}

// Open opens or creates a database in dir, recovering from the WAL.
func Open(dir string, opts Options) (*DB, error) {
	start := time.Now()
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(filepath.Join(dir, "LOCK"))
	if err != nil {
		return nil, err
	}
	db := &DB{
		opts:       opts,
		dir:        dir,
		lock:       lock,
		mem:        memtable.New(),
		compactPtr: make([][]byte, opts.MaxLevels),
		wake:       make(chan struct{}, 1),
		workerDone: make(chan struct{}),
	}
	db.cond = sync.NewCond(&db.mu)
	if err := db.recover(); err != nil {
		lock.Close()
		return nil, err
	}
	go db.worker()
	db.signalWorker()
	db.counters.recoveryNanos.Store(int64(time.Since(start)))
	return db, nil
}

func (db *DB) recover() error {
	m, err := readManifest(db.manifestPath(), db.opts.MaxLevels)
	if err != nil {
		return err
	}
	db.nextFile = m.NextFile
	db.flushedSeq = m.FlushedSeq

	live := map[uint64]bool{}
	v := newVersion(db.opts.MaxLevels)
	for n, lvl := range m.Levels {
		if n >= db.opts.MaxLevels {
			return fmt.Errorf("manifest has %d levels, options allow %d", len(m.Levels), db.opts.MaxLevels)
		}
		for _, mf := range lvl {
			live[mf.Num] = true
			f := &fileMeta{num: mf.Num, size: mf.Size, db: db, props: sstable.Properties{
				NumEntries: mf.NumEntries, MinSeq: mf.MinSeq, MaxSeq: mf.MaxSeq,
				Smallest: mf.Smallest, Largest: mf.Largest, DataBytes: mf.DataBytes, NumBlocks: mf.NumBlocks,
			}}
			r, err := sstable.Open(db.tablePath(f.num), &db.sstStats)
			if err != nil {
				return err
			}
			f.reader = r
			v.addFile(n, f)
			if f.num >= db.nextFile {
				db.nextFile = f.num + 1
			}
		}
	}
	// L0 is searched newest-first and the manifest stores it in that order,
	// but addFile prepends, so restore the order by file number: flushes are
	// serialised, so a higher number always means newer data.
	sort.Slice(v.levels[0], func(i, j int) bool { return v.levels[0][i].num > v.levels[0][j].num })
	v.ref()
	db.current = v

	// Remove tables and temp files the manifest does not know about: they
	// were being written when a previous process died.
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".sst") {
			num, err := strconv.ParseUint(strings.TrimSuffix(name, ".sst"), 10, 64)
			if err == nil && !live[num] {
				os.Remove(filepath.Join(db.dir, name))
				if num >= db.nextFile {
					db.nextFile = num + 1
				}
			}
		} else if strings.HasSuffix(name, ".tmp") {
			os.Remove(filepath.Join(db.dir, name))
		}
	}

	// Replay the WAL into a fresh memtable, skipping records already
	// covered by flushed tables.
	db.wal, err = wal.Open(db.dir, wal.Options{Mode: db.opts.SyncMode, GroupCommitWindow: db.opts.GroupCommitWindow})
	if err != nil {
		return err
	}
	oldSegs, err := db.wal.Segments()
	if err != nil {
		return err
	}
	maxSeq := db.flushedSeq
	replayStart := time.Now()
	insert := func(seq uint64, kind ikey.Kind, key, value []byte) {
		if seq > maxSeq {
			maxSeq = seq
		}
		if seq > db.flushedSeq {
			db.counters.replayedRecords.Add(1)
			db.mem.Put(ikey.Encode(nil, key, seq, kind), append([]byte(nil), value...))
		}
	}
	err = db.wal.Replay(func(_ uint64, payload []byte) error {
		db.counters.replayedBytes.Add(int64(len(payload)))
		if len(payload) > 0 && payload[0] == recordBatch {
			return replayBatch(payload, insert)
		}
		seq, kind, key, value, err := decodeRecord(payload)
		if err != nil {
			return err
		}
		insert(seq, kind, key, value)
		return nil
	})
	if err != nil {
		return err
	}
	db.counters.replayNanos.Store(int64(time.Since(replayStart)))
	db.seq = maxSeq
	db.visible.Store(maxSeq)
	if err := db.wal.Start(); err != nil {
		return err
	}
	// Flush recovered data right away so the old segments can go and the
	// next recovery starts from the tables.
	if db.mem.Count() > 0 {
		db.imm = append(db.imm, &immTable{table: db.mem, maxSeq: maxSeq, segs: oldSegs})
		db.mem = memtable.New()
		if err := db.flushOne(); err != nil {
			return err
		}
	} else {
		for _, s := range oldSegs {
			if err := db.wal.Remove(s); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- record encoding -------------------------------------------------------

// WAL payload: kind u8, seq u64 LE, keylen u32 LE, key, value.
func encodeRecord(seq uint64, kind ikey.Kind, key, value []byte) []byte {
	buf := make([]byte, 0, recordHeader+len(key)+len(value))
	buf = append(buf, byte(kind))
	buf = binary.LittleEndian.AppendUint64(buf, seq)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	return append(buf, value...)
}

func decodeRecord(p []byte) (seq uint64, kind ikey.Kind, key, value []byte, err error) {
	if len(p) < recordHeader {
		return 0, 0, nil, nil, errors.New("engine: short wal record")
	}
	kind = ikey.Kind(p[0])
	seq = binary.LittleEndian.Uint64(p[1:9])
	kl := binary.LittleEndian.Uint32(p[9:13])
	if uint64(len(p)) < recordHeader+uint64(kl) {
		return 0, 0, nil, nil, errors.New("engine: bad wal record key length")
	}
	return seq, kind, p[recordHeader : recordHeader+kl], p[recordHeader+kl:], nil
}

// A batch record is recordBatch u8, first seq u64, count u32, then for each op:
// kind u8, uvarint keylen, uvarint valuelen, key, value. Ops take consecutive
// sequence numbers. One record has one checksum, so after a crash replay sees the
// whole batch or none of it.
func encodeBatch(first uint64, b *Batch) []byte {
	buf := make([]byte, 0, b.encodedBytes())
	buf = append(buf, recordBatch)
	buf = binary.LittleEndian.AppendUint64(buf, first)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b.ops)))
	for _, op := range b.ops {
		buf = append(buf, byte(op.kind))
		buf = binary.AppendUvarint(buf, uint64(len(op.key)))
		buf = binary.AppendUvarint(buf, uint64(len(op.value)))
		buf = append(buf, op.key...)
		buf = append(buf, op.value...)
	}
	return buf
}

// replayBatch calls fn for each op in a batch record.
func replayBatch(p []byte, fn func(seq uint64, kind ikey.Kind, key, value []byte)) error {
	bad := errors.New("engine: bad batch record")
	if len(p) < 13 {
		return bad
	}
	seq := binary.LittleEndian.Uint64(p[1:9])
	count := binary.LittleEndian.Uint32(p[9:13])
	p = p[13:]
	for i := uint32(0); i < count; i++ {
		if len(p) < 1 {
			return bad
		}
		kind := ikey.Kind(p[0])
		kl, n1 := binary.Uvarint(p[1:])
		if n1 <= 0 {
			return bad
		}
		vl, n2 := binary.Uvarint(p[1+n1:])
		if n2 <= 0 {
			return bad
		}
		p = p[1+n1+n2:]
		if uint64(len(p)) < kl+vl {
			return bad
		}
		fn(seq, kind, p[:kl], p[kl:kl+vl])
		p = p[kl+vl:]
		seq++
	}
	return nil
}

// ---- writes ----------------------------------------------------------------

// Put stores value under key. It returns once the write is durable.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	db.counters.puts.Add(1)
	return db.write(ikey.KindValue, key, value)
}

// Delete removes key. It returns once the tombstone is durable.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	db.counters.deletes.Add(1)
	return db.write(ikey.KindDelete, key, nil)
}

// Write applies every op in b, or none of them. It returns once the batch is
// durable according to the sync mode. Readers see the whole batch or none of it,
// because visible jumps past all its sequence numbers at once.
func (db *DB) Write(b *Batch) error {
	if len(b.ops) == 0 {
		return nil
	}
	if b.encodedBytes() > wal.MaxRecordBytes {
		return ErrTooLarge
	}
	db.writeMu.Lock()
	if db.closing.Load() {
		db.writeMu.Unlock()
		return ErrClosed
	}
	if err := db.makeRoomForWrite(); err != nil {
		db.writeMu.Unlock()
		return err
	}
	first := db.seq + 1
	db.seq += uint64(len(b.ops))
	last := db.seq
	fut := db.wal.Append(encodeBatch(first, b))
	for i, op := range b.ops {
		db.mem.Put(ikey.Encode(nil, op.key, first+uint64(i), op.kind), append([]byte(nil), op.value...))
	}
	db.writeMu.Unlock()

	if err := fut.Wait(); err != nil {
		return err
	}
	db.advanceVisible(last)
	return nil
}

// advanceVisible raises visible to seq. Every record with a lower sequence number
// sits earlier in the WAL and is therefore durable too, so this is safe.
func (db *DB) advanceVisible(seq uint64) {
	for {
		cur := db.visible.Load()
		if cur >= seq || db.visible.CompareAndSwap(cur, seq) {
			return
		}
	}
}

func (db *DB) write(kind ikey.Kind, key, value []byte) error {
	// Reject this before it gets a sequence number. Recovery cannot replay a
	// record above the WAL's limit, so acknowledging one would lose it later.
	if recordHeader+len(key)+len(value) > wal.MaxRecordBytes {
		return ErrTooLarge
	}
	db.writeMu.Lock()
	if db.closing.Load() {
		db.writeMu.Unlock()
		return ErrClosed
	}
	if err := db.makeRoomForWrite(); err != nil {
		db.writeMu.Unlock()
		return err
	}
	db.seq++
	seq := db.seq
	fut := db.wal.Append(encodeRecord(seq, kind, key, value))
	db.mem.Put(ikey.Encode(nil, key, seq, kind), append([]byte(nil), value...))
	db.writeMu.Unlock()

	if err := fut.Wait(); err != nil {
		return err
	}
	db.advanceVisible(seq)
	return nil
}

// makeRoomForWrite freezes the memtable when full, stalling if too much
// unflushed data has accumulated. Caller holds writeMu.
func (db *DB) makeRoomForWrite() error {
	if db.mem.Size() < db.opts.MemtableBytes {
		return nil
	}
	db.mu.Lock()
	stalled := false
	for !db.closed && db.bgErr == nil &&
		(len(db.imm) >= db.opts.MaxImmutableMemtables ||
			(!db.opts.DisableAutoCompaction && len(db.current.levels[0]) >= db.opts.L0StopWritesTrigger)) {
		if !stalled {
			stalled = true
			db.counters.stalls.Add(1)
		}
		db.signalWorker()
		db.cond.Wait()
	}
	err := db.bgErr
	db.mu.Unlock()
	if err != nil {
		return err
	}
	seg, err := db.wal.Rotate()
	if err != nil {
		return err
	}
	db.mu.Lock()
	db.imm = append(db.imm, &immTable{table: db.mem, maxSeq: db.seq, segs: []uint64{seg}})
	db.mem = memtable.New()
	db.mu.Unlock()
	db.signalWorker()
	return nil
}

// ---- reads -----------------------------------------------------------------

// Get returns the value stored under key or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.counters.gets.Add(1)
	if testHookBeforeCapture != nil {
		testHookBeforeCapture()
	}
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil, ErrClosed
	}
	mem := db.mem
	imm := db.imm
	v := db.current
	v.ref()
	// Load the snapshot only after capturing the version. A flush or
	// compaction drops old versions of a key once a newer one is visible,
	// judged by the value of visible when it started. visible never goes
	// backwards, so a snapshot loaded now is at least that value, and every
	// version dropped from v is shadowed by one this read can see. Loading
	// the snapshot first would let a read see neither of them.
	snap := db.visible.Load()
	db.mu.Unlock()
	defer v.unref()

	search := ikey.SearchKey(nil, key, snap)
	if k, val, ok := mem.Seek(search); ok && bytes.Equal(ikey.UserKey(k), key) {
		return result(k, val)
	}
	for i := len(imm) - 1; i >= 0; i-- {
		if k, val, ok := imm[i].table.Seek(search); ok && bytes.Equal(ikey.UserKey(k), key) {
			return result(k, val)
		}
	}
	for _, f := range v.levels[0] {
		if !f.containsUser(key) || !f.reader.MayContain(key) {
			continue
		}
		k, val, found, err := f.reader.Get(search)
		if err != nil {
			return nil, err
		}
		if found && bytes.Equal(ikey.UserKey(k), key) {
			return result(k, val)
		}
	}
	for n := 1; n < len(v.levels); n++ {
		f := v.fileForKey(n, key)
		if f == nil || !f.reader.MayContain(key) {
			continue
		}
		k, val, found, err := f.reader.Get(search)
		if err != nil {
			return nil, err
		}
		if found && bytes.Equal(ikey.UserKey(k), key) {
			return result(k, val)
		}
	}
	return nil, ErrNotFound
}

func result(k, val []byte) ([]byte, error) {
	if _, _, kind := ikey.Split(k); kind == ikey.KindDelete {
		return nil, ErrNotFound
	}
	return append([]byte(nil), val...), nil
}

// ---- lifecycle -------------------------------------------------------------

// Flush freezes the memtable and waits until it has been written to L0.
func (db *DB) Flush() error {
	db.writeMu.Lock()
	db.mu.Lock()
	empty := db.mem.Count() == 0
	db.mu.Unlock()
	if !empty {
		seg, err := db.wal.Rotate()
		if err != nil {
			db.writeMu.Unlock()
			return err
		}
		db.mu.Lock()
		db.imm = append(db.imm, &immTable{table: db.mem, maxSeq: db.seq, segs: []uint64{seg}})
		db.mem = memtable.New()
		db.mu.Unlock()
	}
	db.writeMu.Unlock()
	db.signalWorker()
	db.mu.Lock()
	defer db.mu.Unlock()
	for len(db.imm) > 0 && db.bgErr == nil && !db.closed {
		db.cond.Wait()
	}
	return db.bgErr
}

// WaitIdle flushes the memtable and blocks until the background worker has
// nothing left to do: no frozen memtables and no level over its target.
func (db *DB) WaitIdle() error { return db.drain(false) }

// Compact flushes the memtable and compacts until all data sits in a single
// level. It is intended for tests and benchmarks.
func (db *DB) Compact() error { return db.drain(true) }

// drain asks the worker for a full pass and waits until it reports that
// nothing is left. The request flags are cleared by the worker itself, so
// this cannot return early if the worker was between passes when called.
func (db *DB) drain(manual bool) error {
	if err := db.Flush(); err != nil {
		return err
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	db.idleReq = true
	if manual {
		db.manualReq = true
	}
	db.signalWorker()
	for db.idleReq && db.bgErr == nil && !db.closed {
		db.cond.Wait()
	}
	return db.bgErr
}

// Close flushes the memtable (unless NoFlushOnClose), stops background work
// and releases all files.
func (db *DB) Close() error {
	if db.closing.Swap(true) {
		return nil
	}
	var err error
	if !db.opts.NoFlushOnClose {
		err = db.Flush()
	} else if serr := db.wal.Sync(); serr != nil {
		err = serr
	}
	db.mu.Lock()
	db.closed = true
	db.cond.Broadcast()
	db.mu.Unlock()
	db.signalWorker()
	<-db.workerDone
	if werr := db.wal.Close(); err == nil {
		err = werr
	}
	db.mu.Lock()
	for _, lvl := range db.current.levels {
		for _, f := range lvl {
			f.reader.Close()
		}
	}
	db.mu.Unlock()
	db.lock.Close()
	return err
}

func (db *DB) signalWorker() {
	select {
	case db.wake <- struct{}{}:
	default:
	}
}

// ---- stats -----------------------------------------------------------------

// Stats is a point-in-time snapshot of engine counters.
type Stats struct {
	Puts, Deletes, Gets                     int64
	WALBytes, WALRecords, WALSyncs          int64
	WALBatches, WALTruncatedBytes           int64
	Flushes, FlushBytes                     int64
	Compactions, CompactionBytesRead        int64
	CompactionBytesWritten, TrivialMoves    int64
	BlockReads, BloomChecks, BloomNegatives int64
	WriteStalls                             int64
	ReplayedRecords, ReplayedBytes          int64
	RecoveryDuration                        time.Duration // whole Open, including flushing replayed data
	ReplayDuration                          time.Duration // WAL read and memtable rebuild only
	MemtableBytes                           int64
	ImmutableMemtables                      int
	LevelFiles                              []int
	LevelBytes                              []int64
	TotalSSTBytes                           int64
}

// Stats returns current counters and level shape.
func (db *DB) Stats() Stats {
	ws := db.wal.Stats()
	s := Stats{
		Puts: db.counters.puts.Load(), Deletes: db.counters.deletes.Load(), Gets: db.counters.gets.Load(),
		WALBytes: ws.BytesWritten.Load(), WALRecords: ws.Records.Load(), WALSyncs: ws.Syncs.Load(),
		WALBatches: ws.Batches.Load(), WALTruncatedBytes: ws.TruncatedBytes.Load(),
		Flushes: db.counters.flushes.Load(), FlushBytes: db.counters.flushBytes.Load(),
		Compactions: db.counters.compactions.Load(), CompactionBytesRead: db.counters.compactRead.Load(),
		CompactionBytesWritten: db.counters.compactWrite.Load(), TrivialMoves: db.counters.trivialMoves.Load(),
		BlockReads: db.sstStats.BlockReads.Load(), BloomChecks: db.sstStats.BloomChecks.Load(),
		BloomNegatives:  db.sstStats.BloomNegatives.Load(),
		WriteStalls:     db.counters.stalls.Load(),
		ReplayedRecords: db.counters.replayedRecords.Load(), ReplayedBytes: db.counters.replayedBytes.Load(),
		RecoveryDuration: time.Duration(db.counters.recoveryNanos.Load()),
		ReplayDuration:   time.Duration(db.counters.replayNanos.Load()),
	}
	db.mu.Lock()
	s.MemtableBytes = db.mem.Size()
	s.ImmutableMemtables = len(db.imm)
	for n, lvl := range db.current.levels {
		s.LevelFiles = append(s.LevelFiles, len(lvl))
		b := db.current.levelBytes(n)
		s.LevelBytes = append(s.LevelBytes, b)
		s.TotalSSTBytes += b
	}
	db.mu.Unlock()
	return s
}
