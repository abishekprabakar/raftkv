// Package wal implements a segmented write-ahead log with per-record CRC32C
// checksums and group commit.
//
// On-disk layout: the log directory holds files named NNNNNNNNNNNNNNNNNNNN.wal
// (20-digit, zero-padded segment number). Each segment is a sequence of
// records:
//
//	+----------+-----------+-----------------+
//	| len u32  | crc32c u32| payload (len B) |
//	+----------+-----------+-----------------+
//
// Both integers are little-endian. The CRC covers only the payload. A record
// whose length or CRC does not check out ends the readable part of a
// segment. If it is in the newest segment and no valid record follows it, it
// is a torn tail: a write that never finished, which is expected after a crash
// and is truncated away. Anything else is reported as ErrCorrupt, because it
// means records that may have been acknowledged are damaged.
//
// Durability: Append enqueues the record and returns a Future. A single
// syncer goroutine drains everything that queued up since the last fsync,
// writes it with one write(2) and durably flushes it with one fsync, then
// completes every Future in the batch (group commit). Under load the fsync
// cost is therefore amortised across many concurrent writers.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SyncMode controls when Append reports a record as durable.
type SyncMode int

const (
	// GroupCommit batches concurrent appends into one write and one fsync.
	GroupCommit SyncMode = iota
	// SyncEach performs a write and an fsync for every single record.
	SyncEach
	// NoSync writes records to the OS page cache, and only calls fsync when a
	// segment is finished. Acknowledged records survive a process crash, but a
	// power loss can take the tail of the newest segment. What is left is always a
	// prefix of what was written, so a caller that can replay the rest, as the
	// replicated store can from its Raft log, loses nothing.
	NoSync
)

func (m SyncMode) String() string {
	switch m {
	case GroupCommit:
		return "group_commit"
	case SyncEach:
		return "fsync_per_write"
	case NoSync:
		return "no_sync"
	}
	return "unknown"
}

// MaxRecordBytes is the largest payload Append accepts. Replay treats a larger
// length field as damage, so the limit has to be enforced on the way in.
const MaxRecordBytes = 64 << 20

const (
	headerSize = 8
	suffix     = ".wal"
)

var (
	// ErrCorrupt is returned when a non-tail record fails validation.
	ErrCorrupt = errors.New("wal: corrupt record")
	// ErrClosed is returned by operations on a closed log.
	ErrClosed = errors.New("wal: closed")
	// ErrTooLarge is returned by Append for a payload over MaxRecordBytes.
	ErrTooLarge = errors.New("wal: record too large")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Stats counts what the log has done since it was opened.
type Stats struct {
	BytesWritten   atomic.Int64
	Records        atomic.Int64
	Syncs          atomic.Int64
	Batches        atomic.Int64
	TruncatedBytes atomic.Int64
}

// Future resolves once the appended record is durable.
type Future struct {
	done chan struct{}
	err  error
}

// Wait blocks until the record is durable (or failed).
func (f *Future) Wait() error {
	<-f.done
	return f.err
}

// Options configure a Log.
type Options struct {
	Mode SyncMode
	// GroupCommitWindow is how long the syncer waits for more writers to
	// queue up before starting a batch, applied only when the previous
	// batch held more than one record (so a lone writer never pays it).
	// Without it, with N closed-loop clients the syncer alternates between
	// batches of k and N-k records; the window lets writers released by the
	// last fsync rejoin, roughly doubling records per fsync.
	GroupCommitWindow time.Duration
}

// Log is a segmented write-ahead log.
type Log struct {
	dir    string
	mode   SyncMode
	window time.Duration
	stats  Stats

	mu       sync.Mutex
	cond     *sync.Cond // broadcast when a batch completes or state changes
	pending  []byte
	waiters  []*Future
	inflight bool
	closed   bool
	err      error

	cur    *os.File
	curSeg uint64
	curOff int64

	wake chan struct{}
	done chan struct{}
}

// Open opens (creating if needed) the log directory. It does not create an
// active segment: call Replay first, then Start.
func Open(dir string, opts Options) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	l := &Log{dir: dir, mode: opts.Mode, wake: make(chan struct{}, 1), done: make(chan struct{})}
	if opts.Mode == GroupCommit {
		l.window = opts.GroupCommitWindow // only worth it when there is an fsync to amortise
	}
	l.cond = sync.NewCond(&l.mu)
	return l, nil
}

// Stats returns the log counters.
func (l *Log) Stats() *Stats { return &l.stats }

// Segments lists existing segment numbers in ascending order.
func (l *Log) Segments() ([]uint64, error) {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return nil, err
	}
	var segs []uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(name, suffix), 10, 64)
		if err != nil {
			continue
		}
		segs = append(segs, n)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i] < segs[j] })
	return segs, nil
}

func (l *Log) segPath(n uint64) string {
	return filepath.Join(l.dir, fmt.Sprintf("%020d%s", n, suffix))
}

// Replay reads every segment in order and calls fn for each valid record. A
// torn tail in the newest segment is truncated; any other damage returns
// ErrCorrupt. Damage to the very last record cannot be told apart from a torn
// write, so it counts as a torn tail.
func (l *Log) Replay(fn func(seg uint64, payload []byte) error) error {
	segs, err := l.Segments()
	if err != nil {
		return err
	}
	for i, seg := range segs {
		last := i == len(segs)-1
		if err := l.replaySegment(seg, last, fn); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) replaySegment(seg uint64, last bool, fn func(uint64, []byte) error) error {
	path := l.segPath(seg)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	r := &reader{f: f}
	var off int64
	for {
		payload, n, err := r.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if !last {
				return fmt.Errorf("%w: segment %d offset %d: %v", ErrCorrupt, seg, off, err)
			}
			// Without fsync, a power cut can keep later pages of the file and lose
			// earlier ones, so a valid record after the damage proves nothing, and
			// keeping it would leave a hole. Everything from the damage on goes.
			if l.mode != NoSync {
				after, aerr := validRecordAfter(path, off)
				if aerr != nil {
					return aerr
				}
				if after {
					return fmt.Errorf("%w: segment %d offset %d: %v, and valid records follow it", ErrCorrupt, seg, off, err)
				}
			}
			l.stats.TruncatedBytes.Add(size - off)
			f.Close()
			return truncateDurably(path, off)
		}
		if err := fn(seg, payload); err != nil {
			return err
		}
		off += int64(n)
	}
}

// validRecordAfter reports whether a complete, checksummed record starts after
// the damaged record at off. Records reach the file in order, so a write that
// was cut off cannot have a good record after it: finding one means the damage
// is in data that was already on disk. The damaged length field cannot be
// trusted to find the next record, so every byte offset is tried. This only
// runs on a damaged log, so its cost does not matter.
func validRecordAfter(path string, off int64) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	size := int64(len(data))
	for i := off + 1; i+headerSize < size; i++ {
		n := int64(binary.LittleEndian.Uint32(data[i:]))
		if n == 0 || n > MaxRecordBytes || i+headerSize+n > size {
			continue
		}
		crc := binary.LittleEndian.Uint32(data[i+4:])
		if crc32.Checksum(data[i+headerSize:i+headerSize+n], castagnoli) == crc {
			return true, nil
		}
	}
	return false, nil
}

// truncateDurably cuts the file to size and fsyncs it, so a torn tail cannot
// come back after a power loss and turn into corruption in an older segment.
func truncateDurably(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

type reader struct {
	f   *os.File
	buf []byte
	pos int
	end int
	eof bool
}

func (r *reader) fill(n int) error {
	if r.end-r.pos >= n {
		return nil
	}
	if r.pos > 0 {
		copy(r.buf, r.buf[r.pos:r.end])
		r.end -= r.pos
		r.pos = 0
	}
	if cap(r.buf) < n {
		nb := make([]byte, n+256<<10)
		copy(nb, r.buf[:r.end])
		r.buf = nb
	}
	r.buf = r.buf[:cap(r.buf)]
	for r.end < n && !r.eof {
		m, err := r.f.Read(r.buf[r.end:])
		r.end += m
		if err == io.EOF {
			r.eof = true
		} else if err != nil {
			return err
		}
	}
	if r.end < n {
		return io.ErrUnexpectedEOF
	}
	return nil
}

// next returns the next payload and the number of bytes consumed. io.EOF
// means a clean end of file; any other error means a bad or partial record.
func (r *reader) next() ([]byte, int, error) {
	if err := r.fill(headerSize); err != nil {
		if err == io.ErrUnexpectedEOF && r.end == r.pos {
			return nil, 0, io.EOF
		}
		return nil, 0, err
	}
	hdr := r.buf[r.pos : r.pos+headerSize]
	n := binary.LittleEndian.Uint32(hdr[0:4])
	crc := binary.LittleEndian.Uint32(hdr[4:8])
	if n == 0 || n > MaxRecordBytes {
		return nil, 0, fmt.Errorf("bad record length %d", n)
	}
	if err := r.fill(headerSize + int(n)); err != nil {
		return nil, 0, err
	}
	payload := r.buf[r.pos+headerSize : r.pos+headerSize+int(n)]
	if crc32.Checksum(payload, castagnoli) != crc {
		return nil, 0, errors.New("crc mismatch")
	}
	r.pos += headerSize + int(n)
	return payload, headerSize + int(n), nil
}

// Start creates the active segment (numbered after every existing one) and
// launches the syncer. It must be called once, after Replay.
func (l *Log) Start() error {
	segs, err := l.Segments()
	if err != nil {
		return err
	}
	var next uint64 = 1
	if len(segs) > 0 {
		next = segs[len(segs)-1] + 1
	}
	if err := l.openSegment(next); err != nil {
		return err
	}
	go l.syncLoop()
	return nil
}

func (l *Log) openSegment(n uint64) error {
	f, err := os.OpenFile(l.segPath(n), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		f.Close()
		return err
	}
	l.cur, l.curSeg, l.curOff = f, n, 0
	return nil
}

// CurrentSegment returns the number of the active segment.
func (l *Log) CurrentSegment() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.curSeg
}

// Append frames payload and enqueues it. The returned Future resolves when
// the record is durable according to the sync mode. Appends made from a
// single goroutine (or under a caller-held lock) land in the file in call
// order.
func (l *Log) Append(payload []byte) *Future {
	fut := &Future{done: make(chan struct{})}
	if len(payload) > MaxRecordBytes {
		fut.err = ErrTooLarge
		close(fut.done)
		return fut
	}
	l.mu.Lock()
	if l.closed || l.err != nil {
		err := l.err
		if err == nil {
			err = ErrClosed
		}
		l.mu.Unlock()
		fut.err = err
		close(fut.done)
		return fut
	}
	var hdr [headerSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(payload, castagnoli))
	l.pending = append(l.pending, hdr[:]...)
	l.pending = append(l.pending, payload...)
	l.waiters = append(l.waiters, fut)
	l.stats.Records.Add(1)
	if l.mode == SyncEach {
		// Serialise the whole write+fsync under the lock: exactly one
		// fsync per record, no batching.
		l.flushLocked()
		l.mu.Unlock()
		return fut
	}
	l.mu.Unlock()
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return fut
}

// flushLocked writes and syncs everything pending. Caller holds l.mu.
func (l *Log) flushLocked() {
	data, waiters := l.pending, l.waiters
	l.pending, l.waiters = nil, nil
	err := l.writeAndSync(data)
	l.complete(waiters, err)
}

func (l *Log) writeAndSync(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	if _, err := l.cur.Write(data); err != nil {
		return err
	}
	l.curOff += int64(len(data))
	l.stats.BytesWritten.Add(int64(len(data)))
	l.stats.Batches.Add(1)
	if l.mode == NoSync {
		return nil
	}
	l.stats.Syncs.Add(1)
	return l.cur.Sync()
}

func (l *Log) complete(waiters []*Future, err error) {
	for _, w := range waiters {
		w.err = err
		close(w.done)
	}
}

func (l *Log) syncLoop() {
	defer close(l.done)
	prevBatch := 0
	for range l.wake {
		for {
			l.mu.Lock()
			if len(l.pending) == 0 || l.closed {
				closed := l.closed
				l.mu.Unlock()
				if closed {
					return
				}
				break
			}
			if l.window > 0 && prevBatch > 1 && !l.closed {
				l.mu.Unlock()
				time.Sleep(l.window)
				l.mu.Lock()
			}
			data, waiters := l.pending, l.waiters
			prevBatch = len(waiters)
			l.pending, l.waiters = nil, nil
			l.inflight = true
			f := l.cur
			l.mu.Unlock()

			err := l.writeAndSyncFile(f, data)

			l.mu.Lock()
			l.inflight = false
			if err != nil && l.err == nil {
				l.err = err
			}
			l.cond.Broadcast()
			l.mu.Unlock()
			l.complete(waiters, err)
		}
	}
}

// writeAndSyncFile is the syncer's version of writeAndSync; it does not
// touch l.cur so a concurrent Rotate (which waits for !inflight) is safe.
func (l *Log) writeAndSyncFile(f *os.File, data []byte) error {
	if _, err := f.Write(data); err != nil {
		return err
	}
	l.stats.BytesWritten.Add(int64(len(data)))
	l.stats.Batches.Add(1)
	if l.mode == NoSync {
		return nil
	}
	l.stats.Syncs.Add(1)
	return f.Sync()
}

// waitQuiescentLocked blocks until nothing is pending or in flight.
func (l *Log) waitQuiescentLocked() {
	for len(l.pending) > 0 || l.inflight {
		if len(l.pending) > 0 && !l.inflight {
			select {
			case l.wake <- struct{}{}:
			default:
			}
		}
		l.cond.Wait()
	}
}

// Sync blocks until every record appended before the call is durable.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.waitQuiescentLocked()
	return l.err
}

// Rotate makes every record appended so far durable, closes the active
// segment and opens a new one. It returns the number of the segment that
// was just closed. Appends issued after Rotate returns land in the new
// segment; the caller is responsible for ordering Rotate with respect to
// its own Appends.
func (l *Log) Rotate() (closedSeg uint64, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	l.waitQuiescentLocked()
	if l.err != nil {
		return 0, l.err
	}
	old, oldSeg := l.cur, l.curSeg
	// A finished segment must be whole on disk, even without fsync on each write.
	// Otherwise a power cut could keep the next segment and lose the end of this
	// one, leaving a hole in the middle of the log.
	if l.mode == NoSync {
		l.stats.Syncs.Add(1)
		if err := old.Sync(); err != nil {
			return 0, err
		}
	}
	if err := old.Close(); err != nil {
		return 0, err
	}
	if err := l.openSegment(oldSeg + 1); err != nil {
		return 0, err
	}
	return oldSeg, nil
}

// Remove deletes a segment that is no longer needed. Removing the active
// segment is an error.
func (l *Log) Remove(seg uint64) error {
	l.mu.Lock()
	cur := l.curSeg
	l.mu.Unlock()
	if seg == cur {
		return fmt.Errorf("wal: cannot remove active segment %d", seg)
	}
	if err := os.Remove(l.segPath(seg)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(l.dir)
}

// Close flushes pending records, stops the syncer and closes the segment.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.waitQuiescentLocked()
	l.closed = true
	err := l.err
	cur := l.cur
	started := cur != nil
	l.mu.Unlock()
	if started {
		close(l.wake)
		<-l.done
		if cerr := cur.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
