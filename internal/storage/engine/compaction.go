package engine

import (
	"bytes"
	"os"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
	"github.com/abishekprabakar/raftkv/internal/storage/sstable"
)

// Compaction strategy: leveled.
//
// L0 holds whole flushed memtables whose key ranges overlap. Every other
// level is a single sorted run of non-overlapping files, and level n+1 is
// LevelMultiplier times larger than level n. A compaction merges one file of
// level n (or all of L0) with the overlapping files of level n+1 and writes
// new files for level n+1. Reads touch at most one file per level beyond L0,
// and space amplification stays near 1.1x, at the cost of rewriting each
// byte roughly once per level (write amplification around LevelMultiplier
// per level in the worst case). The alternative, size-tiered compaction,
// writes less but can hold several versions of a key range at once, so it
// has worse read and space amplification; for a replicated store where every
// replica pays the disk cost, leveled is the better fit.

// worker is the single background goroutine. It drains frozen memtables
// first, then runs compactions while any level is over its target.
func (db *DB) worker() {
	defer close(db.workerDone)
	for range db.wake {
		for {
			db.mu.Lock()
			if db.closed {
				db.mu.Unlock()
				return
			}
			if db.bgErr != nil {
				db.bgBusy = false
				db.manualReq, db.idleReq = false, false
				db.cond.Broadcast()
				db.mu.Unlock()
				break
			}
			if len(db.imm) > 0 {
				db.bgBusy = true
				db.mu.Unlock()
				db.setBgErr(db.flushOne())
				continue
			}
			var c *compaction
			if !db.opts.DisableAutoCompaction || db.manualReq {
				c = db.pickCompaction()
			}
			if c == nil {
				db.bgBusy = false
				db.manualReq, db.idleReq = false, false
				db.cond.Broadcast()
				db.mu.Unlock()
				break
			}
			db.bgBusy = true
			db.mu.Unlock()
			db.setBgErr(db.runCompaction(c))
		}
	}
}

func (db *DB) setBgErr(err error) {
	if err == nil {
		return
	}
	db.mu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.cond.Broadcast()
	db.mu.Unlock()
}

// flushOne writes the oldest frozen memtable to a new L0 table.
func (db *DB) flushOne() error {
	db.mu.Lock()
	imm := db.imm[0]
	smallestSnap := db.visible.Load()
	db.mu.Unlock()

	it := memIter{imm.table.Iter()}
	it.SeekToFirst()
	outs, err := db.writeTables(it, 0, smallestSnap, 1<<62)
	if err != nil {
		return err
	}
	var bytesOut int64
	for _, f := range outs {
		bytesOut += int64(f.size)
	}
	db.counters.flushes.Add(1)
	db.counters.flushBytes.Add(bytesOut)

	db.mu.Lock()
	nv := db.current.clone()
	for _, f := range outs {
		nv.addFile(0, f)
	}
	db.flushedSeq = imm.maxSeq
	if err := db.writeManifestLocked(nv); err != nil {
		db.mu.Unlock()
		nv.unref()
		return err
	}
	db.installLocked(nv)
	db.imm = db.imm[1:]
	db.cond.Broadcast()
	db.mu.Unlock()

	for _, seg := range imm.segs {
		if err := db.wal.Remove(seg); err != nil {
			return err
		}
	}
	return nil
}

// installLocked makes nv the current version. Caller holds db.mu.
func (db *DB) installLocked(nv *version) {
	old := db.current
	nv.ref()
	db.current = nv
	old.unref()
}

// writeTables drains it into tables for level outLevel, splitting at
// targetBytes, dropping shadowed versions and obsolete tombstones.
func (db *DB) writeTables(it internalIter, outLevel int, smallestSnap uint64, targetBytes int64) ([]*fileMeta, error) {
	db.mu.Lock()
	base := db.current
	base.ref()
	db.mu.Unlock()
	defer base.unref()
	var (
		outs     []*fileMeta
		w        *sstable.Writer
		f        *os.File
		num      uint64
		lastUser []byte
		lastSeq  uint64
		haveLast bool
		lastOut  []byte // user key of the last entry written to the open file
	)
	finish := func() error {
		if w == nil {
			return nil
		}
		props, err := w.Finish()
		if err != nil {
			f.Close()
			return err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		r, err := sstable.Open(db.tablePath(num), &db.sstStats)
		if err != nil {
			return err
		}
		outs = append(outs, &fileMeta{num: num, size: uint64(info.Size()), props: props, reader: r, db: db})
		w, f = nil, nil
		return nil
	}
	fail := func(err error) ([]*fileMeta, error) {
		if f != nil {
			f.Close()
			os.Remove(db.tablePath(num))
		}
		for _, o := range outs {
			o.reader.Close()
			os.Remove(db.tablePath(o.num))
		}
		return nil, err
	}

	for ; it.Valid(); it.Next() {
		key := it.Key()
		user, seq, kind := ikey.Split(key)
		drop := false
		if haveLast && bytes.Equal(user, lastUser) {
			// A newer version of this key precedes it. If that version is
			// visible to every reader, nobody can observe this one.
			if lastSeq <= smallestSnap {
				drop = true
			}
		} else {
			lastUser = append(lastUser[:0], user...)
			haveLast = true
		}
		if !drop && kind == ikey.KindDelete && seq <= smallestSnap && isBaseLevelForKey(base, user, outLevel) {
			// No older version can exist below, so the tombstone has
			// nothing left to hide.
			drop = true
		}
		lastSeq = seq
		if drop {
			continue
		}
		if w != nil && int64(w.EstimatedSize()) >= targetBytes && !bytes.Equal(user, lastOut) {
			// Never split two versions of one user key across files.
			if err := finish(); err != nil {
				return fail(err)
			}
		}
		if w == nil {
			db.mu.Lock()
			num = db.nextFile
			db.nextFile++
			db.mu.Unlock()
			var err error
			f, err = os.OpenFile(db.tablePath(num), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
			if err != nil {
				return fail(err)
			}
			w = sstable.NewWriter(f, sstable.WriterOptions{BlockBytes: db.opts.BlockBytes, BloomBitsPerKey: db.opts.BloomBitsPerKey})
		}
		if err := w.Add(key, it.Value()); err != nil {
			return fail(err)
		}
		lastOut = append(lastOut[:0], user...)
	}
	if err := it.Err(); err != nil {
		return fail(err)
	}
	if err := finish(); err != nil {
		return fail(err)
	}
	return outs, nil
}

// isBaseLevelForKey reports whether no level deeper than outLevel can hold
// a version of user. For a flush (outLevel 0) existing L0 files count too.
func isBaseLevelForKey(v *version, user []byte, outLevel int) bool {
	first := outLevel + 1
	if outLevel == 0 {
		first = 0
	}
	for n := first; n < len(v.levels); n++ {
		for _, f := range v.levels[n] {
			if f.containsUser(user) {
				return false
			}
		}
	}
	return true
}

type compaction struct {
	level  int
	inputs [2][]*fileMeta // [0] from level, [1] from level+1
}

// pickCompaction chooses the most overfull level. Caller holds db.mu.
func (db *DB) pickCompaction() *compaction {
	v := db.current
	bestLevel, bestScore := -1, 0.0
	for n := 0; n < len(v.levels)-1; n++ {
		var score float64
		if n == 0 {
			score = float64(len(v.levels[0])) / float64(db.opts.L0CompactionTrigger)
		} else {
			score = float64(v.levelBytes(n)) / float64(db.opts.maxBytesForLevel(n))
		}
		if score > bestScore {
			bestLevel, bestScore = n, score
		}
	}
	if bestScore < 1 {
		if !db.manualReq {
			return nil
		}
		// Manual compaction: push the shallowest data down until everything
		// sits in one level, so Compact() leaves no overlapping runs and no
		// tombstone that could be dropped.
		lo, hi := -1, -1
		for n := range v.levels {
			if len(v.levels[n]) > 0 {
				if lo < 0 {
					lo = n
				}
				hi = n
			}
		}
		if lo < 0 || lo == hi {
			return nil
		}
		bestLevel = lo
	}
	c := &compaction{level: bestLevel}
	if bestLevel == 0 {
		c.inputs[0] = append([]*fileMeta(nil), v.levels[0]...)
	} else {
		lvl := v.levels[bestLevel]
		ptr := db.compactPtr[bestLevel]
		for _, f := range lvl {
			if ptr == nil || bytes.Compare(f.largestUser(), ptr) > 0 {
				c.inputs[0] = []*fileMeta{f}
				break
			}
		}
		if c.inputs[0] == nil {
			c.inputs[0] = []*fileMeta{lvl[0]}
		}
	}
	smallest, largest := keyRange(c.inputs[0])
	c.inputs[1] = v.overlapping(bestLevel+1, smallest, largest)
	return c
}

func keyRange(files []*fileMeta) (smallest, largest []byte) {
	for _, f := range files {
		if smallest == nil || bytes.Compare(f.smallestUser(), smallest) < 0 {
			smallest = f.smallestUser()
		}
		if largest == nil || bytes.Compare(f.largestUser(), largest) > 0 {
			largest = f.largestUser()
		}
	}
	return
}

// runCompaction merges c's inputs into level+1.
func (db *DB) runCompaction(c *compaction) error {
	out := c.level + 1
	db.counters.compactions.Add(1)

	// Trivial move: a single input with nothing to merge against just
	// changes level in the manifest.
	if len(c.inputs[0]) == 1 && len(c.inputs[1]) == 0 {
		f := c.inputs[0][0]
		db.counters.trivialMoves.Add(1)
		db.mu.Lock()
		defer db.mu.Unlock()
		nv := db.current.clone()
		nv.removeFile(c.level, f)
		nv.addFile(out, f)
		db.compactPtr[c.level] = append([]byte(nil), f.largestUser()...)
		if err := db.writeManifestLocked(nv); err != nil {
			nv.unref()
			return err
		}
		db.installLocked(nv)
		db.cond.Broadcast()
		return nil
	}

	var children []internalIter
	var bytesIn int64
	if c.level == 0 {
		for _, f := range c.inputs[0] { // already newest first
			children = append(children, tableIter{f.reader.Iter()})
			bytesIn += int64(f.size)
		}
	} else {
		children = append(children, newLevelIter(c.inputs[0]))
		for _, f := range c.inputs[0] {
			bytesIn += int64(f.size)
		}
	}
	if len(c.inputs[1]) > 0 {
		children = append(children, newLevelIter(c.inputs[1]))
		for _, f := range c.inputs[1] {
			bytesIn += int64(f.size)
		}
	}
	merged := newMergeIter(children)
	merged.SeekToFirst()
	smallestSnap := db.visible.Load()
	outs, err := db.writeTables(merged, out, smallestSnap, db.opts.TargetFileBytes)
	if err != nil {
		return err
	}
	var bytesOut int64
	for _, f := range outs {
		bytesOut += int64(f.size)
	}
	db.counters.compactRead.Add(bytesIn)
	db.counters.compactWrite.Add(bytesOut)

	db.mu.Lock()
	defer db.mu.Unlock()
	nv := db.current.clone()
	for _, f := range c.inputs[0] {
		nv.removeFile(c.level, f)
	}
	for _, f := range c.inputs[1] {
		nv.removeFile(out, f)
	}
	for _, f := range outs {
		nv.addFile(out, f)
	}
	if c.level > 0 {
		_, largest := keyRange(c.inputs[0])
		db.compactPtr[c.level] = append([]byte(nil), largest...)
	}
	if err := db.writeManifestLocked(nv); err != nil {
		nv.unref()
		for _, f := range outs {
			f.reader.Close()
			os.Remove(db.tablePath(f.num))
		}
		return err
	}
	db.installLocked(nv)
	db.cond.Broadcast()
	return nil
}
