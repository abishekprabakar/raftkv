package engine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"

	"github.com/abishekprabakar/raftkv/internal/storage/ikey"
	"github.com/abishekprabakar/raftkv/internal/storage/sstable"
)

// fileMeta describes one SSTable. Files are reference counted: every
// version that lists the file holds one reference, and the file is closed
// and deleted when the count drops to zero. Iterators keep the version they
// started with alive, so compaction never deletes a file under a reader.
type fileMeta struct {
	num    uint64
	size   uint64
	props  sstable.Properties
	reader *sstable.Reader
	refs   atomic.Int32
	db     *DB
}

func (f *fileMeta) smallestUser() []byte { return ikey.UserKey(f.props.Smallest) }
func (f *fileMeta) largestUser() []byte  { return ikey.UserKey(f.props.Largest) }

// containsUser reports whether the file's key range may hold user key k.
func (f *fileMeta) containsUser(k []byte) bool {
	return bytes.Compare(f.smallestUser(), k) <= 0 && bytes.Compare(k, f.largestUser()) <= 0
}

func (f *fileMeta) ref() { f.refs.Add(1) }

func (f *fileMeta) unref() {
	if f.refs.Add(-1) == 0 {
		f.reader.Close()
		if !f.db.closing.Load() {
			os.Remove(f.db.tablePath(f.num))
		}
	}
}

// version is an immutable snapshot of the LSM tree's file layout.
// levels[0] is ordered newest file first; levels[n>=1] hold
// non-overlapping files ordered by smallest key.
type version struct {
	levels [][]*fileMeta
	refs   atomic.Int32
}

func newVersion(nLevels int) *version {
	return &version{levels: make([][]*fileMeta, nLevels)}
}

func (v *version) ref() { v.refs.Add(1) }

func (v *version) unref() {
	if v.refs.Add(-1) == 0 {
		for _, lvl := range v.levels {
			for _, f := range lvl {
				f.unref()
			}
		}
	}
}

// clone returns a copy whose files are referenced by both versions.
func (v *version) clone() *version {
	nv := newVersion(len(v.levels))
	for i, lvl := range v.levels {
		nv.levels[i] = append([]*fileMeta(nil), lvl...)
		for _, f := range lvl {
			f.ref()
		}
	}
	return nv
}

// addFile inserts f into level n keeping the level's ordering invariant.
func (v *version) addFile(n int, f *fileMeta) {
	f.ref()
	if n == 0 {
		v.levels[0] = append([]*fileMeta{f}, v.levels[0]...)
		return
	}
	old := v.levels[n]
	i := sort.Search(len(old), func(i int) bool {
		return bytes.Compare(old[i].smallestUser(), f.smallestUser()) > 0
	})
	// Build a fresh slice: other versions may share old's backing array.
	lvl := make([]*fileMeta, 0, len(old)+1)
	lvl = append(lvl, old[:i]...)
	lvl = append(lvl, f)
	lvl = append(lvl, old[i:]...)
	v.levels[n] = lvl
}

// removeFile drops f from level n and releases the version's reference.
func (v *version) removeFile(n int, f *fileMeta) {
	lvl := v.levels[n]
	for i, g := range lvl {
		if g == f {
			v.levels[n] = append(lvl[:i:i], lvl[i+1:]...)
			f.unref()
			return
		}
	}
}

func (v *version) levelBytes(n int) int64 {
	var b int64
	for _, f := range v.levels[n] {
		b += int64(f.size)
	}
	return b
}

// overlapping returns the files in level n whose user-key range intersects
// [smallest, largest].
func (v *version) overlapping(n int, smallest, largest []byte) []*fileMeta {
	var out []*fileMeta
	for _, f := range v.levels[n] {
		if sstable.Overlaps(f.props, smallest, largest) {
			out = append(out, f)
		}
	}
	return out
}

// fileForKey returns the single file in level n >= 1 that may contain k.
func (v *version) fileForKey(n int, k []byte) *fileMeta {
	lvl := v.levels[n]
	// First file whose smallest key is > k, then step back one.
	i := sort.Search(len(lvl), func(i int) bool {
		return bytes.Compare(lvl[i].smallestUser(), k) > 0
	})
	if i == 0 {
		return nil
	}
	f := lvl[i-1]
	if bytes.Compare(k, f.largestUser()) > 0 {
		return nil
	}
	return f
}

// ---- manifest -------------------------------------------------------------

// The manifest is a JSON document rewritten atomically (write temp, fsync,
// rename, fsync directory) whenever the set of live tables changes. It is
// the authority on which .sst files are live; anything else in the
// directory is garbage left by a crash and is removed at Open.
type manifest struct {
	Format     int             `json:"format"`
	NextFile   uint64          `json:"next_file"`
	FlushedSeq uint64          `json:"flushed_seq"`
	Levels     []manifestLevel `json:"levels"`
}

type manifestLevel []manifestFile

type manifestFile struct {
	Num        uint64 `json:"num"`
	Size       uint64 `json:"size"`
	NumEntries uint64 `json:"entries"`
	MinSeq     uint64 `json:"min_seq"`
	MaxSeq     uint64 `json:"max_seq"`
	Smallest   []byte `json:"smallest"`
	Largest    []byte `json:"largest"`
	DataBytes  uint64 `json:"data_bytes"`
	NumBlocks  uint64 `json:"blocks"`
}

const manifestName = "MANIFEST"

func (db *DB) manifestPath() string { return filepath.Join(db.dir, manifestName) }

func (db *DB) tablePath(num uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", num))
}

// writeManifestLocked persists v. Caller holds db.mu.
func (db *DB) writeManifestLocked(v *version) error {
	m := manifest{Format: 1, NextFile: db.nextFile, FlushedSeq: db.flushedSeq, Levels: make([]manifestLevel, len(v.levels))}
	for i, lvl := range v.levels {
		m.Levels[i] = manifestLevel{}
		for _, f := range lvl {
			p := f.props
			m.Levels[i] = append(m.Levels[i], manifestFile{
				Num: f.num, Size: f.size, NumEntries: p.NumEntries, MinSeq: p.MinSeq, MaxSeq: p.MaxSeq,
				Smallest: p.Smallest, Largest: p.Largest, DataBytes: p.DataBytes, NumBlocks: p.NumBlocks,
			})
		}
	}
	data, err := json.Marshal(&m)
	if err != nil {
		return err
	}
	tmp := db.manifestPath() + ".tmp"
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
	if err := os.Rename(tmp, db.manifestPath()); err != nil {
		return err
	}
	return syncDir(db.dir)
}

// readManifest loads the manifest, returning an empty one if none exists.
func readManifest(path string, nLevels int) (manifest, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return manifest{Format: 1, NextFile: 1, Levels: make([]manifestLevel, nLevels)}, nil
	}
	if err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, fmt.Errorf("manifest: %w", err)
	}
	if m.NextFile == 0 {
		m.NextFile = 1
	}
	for len(m.Levels) < nLevels {
		m.Levels = append(m.Levels, manifestLevel{})
	}
	return m, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
