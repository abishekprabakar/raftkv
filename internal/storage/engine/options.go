package engine

import (
	"time"

	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

// Options configure a DB. Zero values select the defaults listed.
type Options struct {
	// SyncMode controls WAL durability. Default: wal.GroupCommit.
	SyncMode wal.SyncMode
	// GroupCommitWindow is how long the WAL syncer waits for concurrent
	// writers to join a batch once batching has started. Default 100us;
	// negative disables.
	GroupCommitWindow time.Duration
	// MemtableBytes is the size at which the memtable is frozen. Default 4 MiB.
	MemtableBytes int64
	// MaxImmutableMemtables is how many frozen memtables may queue before
	// writers stall. Default 2.
	MaxImmutableMemtables int
	// BlockBytes is the SSTable data block size. Default 4096.
	BlockBytes int
	// BloomBitsPerKey sizes the per-table bloom filter. Default 10.
	BloomBitsPerKey int
	// DisableBloomFilter turns filters off (used by benchmarks).
	DisableBloomFilter bool
	// L0CompactionTrigger is the L0 file count that schedules a compaction.
	// Default 4.
	L0CompactionTrigger int
	// L0StopWritesTrigger is the L0 file count at which writers stall.
	// Default 12.
	L0StopWritesTrigger int
	// BaseLevelBytes is the target size of L1; each deeper level is
	// LevelMultiplier times larger. Default 10 MiB.
	BaseLevelBytes int64
	// LevelMultiplier is the size ratio between adjacent levels. Default 10.
	LevelMultiplier int64
	// TargetFileBytes is the output file size for compactions. Default 2 MiB.
	TargetFileBytes int64
	// MaxLevels is the number of levels. Default 7.
	MaxLevels int
	// DisableAutoCompaction stops the background worker from compacting.
	// Flushes still happen. Compact() still works.
	DisableAutoCompaction bool
	// NoFlushOnClose leaves the memtable in the WAL at Close so the next
	// Open must replay it (used by the recovery benchmark).
	NoFlushOnClose bool
}

func (o Options) withDefaults() Options {
	def := func(v *int64, d int64) {
		if *v <= 0 {
			*v = d
		}
	}
	defi := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	if o.GroupCommitWindow == 0 {
		o.GroupCommitWindow = 100 * time.Microsecond
	} else if o.GroupCommitWindow < 0 {
		o.GroupCommitWindow = 0
	}
	def(&o.MemtableBytes, 4<<20)
	defi(&o.MaxImmutableMemtables, 2)
	defi(&o.BlockBytes, 4096)
	defi(&o.BloomBitsPerKey, 10)
	if o.DisableBloomFilter {
		o.BloomBitsPerKey = 0
	}
	defi(&o.L0CompactionTrigger, 4)
	defi(&o.L0StopWritesTrigger, 12)
	def(&o.BaseLevelBytes, 10<<20)
	def(&o.LevelMultiplier, 10)
	def(&o.TargetFileBytes, 2<<20)
	defi(&o.MaxLevels, 7)
	return o
}

// maxBytesForLevel returns the size target of level n >= 1.
func (o Options) maxBytesForLevel(n int) int64 {
	b := o.BaseLevelBytes
	for i := 1; i < n; i++ {
		b *= o.LevelMultiplier
	}
	return b
}
