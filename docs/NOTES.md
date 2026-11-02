# Notes on the storage engine: decisions, failure modes, questions

These notes explain why each design decision was made, what the
alternatives were, how things fail, and how to answer the questions this
project is likely to prompt. Numbers are deliberately absent; quote them
from `results/storage.json` and `results/crash.json`. Raft questions and each
milestone's notes are in `LEARNING.md`.

## Why an LSM tree at all

Point lookups and range scans on a B-tree are great, but every write is a
random in-place page update, and durable random writes are the slowest thing
a disk does. An LSM tree turns every write into a sequential append (WAL)
plus an in-memory insert, and defers the sorting work to background
compaction, which rewrites data in large sequential batches. The price is
read amplification (several places to look) and write amplification
(rewriting data as it moves down levels). This is the design behind LevelDB,
RocksDB and Cassandra. DynamoDB is not an example: AWS describes its storage
replicas as a write-ahead log plus a B-tree.

## Design decisions and alternatives

### WAL

- **CRC32C per record, length-prefixed framing.** Detects torn writes and bit
  rot. Castagnoli is hardware accelerated on x86 and arm64. The CRC covers
  the payload only; a corrupt length is caught by the bounds check and the
  CRC of whatever follows.
- **Truncate a torn tail, error on anything else.** After a crash, only the
  last segment can have a partial record (older segments were fully synced
  before rotation). A torn write is by definition the end of the data, so if a
  valid record follows the damage, the damage is in data that reached the disk
  and Open refuses to continue. The one blind spot is the very last record: a
  flipped bit there looks the same as a write that never finished.
- **Group commit with a dedicated syncer goroutine** instead of
  leader/follower batching on the write lock: simpler to reason about, no
  lock hand-off, and writers are never blocked behind fsync while holding
  the write lock.
- **Straggler window.** With N closed-loop clients and no window, the syncer
  grabs whatever is queued the instant an fsync finishes, before the k
  writers it just released have re-queued, so batches alternate k and N-k
  and average N/2. Waiting 100 us (3% of a 3 ms F_FULLFSYNC) when the last
  batch had more than one record lets batches fill to N. MySQL exposes the
  same knob as `binlog_group_commit_sync_delay`. Single writers skip it.
- Alternative considered: **one WAL per memtable vs one global WAL.** We
  rotate the segment exactly when the memtable is frozen, so segment ==
  memtable and WAL garbage collection is a one-line delete after flush.

### Memtable

- **Skiplist** (LevelDB/RocksDB default). O(log n) insert and seek, trivially
  iterable in order, and supports lock-free concurrent readers with a single
  writer because nodes are published bottom-up with atomic stores and never
  mutated.
- Alternatives: a **red-black tree** needs a lock for readers or a
  copy-on-write scheme; a **sorted array/vector** has O(n) inserts; a
  **hash index + unsorted log** cannot serve range scans or flush in order.

### Internal keys and sequence numbers

- Every write gets a monotonically increasing sequence number; memtable and
  SSTable entries store `(user key, seq, kind)` and sort newest-first within
  a key. This gives: multi-version reads (snapshot = sequence number),
  deterministic dedup during compaction, idempotent WAL replay, and a clean
  way to hide not-yet-durable writes from readers.
- Alternative: keying by user key only with in-place memtable updates. That
  cannot express snapshots and forces the WAL fsync inside the write lock.

### SSTable

- **4 KiB data blocks with a sparse index** (one entry per block) rather than
  a dense index: the index for a 2 MiB file is a few hundred entries, lives
  in memory, and a lookup costs one block read.
- **Bloom filter per file over user keys, 10 bits/key, k=6.** Turns most
  "key not in this file" questions into a memory lookup. Critical for
  leveled compaction where a miss would otherwise read one block per level.
  Alternatives: ribbon/xor filters (smaller, more complex), partitioned
  filters for very large files (unnecessary at 2 MiB).
- **Checksummed blocks.** A flipped bit in a data block is detected on read
  rather than returned as a wrong value.
- Omitted on purpose: prefix compression with restart points, block
  compression, a block cache. Each is a contained follow-up.

### Leveled compaction

- Chosen over size-tiered because reads check at most one file per level
  past L0 and steady-state space amplification is ~1.1x. Size-tiered has
  lower write amplification but worse read and space amplification; it fits
  write-heavy, scan-light workloads (Cassandra's default for a long time).
- **Trivial move**: a file with nothing to merge against changes level
  without I/O. Common right after a flush when L1 is empty.
- **Tombstone dropping** only when no deeper level could hold an older
  version of the key. Dropping early would resurrect deleted data.
- **Never split two versions of one user key across output files**,
  otherwise the "one file per level contains the key" invariant breaks.
- **Dedup across versions uses the durable sequence at compaction start**:
  an older version is dropped only if the newer one is visible to every
  possible reader. Readers pin the version (file set) they started with, so
  they never observe a compaction's output mid-read.

### Manifest and recovery

- JSON manifest, atomically replaced. The manifest is the only authority on
  which `.sst` files are live; anything else is garbage and deleted on open.
  This is what makes a crash in the middle of compaction harmless.
- `flushed_seq` lets replay skip records already in tables, so a crash after
  the manifest write but before the WAL segment delete does not duplicate
  data.
- Recovered data is flushed immediately so the next crash recovers from
  tables, not from a growing WAL.

## Failure modes and how they are handled

| Failure | Consequence | Mechanism |
|---|---|---|
| Process killed mid-write | partial record at WAL tail | CRC/length check, tail truncation |
| Killed after WAL fsync, before memtable insert | no inconsistency: record is in WAL, replay applies it | WAL is the source of truth |
| Killed mid-flush | orphan `.sst` with no manifest entry | deleted at next open; WAL segment still present |
| Killed after new table written, before manifest | same as above | same |
| Killed after manifest write, before WAL segment delete | WAL and table both have the data | replay skips `seq <= flushed_seq` |
| Killed mid-compaction | orphan outputs, inputs still live in manifest | orphans deleted; compaction simply re-runs |
| Killed during recovery (replay or flush) | partial flush orphan | same orphan logic; replay is idempotent |
| Bit flip in a WAL record | checksum mismatch | valid record after it: refuse to open; last record: cut off as a torn write |
| Bit flip in an SSTable block | checksum mismatch on read | read returns an error rather than bad data |
| Two processes open the same directory | would corrupt | `flock` on `LOCK` |
| Writers outrun compaction | unbounded L0 would make reads slow | stall when frozen memtables or L0 files exceed thresholds |
| fsync acknowledged but power lost (consumer SSD lying) | not defensible in software | document; F_FULLFSYNC on macOS mitigates |

What the SIGKILL harness does **not** test: anything that depends on the page
cache being lost. A killed process's completed `write(2)` calls survive, so the
harness passes even with fsync turned off, and it says nothing about power
loss. The harness adds torn WAL tails itself because SIGKILL never produces one.
Sector reordering below the OS is covered only by the checksum unit tests that
corrupt files directly, and only a power-cut test would exercise it end to end.

## What the crash harness found

The first 15-trial run failed on trial 8: a `Get` returned a value from op
189 for a key whose op 284 had been acknowledged, while a full `Scan` of the
same database returned the right value. Scan and Get disagreeing pointed at
the one place they differ: the order in which L0 files are searched. The
manifest stores L0 newest-first, but the recovery code added files with the
same prepend operation a flush uses, which reversed the order after every
reopen. Get then found the oldest L0 version first and returned it; Scan was
unaffected because the merge orders by sequence number inside each key. The
fix sorts L0 by file number (flushes are serialised, so a higher number is
always newer) and a regression test reopens a database with three L0
versions of each key. Lesson worth telling: the bug was invisible to every
unit test because it needed a reopen with more than one L0 file, and the
harness hit it within seconds because it crashes and reopens hundreds of
times with tiny memtables.

## Questions and answers

**Q: Walk me through what happens on `Put`.**
Lock the writer mutex, assign the next sequence number, encode the record,
enqueue it in the WAL, insert `(key, seq, value)` into the skiplist, unlock.
Wait for the WAL syncer to fsync the batch that includes the record. Advance
the visible sequence. Return. Readers that started before the fsync cannot
see the value because their snapshot sequence is lower.

**Q: Why is holding the lock across WAL append and memtable insert
important?** It makes WAL order equal sequence order. Recovery replays in
file order; if two writers could interleave, a replayed older write could
clobber a newer one. The sequence number in each record would still let us
fix it, but the invariant is cheaper than the fix.

**Q: How do you know a write is durable before you acknowledge it?** The
syncer calls `fsync` (`F_FULLFSYNC` on macOS) after the `write`, and only
then completes the futures. That comes from reading the code, not from the
harness. SIGKILL leaves the page cache alive, so the harness proves a write is
not acknowledged before it reaches the OS, and it still passes with fsync
turned off. Proving the fsync itself needs a fake disk that drops unsynced
writes, or real power cuts.

**Q: What is group commit and why does it help?** Many writers' records are
written with one `write` and one `fsync`. Fsync latency is the bottleneck
for durable writes; batching N records per fsync gives up to N times the
throughput at roughly the same per-write latency. The benchmark shows
fsync/s staying flat while ops/s scales with the client count.

**Q: Why does throughput still scale with client count under group commit?
Isn't fsync the limit?** Fsync is the limit on batches per second, not
records per second. Each batch carries every record queued while the
previous fsync ran, so more concurrent writers means bigger batches. A
single writer gets exactly one record per fsync and no benefit.

**Q: How does a read find the newest value?** Internal keys sort newest
first within a user key. Seek to `SearchKey(key, snapshot)`; the first entry
with that user key is the newest visible version. Check memtable, frozen
memtables, L0 newest-first, then one file per level. Stop at the first hit;
a tombstone is a hit meaning "absent".

**Q: Why can L0 files overlap but L1+ cannot?** L0 files are whole
memtables flushed as-is, so they cover arbitrary ranges. Compacting L0 into
L1 merges them with the overlapping L1 files and writes a sorted run.
Keeping deeper levels non-overlapping is what lets a lookup touch one file
per level.

**Q: What is write amplification and where does it come from here?** Bytes
physically written divided by bytes the user wrote. Each byte is written to
the WAL once, flushed once, and rewritten roughly once per level it passes
through, plus a share of the next level's data it is merged with (up to the
level ratio, 10x, in the worst case). The benchmark reports the measured
value on a specific workload.

**Q: Space amplification?** SSTable bytes on disk divided by live user
bytes. Comes from stale versions not yet compacted away and from
tombstones. Leveled compaction keeps it low because only L0 and the
not-yet-merged part of the newest level hold duplicates.

**Q: How do bloom filters change read cost for missing keys?** Without them
a miss reads one block from every level whose key range covers the key.
With them, a miss costs a memory probe per file and a block read only on a
false positive (about 1% at 10 bits/key). The benchmark measures block reads
per lookup with and without.

**Q: What happens if the process dies in the middle of compaction?** The
new files are not in the manifest, so on restart they are deleted and the
input files are still live. The compaction runs again. The manifest rename
is the atomic commit point.

**Q: Why rewrite the manifest instead of appending version edits like
LevelDB?** Simplicity and inspectability. It costs a few KiB per flush at
this scale. A version-edit log is the right call when there are tens of
thousands of files.

**Q: How do iterators stay consistent while compaction runs?** They take a
reference on the current version and on the memtables. Reference counts on
files mean a file is deleted only when no version, and so no iterator, can
reach it. The snapshot sequence hides writes that happened after the
iterator was created.

**Q: What would you change for production?** A block cache with
charged memory accounting; prefix-compressed blocks with restart points;
compression per block; partitioned bloom filters; rate-limited compaction
I/O; a version-edit manifest; and checksums over the whole file at open
time for paranoia mode.

**Q: How did you test it?** Unit tests per package including corruption
injection (bit flips, truncated tails); a randomized model test that checks
Get and Scan against a map through flushes, compactions and reopen; a
concurrency test under the race detector; and the SIGKILL harness that kills
a child process at random points hundreds of times, kills recovery while it has
work to do, adds torn WAL tails, and checks every acknowledged write and every
stored checksum. I also broke the code on purpose to confirm the tests can fail.
