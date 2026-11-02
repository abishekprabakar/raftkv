# raftkv design

raftkv is a replicated key-value store written in Go with no external
storage or consensus libraries. Phase 1 (this document's current scope) is
the single-node storage engine: a log-structured merge tree in
`internal/storage`. Later phases add Raft replication and AWS deployment.

## Package layout

```
raftkv/
  cmd/
    storagebench/      make bench-storage  -> results/storage.json
    crashtest/         make crash-test     -> results/crash.json
    results/           make results        -> results/RESULTS.md and the README block
  internal/
    hwinfo/            records CPU, memory, OS, filesystem for every results file
    storage/
      ikey/            internal key encoding and ordering (user key + seq + kind)
      wal/             segmented write-ahead log, CRC32C per record, group commit
      memtable/        lock-free-read skiplist keyed by internal key
      sstable/         immutable sorted tables: blocks, sparse index, bloom filter
      engine/          the DB: write path, read path, flush, leveled compaction,
                       manifest, recovery, range iterators
  docs/                DESIGN.md (this file), NOTES.md
  results/             machine-generated JSON from the tools in cmd/
```

Dependency direction is strictly downward: `engine` imports `wal`,
`memtable`, `sstable`, `ikey`; those four import only each other's leaves
(`ikey`) and the standard library.

## Architecture

```
            Put/Delete                                 Get / Scan
                │                                           │
                ▼                                           ▼
   ┌─────── writeMu ───────┐                 snapshot = visible seq
   │ seq++                 │                 ┌──────────────────────────┐
   │ WAL.Append(record) ───┼──► group-commit │ memtable (newest)        │
   │ memtable.Put(ikey)    │    syncer       │ frozen memtables         │
   └───────────────────────┘    (1 fsync per │ L0 tables, newest first  │
                │                batch)      │ L1..L6: one table/level  │
          wait durable                       └──────────────────────────┘
                │
     visible = max(visible, seq)

   background worker:  frozen memtable ──flush──► L0 ──compact──► L1 ──► ... ──► L6
```

### Write path

1. The writer takes `writeMu`, assigns the next sequence number, appends the
   record to the WAL (non-blocking enqueue) and inserts the internal key into
   the skiplist. Holding one lock across all three steps guarantees that WAL
   file order equals sequence order, which recovery relies on.
2. The writer releases `writeMu` and waits on the WAL future. The WAL's
   syncer goroutine drains every record queued since the last fsync, writes
   them with one `write(2)` and one `fsync`, and completes all their futures
   (group commit). When the previous batch held more than one record the
   syncer first waits a short window (100 us by default) so writers released
   by the last fsync can rejoin; without it, N closed-loop clients settle into
   alternating batches of k and N-k records and only half the possible
   amortisation is realised. A lone writer never pays the window.
3. On completion the writer advances `visible` to its sequence number with a
   CAS loop. Because any record with a lower sequence sits earlier in the
   WAL, it is durable too.

Reads use `visible` as a snapshot and ignore memtable entries with a higher
sequence number, so a value can never be observed before it is on disk. A read
captures the memtables and the version first and loads `visible` second, under
the same lock. Flush and compaction drop old versions based on `visible` when
they started, and it only moves forward, so this order guarantees that anything
dropped is shadowed by a version the read can see.

### Read path

`Get(key)` seeks `SearchKey(key, visible)` in the memtable, then in frozen
memtables newest first, then in L0 files newest first, then in exactly one
file per deeper level found by binary search on file key ranges. Each table
probe is first filtered by the file's key range and bloom filter. A
tombstone found at any stage ends the search with `ErrNotFound`.

`Scan(start, end)` builds a k-way merge over the same sources (a heap of
iterators ordered by internal key) and emits the newest version of each
user key whose sequence is at or below the snapshot, skipping tombstones and
older versions. The iterator holds a reference to the version it started
with, so compaction can neither delete its files nor change what it sees.

### Flush and compaction

When the memtable exceeds `MemtableBytes` (4 MiB default) the writer rotates
the WAL segment and freezes the memtable. A single background goroutine
writes frozen memtables to L0 tables and then runs compactions while any
level is over its target. Writers stall when more than
`MaxImmutableMemtables` frozen tables are queued or L0 has
`L0StopWritesTrigger` files, which bounds read amplification.

Compaction is **leveled**: L0 files overlap, every deeper level is one
sorted run of non-overlapping files, and level n+1 is 10x the size of level
n (L1 = 10 MiB by default). A compaction takes all of L0 (or one file of
level n chosen round-robin) plus the overlapping files of the next level,
merges them, and writes 2 MiB output files. If nothing overlaps, the file is
moved by editing the manifest only (trivial move). While merging:

- an older version is dropped if the newer version preceding it has a
  sequence number at or below the durable sequence at compaction start
  (so no reader could ever need it);
- a tombstone is dropped if no deeper level has a file whose range covers
  the key, because there is nothing left for it to hide;
- an output file is never split between two versions of one user key, so
  the "one file per level" lookup stays exact.

Why leveled over size-tiered: leveled keeps space amplification near 1.1x
and point reads at one file per level, at the price of higher write
amplification. Size-tiered writes less but can hold several overlapping runs
per tier, so reads must check all of them and disk usage can double during a
merge. For a replicated store where every node pays the disk and read cost,
leveled is the better fit; the measured amplification numbers are in
`results/storage.json`.

### Recovery

`Open` reads the manifest, opens every live table, deletes any `.sst` or
`.tmp` file the manifest does not list (left by a crash mid-compaction),
replays the WAL segments in order skipping records whose sequence is at or
below `flushed_seq`, flushes the rebuilt memtable to L0, deletes the old
segments and starts a fresh one. Damage in the newest segment with no valid
record after it is a torn write and is truncated. Any other damage is reported
as an error because no fsynced record may ever fail its checksum. Damage to
the very last record looks exactly like a torn write, so that one record is
cut off.

## On-disk formats

All integers are little-endian unless stated. Checksums are CRC32C
(Castagnoli).

### Internal key (`ikey`)

```
user key | 8-byte big-endian trailer = seq << 8 | kind      kind: 0 delete, 1 value
```

Ordering: user key ascending, then trailer descending, so the newest version
of a key sorts first. `SearchKey(user, seq)` uses kind 0xFF so it sorts just
before every real entry of that user key with sequence <= seq.

### WAL segment: `NNNNNNNNNNNNNNNNNNNN.wal`

```
record := len u32 | crc32c u32 (over payload) | payload
payload := kind u8 | seq u64 | keylen u32 | key | value
```

A segment is rotated when the memtable is frozen, so one segment corresponds
to one memtable. Segments are removed once their memtable is in an SSTable
recorded by the manifest.

### SSTable: `NNNNNN.sst`

```
data block*   entry := uvarint keylen | uvarint valuelen | internal key | value
              block ends when >= BlockBytes (4 KiB); each block + crc32c u32
index block   per data block: uvarint keylen | last internal key | uvarint offset | uvarint size   + crc
bloom block   k u8 | nbits u32 | bits                                                           + crc
props block   uvarint: entries, min seq, max seq, data bytes, blocks; bytes: smallest, largest + crc
footer (64 B) index off/len, bloom off/len, props off/len u64 each, reserved, magic 0x7261667472616b76
```

A point lookup reads the footer, index, bloom and props once at open, then
exactly one `pread` of one data block per probe. The bloom filter is built
over user keys with 10 bits per key and k = 6 probes (`int(10 * 0.69)`, the
same truncation LevelDB uses; the best value is about 6.9) using one 64-bit
hash split into two 32-bit halves (double hashing).

### Manifest: `MANIFEST`

JSON, rewritten atomically (write `MANIFEST.tmp`, fsync, rename, fsync
directory) whenever the set of live tables changes:

```json
{"format":1,"next_file":N,"flushed_seq":S,
 "levels":[[{"num":..,"size":..,"entries":..,"min_seq":..,"max_seq":..,
             "smallest":"<base64 ikey>","largest":"<base64 ikey>",
             "data_bytes":..,"blocks":..}, ...], [...], ...]}
```

`flushed_seq` is the highest sequence number covered by tables; WAL records
at or below it are skipped on replay. `LOCK` holds an flock so two processes
cannot open the same directory.

## Concurrency model

| Structure | Writers | Readers | Protection |
|---|---|---|---|
| sequence counter, WAL order | one at a time | - | `writeMu` |
| memtable skiplist | one (holder of `writeMu`) | many | atomic next pointers, publish bottom-up |
| frozen memtables, current version, manifest state | background worker | many | `db.mu` (held briefly) |
| SSTable files | written once | many | immutable; reference counts delay deletion |
| `visible` | all writers (CAS max) | all readers | atomic |

The background worker is a single goroutine, so flushes and compactions
never race each other and manifest writes are naturally serialized.

## Tradeoffs

- **Group commit vs fsync per write.** One fsync per record bounds write
  throughput to the fsync rate (a few hundred per second on macOS with
  F_FULLFSYNC). Group commit amortizes the fsync across concurrent writers at
  the cost of slightly higher latency for the first writer in a batch. The
  straggler window trades up to 100 us of latency for roughly twice the
  records per fsync under a fixed client count. The benchmark quantifies
  fsync-per-write, group commit at 1, 8 and 64 clients, and no-sync.
- **Visible-sequence reads.** Simpler than LevelDB's approach of inserting
  into the memtable only after the WAL sync; it keeps the critical section
  short and still never exposes non-durable data. Cost: one atomic load per
  read and an 8-byte trailer per memtable entry.
- **JSON manifest rewritten whole vs an append-only version-edit log.**
  Rewriting keeps the format trivially inspectable and makes the live-file
  set a single atomic rename. It costs O(files) bytes per flush, which is
  negligible at the file counts a 10 MiB-base leveled tree produces.
- **No block cache, no prefix compression, no block compression.** Each
  omission is a well-understood addition; leaving them out keeps the engine
  small enough to reason about and keeps the benchmarks honest about the
  core design. The page cache does most of the work a block cache would.
- **Single background worker.** Removes a class of races (two compactions
  touching one level) at the cost of flush latency when a compaction is
  running. The `MaxImmutableMemtables` stall is the backpressure valve.
