# raftkv design

raftkv is a replicated key-value store written in Go with no external
storage or consensus libraries. Built so far: the single-node storage engine,
a log-structured merge tree in `internal/storage`, the Raft core in `internal/raft`,
and the replicated store that joins them in `internal/kv`. The real network and AWS
deployment come next.

## Package layout

```
raftkv/
  cmd/
    storagebench/      make bench-storage  -> results/storage.json
    crashtest/         make crash-test     -> results/crash.json
    results/           make results        -> results/RESULTS.md and the README block
  internal/
    hwinfo/            records CPU, memory, OS, filesystem for every results file
    kv/                replicated store: sessions, reads, snapshots, backends
    raft/              consensus: election, replication, commit, durable state
      sim/             deterministic simulator and its safety checks
    storage/
      ikey/            internal key encoding and ordering (user key + seq + kind)
      wal/             segmented write-ahead log, CRC32C per record, group commit
      memtable/        lock-free-read skiplist keyed by internal key
      sstable/         immutable sorted tables: blocks, sparse index, bloom filter
      engine/          the DB: write path, read path, flush, leveled compaction,
                       manifest, recovery, range iterators
  docs/                DESIGN.md (this file), NOTES.md, acceptance/
  results/             machine-generated JSON from the tools in cmd/
  scripts/audit/       runners and patches behind the acceptance checks
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

## Raft

`internal/raft` implements Figure 2 of the Raft paper by hand.

### Shape of the code

```
              Tick()      Receive(from, msg)      Propose(data)
                 \               |                   /
                  ▼              ▼                  ▼
            ┌─────────────────────────────────────────────┐
            │ Node: one call at a time, no goroutines     │
            │   handlers change memory and queue messages │
            │                                             │
            │ finish(), at the end of every call:         │
            │   1. Storage.Save(update)  fsync            │
            │   2. StateMachine.Apply(committed entries)  │
            │   3. Transport.Send(queued messages)        │
            └─────────────────────────────────────────────┘
```

A `Node` has no clock, goroutines or network of its own. The caller delivers
time, messages and commands one call at a time. Two things follow:

- **Determinism.** With the same seed and the same order of calls, a cluster does
  exactly the same thing, so M2 can replay any failure from a seed.
- **Persist before reply holds by construction.** Handlers never send. They queue
  messages, and `finish` saves before it sends. No handler can forget the rule.

The cost is that an fsync blocks the node. A real server can reduce this later by
batching several inputs into one call.

### Where Figure 2 lives

| Figure 2 | Code |
|---|---|
| Persistent state | `Node.currentTerm`, `votedFor`, `log`; saved by `finish` through `Storage` |
| Volatile state | `Node.commitIndex`, `lastApplied`, `nextIndex`, `matchIndex` |
| RequestVote receiver | `handleRequestVote`, `candidateLogUpToDate` |
| AppendEntries receiver | `handleAppendEntries` |
| All servers: apply committed entries | `applyCommitted` |
| All servers: higher term means follower | `Receive`, `stepDown` |
| Followers: election timeout | `Tick`, `startElection` |
| Candidates: win a majority | `handleRequestVoteReply`, `becomeLeader` |
| Leaders: heartbeats and replication | `broadcastAppend`, `sendAppend` |
| Leaders: back up on rejection | `handleAppendEntriesReply` |
| Leaders: advance the commit index | `advanceCommit` |

### Where the code goes beyond Figure 2

| Addition | Why |
|---|---|
| Requests and replies are separate one-way messages | A network can lose or reorder either half, and this is what M2 simulates |
| `AppendEntriesReply` carries `MatchIndex` | The leader can no longer read this from the request it sent |
| `ConflictTerm` and `ConflictIndex` | Back up a term at a time instead of an entry at a time (end of section 5.3) |
| A no-op entry when a leader takes over | Section 8: otherwise inherited entries may never commit |
| `max` around the follower's commit update | Reordered messages must not move `commitIndex` backwards |
| One `Update` per input | Term, vote and entries changed by one message cost one fsync and survive together |
| A failed `Save` stops the node for good | Its memory is ahead of its disk, so its promises cannot be trusted |
| Panics on truncating a committed entry, and on two leaders in one term | These can only be bugs, and carrying on would corrupt data |

### Durable state on disk

`FileStorage` uses the same `wal` package as the engine. Each `Update` is one WAL
record, so the checksum makes it all-or-nothing. Truncating the log is a record
too, and replay rebuilds the final state by applying records in order.

```
record := flags u8 | [term, votedFor uvarints] | [truncateFrom uvarint]
          | entry count uvarint | entry*
entry  := index uvarint | term uvarint | kind u8 | data length uvarint | data
```

A torn tail is dropped, which is safe because that `Save` never returned, so no
reply depended on it. Other damage stops the node from starting, because a node
that forgets a vote or an acknowledged entry can break safety.

### Simulation

`internal/raft/sim` runs real `Node`s on a simulated network. A single goroutine
pops events from a time-ordered queue, and every random choice comes from one
generator seeded by the schedule, so a seed fixes the whole run. `go run
./cmd/raftsim -first SEED -count 1 -trace` replays a failure event by event.

```
 seed ─► Schedule ─► event queue ─► tick / deliver / partition / crash / restart / propose
                                          │
                                          ▼
                    host: the node's disk, network link and state machine
                          (raft.Storage, raft.Transport, raft.StateMachine)
                                          │
                                          ▼
                    checks after every event, then a quiet phase for liveness
```

- **Crash points.** Because `Save` is synchronous, a crash between events can never
  land between a save and a send, which is exactly where ordering bugs live. So
  `host.Save` can also crash a node inside a call: before the update is durable,
  or after it is durable but before any message leaves.
- **Profiles.** A seed's remainder picks one of seven: `calm`, `lossy`, `partition`,
  `crash`, `chaos`, `figure8` and `solo`. Figure 8 needs a very particular history,
  so that profile hunts leaders: it crashes each one right after it accepts
  commands, and it sends few entries per message so old entries spread first.
- **Lockstep clocks.** Identical clocks are the worst case for election timeouts,
  because only randomization can break the tie, so many schedules use them.
- **Checks.** Five properties state what Raft guarantees: election safety, log
  matching, leader completeness, state machine safety and liveness. Monitors
  compare each step with Figure 2, and catch a broken rule even if no harm follows
  in that run. They can be switched off to ask whether a bug does visible damage.
- **History, not snapshots.** Properties are checked against everything seen so
  far, such as every (index, term) pair ever written, so later overwrites can't
  erase the evidence. A running hash of each log makes Log Matching O(1) per entry.

The simulated disk never lies about a completed `Save`, so simulation shows the
protocol is right given durable saves. It does not test `FileStorage` or real
fsyncs.

### Not implemented yet

Membership changes. Figure 13's chunked snapshots: a snapshot
travels as one message and is built in memory. Pre-vote and check-quorum, which are
not in Figure 2: a node returning from a partition with a high term forces an
election, and a partitioned leader keeps believing it leads until it hears a higher
term. Safety is unaffected, and ReadIndex covers the stale-leader read problem.

## Replicated KV

`internal/kv` is the state machine Raft drives.

```
 client ─► Server.Submit ─► raft.Propose ─► replicate ─► commit
                                                           │
                       one atomic engine batch ◄── Store.Apply
                       ├─ the data write
                       ├─ the client's session (highest Seq applied)
                       └─ the position (last index and term applied)

 client ─► Server.Read ─► raft.ReadIndex ─► majority confirms ─► Store.Get
```

**One durable record.** The engine runs without fsync on its own WAL, because Raft's
log already makes every entry durable before it can be applied. Writing the same
data durably twice would add a second fsync per write and nothing else. After a
power cut, the store's recorded position says how far it got, and Raft replays the
rest from its log.

That depends on one promise from the engine: a crash loses only the newest writes,
never one from the middle. So the WAL fsyncs each finished segment even in `NoSync`
mode, and `NoSync` replay stops at the first damage rather than skipping past it.

**Exactly-once writes.** A client that gets no answer can't know if its write
happened, so it retries under the same `(client, seq)`. The store keeps the highest
`seq` applied per client and drops repeats. The session update is in the same atomic
batch as the write, because a crash between them would apply the write twice. Keeping
sessions in the store also means they're replicated, snapshotted and identical on
every node, so a retry that lands on a new leader is still recognized. Sessions never
expire, so the table grows with the number of clients ever seen.

**Linearizable reads.** A leader can't trust its own belief that it leads: a newer
leader may exist behind a partition. See `internal/raft/read.go` for the ReadIndex
steps, `TestPlainLeaderReadIsStale` for the failure it prevents, and
`TestPartitionedLeaderRefusesStaleRead` for a cut-off leader refusing the read, step
by step on five nodes. Reads go through
the leader and cost one round of messages, but no fsync.

**Snapshots.** The store's durable contents are the snapshot, so Raft stores only a
marker of where its log starts. Compaction flushes the engine, then cuts the log.
That order matters: once the log is cut it can't rebuild anything the engine might
lose. On disk, a snapshot record holds the complete Raft state in a new WAL segment,
and older segments are then deleted, newest first, so a crash always leaves something
that replays.

Bytes are only built when a lagging follower needs them. The follower restores
before it saves the marker. If it crashes between the two, the state machine is ahead
of the log, and startup finishes the job. In the other order, the log would claim
entries the state machine never received. `Replace` builds a new engine generation
beside the old one and switches with an atomic rename, so a crash leaves one or the
other.

## Network and processes

`internal/server` runs one node as a real process, `cmd/raftkvd`. `internal/client`
and `cmd/raftkv` are the client side. The wire format is `internal/pb/raftkv.proto`.

```
            peers' streams              clients (gRPC)
                 │                            │
      Raft.Stream handler           KV.Write / Get / Status handlers
                 │ inbox (buffered)           │ calls (unbuffered)
                 └──────────────┬─────────────┘
                                ▼
   ticker ──► the loop: one goroutine, one select, one input at a time
                                │  kv.Server ─► raft.Node ─► FileStorage (fsync)
                                │                         └► Store ─► LSM
                                ▼ raft.Transport.Send (never blocks)
                 one queue + one goroutine + one stream per peer
```

**One goroutine owns the core.** `raft.Node` and `kv.Server` are not thread-safe on
purpose: that is what makes the simulator deterministic. The server keeps them that
way. gRPC handlers never call them; they hand a closure to the loop and wait for the
callback's answer on a buffered channel, so the loop never waits on a handler. The
code that runs in production is the code the simulator tested, with no locks added.

**Sending never blocks the loop.** `Send` drops the message into the peer's queue,
or drops it outright if the queue is full. Each peer has its own goroutine and one
long-lived client stream. A dead peer fills its queue and costs nothing else. Raft
already survives lost messages, so dropping is safe, and it bounds memory.

**Replies are messages.** The `Raft` service has a single client-streaming method.
Requests and replies are both one-way `Envelope`s, and a reply travels on the
replier's own stream. That matches `internal/raft`, which never pairs a reply with a
request.

**Redirects in the response body.** A node that isn't the leader answers `Write` and
`Get` with `NotLeader{leader_id, leader_address}`, where the address comes from the
fixed membership list. The client goes there at once. If nobody knows the leader, or
a node can't be reached, it moves on to the next address it knows, pausing 20 ms and
doubling up to 500 ms. Errors that mean "the outcome is unknown" are gRPC
`Unavailable` or `DeadlineExceeded`, and the client retries the write under the same
`(client_id, seq)`; the session table makes that safe. `InvalidArgument` is never
retried.

**Names are looked up on every dial.** Peers and clients dial with gRPC's
`passthrough` scheme, which hands the host name to the dialer each time, instead of
the default resolver, which caches an answer for up to 30 s. Containers change
address when they restart or reconnect, and two can swap: M5 caught node2 and node3
coming back on each other's addresses. A message that reaches the wrong node is
refused, because every envelope names its receiver; the sender then drops that
connection and dials again, since gRPC would otherwise keep reusing a connection
that is healthy but leads to the wrong node. A client drops a connection after an
attempt on it times out, for the same reason: a connection to a node that was cut
off can be dead without anyone having noticed.

**Timeouts that matter on a real network.** gRPC's default reconnect backoff grows
to two minutes, which would keep a restarted node cut off, so it is capped at one
second. A node killed outright never closes its TCP connections, so peers send
keepalive pings on idle connections every 10 s, the least gRPC allows, and on Linux
the 2 s keepalive timeout becomes `TCP_USER_TIMEOUT`, which closes a connection whose
data goes unacknowledged for 2 s. (M4 said pings went every 2 s. That was wrong:
gRPC silently raises any shorter interval to 10 s.) One message can be up to 256
MiB, because a snapshot travels whole.

**Docker Compose.** `compose.yaml` runs three nodes from one distroless image, each
with a named volume for its files. The CLI runs inside the Compose network
(`docker compose run --rm cli ...`), because the addresses in redirects are Compose
service names, which don't resolve on the host.

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
