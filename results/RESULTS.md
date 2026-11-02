# Measured results

Every number below is read from a JSON file produced by a tool in `cmd/`; regenerate this file with `make results`.

## Storage engine (`make bench-storage` -> `results/storage.json`)

Hardware: Apple M5, 10 cores, 16 GB RAM, macOS 26.5.1 arm64, filesystem apfs, go1.27.2. Collected 2026-10-19T17:50:33Z.

Note: On macOS, Go's File.Sync issues fcntl(F_FULLFSYNC), which forces the drive to flush its cache; fsync costs here are higher than Linux ext4 fsync on the same hardware.

Workload: 16-byte keys, 100-byte values. Engine defaults: memtable 4 MiB, block 4 KiB, bloom 10 bits/key, L0 trigger 4, L1 10 MiB x10 per level, 2 MiB files. Group commit window 100 us.

| Metric | Value | JSON field |
|---|---|---|
| Durable write throughput (group commit, 64 clients) | 12,442 ops/s | write.latency.ops_per_sec |
| Durable write latency p50 / p99 | 4.0 ms / 9.9 ms | write.latency.p50_us, p99_us |
| Records per fsync during the write run | 48.2 | write.records_per_fsync |
| Point read throughput (8 clients, 432,124 keys, levels [0 0 27 0 0 0 0]) | 1,179,438 ops/s | read.latency.ops_per_sec |
| Point read latency p50 / p99 | 5 us / 51 us | read.latency.p50_us, p99_us |

### fsync per write vs group commit (8 s each)

| Mode | Clients | ops/s | p50 ms | p99 ms | records/fsync | fsync/s |
|---|---|---|---|---|---|---|
| fsync_per_write | 8 | 307 | 24.98 | 35.19 | 1.0 | 307 |
| group_commit | 1 | 318 | 3.01 | 4.14 | 1.0 | 318 |
| group_commit | 8 | 2,136 | 3.96 | 5.03 | 8.0 | 267 |
| group_commit | 64 | 15,546 | 3.99 | 8.04 | 62.4 | 249 |
| no_sync | 8 | 327,991 | 0.01 | 0.08 | 0.0 | 0 |

### Bloom filters: lookups for missing keys (300,000 keys in DB, 50,000 lookups, 10 bits/key)

| Filter | Levels | Block reads | Block reads / lookup | False positive rate | p50 us | p99 us | ops/s |
|---|---|---|---|---|---|---|---|
| with bloom | [3 5 9 0 0 0 0] | 430 | 0.009 | 0.0086 | 1 | 32 | 3,952,999 |
| without bloom | [3 5 9 0 0 0 0] | 49,999 | 1.000 | n/a | 4 | 152 | 779,971 |

Bloom filters cut disk block reads for missing keys by 116.3x (`bloom.disk_read_reduction_factor`).

### Write and space amplification (1,000,000 writes over 432,124 unique keys, no_sync)

| Metric | Value | JSON field |
|---|---|---|
| User bytes written | 110.6 MB | amplification.user_bytes_written |
| Live user bytes (unique keys) | 47.8 MB | amplification.live_user_bytes |
| WAL bytes | 130.7 MB | amplification.wal_bytes |
| Flush bytes | 119.0 MB | amplification.flush_bytes |
| Compaction bytes written (53 compactions, 10 trivial moves) | 355.9 MB | amplification.compaction_bytes_written |
| Write amplification, total (WAL + flush + compaction) | 5.47x | amplification.write_amplification_total |
| Write amplification, SSTables only | 4.29x | amplification.write_amplification_sst_only |
| Space amplification, steady state (levels [0 4 26 0 0 0 0]) | 1.24x | amplification.steady_state.space_amplification |
| Space amplification after full compaction (levels [0 0 27 0 0 0 0]) | 1.11x | amplification.after_full_compaction.space_amplification |

### WAL recovery

| Metric | Value | JSON field |
|---|---|---|
| WAL replayed | 578,524 records, 75.6 MB | recovery.wal_records, wal_bytes |
| Replay time (read + checksum + rebuild memtable) | 107 ms | recovery.replay_ms |
| Replay rate | 708 MB/s, 5,416,769 records/s | recovery.replay_mb_per_sec |
| Open() including flush of recovered data to L0 | 181 ms | recovery.open_ms_including_flush |

## Crash recovery harness (`make crash-test` -> `results/crash.json`)

Hardware: Apple M5, 10 cores, 16 GB RAM, macOS 26.5.1 arm64, filesystem apfs, go1.27.2. Collected 2026-11-02T07:12:35Z.

Note: On macOS, Go's File.Sync issues fcntl(F_FULLFSYNC), which forces the drive to flush its cache; fsync costs here are higher than Linux ext4 fsync on the same hardware.

Each trial starts a child process with 4 writer goroutines over 500 keys each (memtable 65536 bytes, L0 trigger 2, group_commit) and kills it with SIGKILL after a random delay (30% in [0,60), 45% in [60,400), 25% in [400,1500)). It adds a torn WAL tail in 1 in 4 trials, kills recovery 0 to 3 times in a row (0 to 29 after Open starts), then reopens the directory and verifies acknowledged writes, prefix consistency per writer and every value checksum. Seed 1.

SIGKILL leaves the OS page cache intact, so this harness cannot detect a missing fsync and says nothing about power loss.

| Metric | Value | JSON field |
|---|---|---|
| Trials (SIGKILL at a random point) | 500 | trials |
| Passed / failed | 500 / 0 | passed, failed |
| Acknowledged writes checked against the recovered state | 153,460 | total_acked_ops |
| Stored values checksum-validated | 864,464 | total_values_validated |
| Trials with data already compacted below L0 at the kill | 499 | trials_with_data_below_l0 |
| Trials killed before the first ack | 46 | trials_killed_before_first_ack |
| Recovery kills (total / during Open with WAL data present) | 692 / 318 | recovery_kills, recovery_kills_mid_open |
| Durable but unacknowledged writes seen (total / max per trial) | 1,358 / 4 | total_unacked_durable_ops, max_unacked_durable_ops |
| Torn WAL tails injected | 123 | torn_tails_injected |
| Torn tails seen truncated by the final Open (a recovery child truncates the rest) | 43 | trials_with_truncated_wal_tail |
| Wall time | 214 s | wall_seconds |

## Raft simulation (`make sim` -> `results/sim.json`)

Hardware: Apple M5, 10 cores, 16 GB RAM, macOS 26.5.1 arm64, filesystem apfs, go1.27.2. Collected 2026-11-02T07:10:09Z.

Note: On macOS, Go's File.Sync issues fcntl(F_FULLFSYNC), which forces the drive to flush its cache; fsync costs here are higher than Linux ext4 fsync on the same hardware.

Each schedule runs real Raft nodes and KV servers on a simulated network with a virtual clock, with simulated clients reading and writing, so one seed fixes the whole run and any failure replays exactly. After the faults stop, every schedule must elect a leader, bring every node up to date and end with identical state machines. The simulator stands in for both disks and models crashes during a save and power cuts, so this says nothing about real fsync behavior.

| Metric | Value | JSON field |
|---|---|---|
| Schedules (first seed 1) | 10,000 | schedules |
| Schedules that failed a check | 0 | violations |
| Messages sent | 20,796,528 | totals.messages_sent |
| Messages dropped | 791,134 | totals.messages_dropped |
| Messages duplicated | 334,495 | totals.messages_duplicated |
| Messages delayed past an election timeout | 79,115 | totals.messages_delayed_past_election_timeout |
| Messages reordered | 2,016,003 | totals.messages_reordered |
| Partitions (one-way cuts among them) | 29,513 (5,664) | totals.partitions, one_way_cuts |
| Crash-restarts (during a save / after a save, before sending) | 171,001 (16,129 / 15,981) | totals.crashes, crashes_during_save, crashes_after_save |
| Power-loss crashes | 85,704 | totals.power_loss_crashes |
| Leaders elected | 107,471 | totals.leaders_elected |
| Entries committed | 1,195,665 | totals.entries_committed |
| Writes acknowledged to clients | 974,920 | totals.writes_acknowledged |
| Retried writes dropped by a session | 571,155 | totals.retried_writes_dropped |
| Reads served through ReadIndex | 414,376 | totals.reads_served |
| Reads refused | 213,474 | totals.reads_refused |
| Log compactions | 118,667 | totals.log_compactions |
| Snapshots installed | 8,864 | totals.snapshots_installed |
| Wall time | 6 s | wall_seconds |

### Planted bugs: which check caught each one (schedules failed, out of 2,000)

| Bug | Checks that fired |
|---|---|
| send-before-save | durable-promise (1,750) |
| forget-voted-for | durable-promise (1,750) |
| count-old-terms | leader-completeness (23) |
| always-truncate | panic (1,502), commit-majority (132), leader-completeness (3), durable-promise (1), liveness (1) |
| commit-to-last-index | commit-bound (52) |
| no-up-to-date-check | leader-completeness (999), state-machine-safety (1) |
| commit-can-decrease | commit-monotonic (584) |
| fixed-timeout | liveness (332) |
| save-after-apply | durable-apply (1,770) |
| plain-leader-read | kv-linearizable (16) |
| read-skips-term-check | kv-linearizable (28) |
| no-dedup | kv-exactly-once (1,344) |
| sessions-not-in-snapshot | kv-divergence (101), kv-exactly-once (99) |
| session-outside-batch | kv-exactly-once (562), kv-divergence (291) |
| compact-without-flush | panic (278) |
| install-keeps-conflicting-log | state-machine-safety (10) |
| stale-snapshot-rolls-back | commit-monotonic (11), panic (3) |
| restore-position-lost | panic (110) |
| save-before-restore | panic (7) |

