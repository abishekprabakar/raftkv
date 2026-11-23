# raftkv

A replicated key-value store written from scratch in Go: a log-structured
merge-tree storage engine on every node, replicated with Raft, and deployed on
Kubernetes with Terraform. No external storage engine or consensus library is used.

Status: **the storage engine, Raft and the replicated KV store run as a real
cluster over gRPC**, on Docker Compose and on Kubernetes, with a CLI client that
follows leader redirects. Histories recorded while Docker cuts nodes off the network,
and while Kubernetes deletes and replaces pods, are checked for linearizability with
Porcupine. The Kubernetes cluster is a local kind cluster that one `terraform apply`
creates: no cloud account is involved. Throughput benchmarks of the cluster are still
to come.

## Architecture

```
                    client Put / Delete / Get / Scan
                                 │
                ┌────────────────▼─────────────────┐
                │           engine.DB              │
                │  seq++ ─► WAL append ─► memtable │  write path (one lock,
                │           │ group commit         │  WAL order == seq order)
                │           ▼                      │
                │   fsync per batch ─► ack writer  │
                │                                  │
                │  Get: memtable ─► frozen ─► L0*  │  read path, filtered by
                │        ─► L1 ─► L2 ─► ... ─► L6  │  key range + bloom filter
                └───────────┬──────────────────────┘
                            │ background worker
         frozen memtable ──flush──► L0 ──compact──► L1 ──compact──► L2 ...
          (one WAL segment       (overlapping)     (sorted runs, 10x per level)
           per memtable)

   on disk:  000001.wal  000002.wal ...      <- CRC32C-framed records
             000007.sst  000008.sst ...      <- blocks + sparse index + bloom + props
             MANIFEST                        <- live table set, atomically replaced
             LOCK
```

### On Kubernetes

```
 terraform apply  (deploy/terraform)
   ├─ kind_cluster ......... one Docker container that is a whole Kubernetes node
   ├─ docker build, kind_load   the raftkv image, copied into that node
   └─ kubernetes_* ......... everything in the box below

 namespace raftkv
 ┌─────────────────────────────────────────────────────────────────────────┐
 │  Service raftkv-client ─────► any pod that is READY                     │
 │                                                                         │
 │  StatefulSet raftkv: 3 pods, started together, updated one at a time    │
 │   ┌────────────┐      ┌────────────┐      ┌────────────┐                │
 │   │ raftkv-0   │◄────►│ raftkv-1   │◄────►│ raftkv-2   │  Raft nodes    │
 │   │ gRPC :7000 │      │ gRPC :7000 │      │ gRPC :7000 │  1, 2 and 3    │
 │   │ /livez     │      │ /livez     │      │ /livez     │  probed by     │
 │   │ /readyz    │      │ /readyz    │      │ /readyz    │  the kubelet   │
 │   └─────┬──────┘      └─────┬──────┘      └─────┬──────┘                │
 │   PVC data-raftkv-0   PVC data-raftkv-1   PVC data-raftkv-2             │
 │   (Raft log, WAL and SSTables; each outlives its pod)                   │
 │                                                                         │
 │  headless Service raftkv-peers: a DNS name per pod, the --peers list    │
 │  PodDisruptionBudget raftkv: at most 1 pod evicted at a time            │
 └─────────────────────────────────────────────────────────────────────────┘
 host: 127.0.0.1:17000 ► raftkv-client, :17001 to :17003 ► one pod each
```

- **StatefulSet, not Deployment.** A Raft node has an identity and a log that belong
  to it. A replaced pod gets the same name and the same volume back, so it returns as
  the same node.
- **Headless Service.** No load balancing, only a DNS name per pod that survives
  restarts. Those names are how the nodes find each other.
- **Readiness is Raft's answer, liveness is the process's.** A follower is ready when
  it follows a leader and has caught up; a leader when a majority confirms it. A node
  with no leader is alive and not ready, so it is taken out of the client Service and
  never restarted for it. A rolling update waits on readiness, so it never stops a
  second node while the first is still catching up.
- **PodDisruptionBudget.** A node drain may evict one raftkv pod and is refused the
  next, which always leaves a majority.
- **Terraform owns all of it.** One `apply` from nothing to three ready pods, one
  `destroy` back to nothing. Provider versions are exact, the node image is pinned by
  digest, and the lock file is committed.

Why each piece is the way it is: [docs/DESIGN.md](docs/DESIGN.md#kubernetes-deployment).

Full design, on-disk formats and tradeoffs: [docs/DESIGN.md](docs/DESIGN.md).
Failure modes, alternatives, and questions with answers: [docs/NOTES.md](docs/NOTES.md).

## Layout

| Path | What |
|---|---|
| `internal/server` | one node as a process: the single-goroutine loop, gRPC services, a stream per peer |
| `internal/client` | client library: follows leader redirects, retries writes under the same session |
| `internal/pb` | the wire format, `raftkv.proto`, and the code generated from it |
| `internal/chaos` | partition harness: seeded faults with `docker network disconnect`, client histories, the Porcupine model, failover read from logs |
| `internal/kube` | Kubernetes harness: deletes pods with kubectl, rolls images with terraform, times failover on one clock, counts lost writes |
| `internal/kv` | replicated store: sessions for exactly-once writes, ReadIndex reads, snapshots |
| `internal/raft` | Raft from Figure 2: election, replication, commit, durable state, snapshots, ReadIndex |
| `internal/raft/sim` | deterministic simulator: faults, crash points, clients and safety checks, all from one seed |
| `internal/storage/wal` | segmented write-ahead log, CRC32C per record, group commit with a straggler window |
| `internal/storage/memtable` | skiplist with lock-free readers and a single writer |
| `internal/storage/sstable` | immutable tables: 4 KiB checksummed blocks, sparse index, bloom filter, properties |
| `internal/storage/engine` | the DB: write/read paths, flush, leveled compaction, manifest, recovery, iterators |
| `internal/storage/ikey` | internal key encoding (user key, sequence, kind) |
| `cmd/raftkvd` | the node process |
| `cmd/raftkv` | the command-line client |
| `cmd/kvworker` | a client the partition harness runs inside each node's container |
| `cmd/chaos` | seeded partition schedules on Docker, checked with Porcupine -> `results/chaos.json` |
| `cmd/kubebench` | failover, restart, rolling update and cold start on Kubernetes -> `results/k8s-*.json` |
| `deploy/terraform` | a kind cluster and raftkv on it: StatefulSet, Services, volume claims, disruption budget, probes |
| `.github/workflows/k8s.yml` | CI: apply, the live checks, 5 failovers, one rolling update, destroy |
| `cmd/storagebench` | benchmarks -> `results/storage.json` |
| `cmd/crashtest` | SIGKILL crash-recovery harness -> `results/crash.json` |
| `cmd/raftsim` | seeded simulation schedules -> `results/sim.json` |
| `cmd/results` | renders `results/*.json` into `results/RESULTS.md` and this README |

## Running it

All commands run from the repository root.

```sh
make race            # unit tests under the race detector
make bench-storage   # storage benchmarks, writes results/storage.json (~3 min)
make crash-test      # 500 SIGKILL trials, writes results/crash.json (~4 min)
make sim             # 10,000 simulated Raft schedules, writes results/sim.json
make chaos           # 100 partition schedules on Docker, writes results/chaos.json (slow: a fresh cluster each)
make results         # regenerate results/RESULTS.md and the tables below
```

A 3-node cluster on Docker Compose:

```sh
docker compose up -d --build --wait               # start three nodes
docker compose run --rm cli put greeting hello    # the CLI runs inside the Compose network
docker compose run --rm cli --addr node2:7000 -v get greeting   # any node; -v shows the redirect
docker compose run --rm cli status                # each node's view: role, term, leader, indexes
docker compose kill node1                         # kill a node; if it led, writes move to a new leader
docker compose down -v                            # stop and delete the data
```

The same three nodes on Kubernetes. Terraform creates the cluster too, so there is
nothing to set up first beyond Docker:

```sh
make kube-up                                      # terraform apply: kind cluster, image, raftkv; returns when 3 pods are ready
export KUBECONFIG=deploy/terraform/kubeconfig     # this cluster only; ~/.kube/config is never touched
kubectl -n raftkv get pods,statefulset,pvc,pdb,svc
kubectl -n raftkv exec raftkv-0 -- raftkv --addr raftkv-client:7000 put greeting hello
kubectl -n raftkv exec raftkv-1 -- raftkv --addr raftkv-client:7000 -v get greeting   # -v shows the redirect to the leader
kubectl -n raftkv delete pod raftkv-0             # it comes back with the same name and the same volume
make kube-smoke                                   # the live checks, 5 leader failovers, one rolling update (what CI runs)
make kube-audit                                   # everything in docs/acceptance/k8s.md, writes results/k8s-*.json (~45 min)
make kube-down                                    # terraform destroy: nothing is left
```

Requirements: Go 1.27 or newer. Docker for either cluster. Kubernetes also needs
Terraform 1.9 or newer and `kubectl`; the kind command-line tool is not needed, since
the Terraform provider has kind built in. The `make` targets run Terraform through
`scripts/k8s/terraform.sh`, which hides the cluster's client key: a plain `terraform
destroy` prints it. `make proto` needs `protoc`; the generated
code is committed, so building doesn't.

## Results

<!-- results:start -->
_Generated from `results/*.json` by `go run ./cmd/results`; see [results/RESULTS.md](results/RESULTS.md) for hardware and methodology._

## Storage engine (`make bench-storage` -> `results/storage.json`)

| Metric | Value | Field in results/storage.json |
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

| Metric | Value | JSON field |
|---|---|---|
| Trials (SIGKILL at a random point) | 500 | trials |
| Passed / failed | 500 / 0 | passed, failed |
| Acknowledged writes checked against the recovered state | more than 150,000 per run; the exact count varies with OS scheduling | total_acked_ops |
| Stored values checksum-validated | 864,464 | total_values_validated |
| Trials with data already compacted below L0 at the kill | 499 | trials_with_data_below_l0 |
| Trials killed before the first ack | 46 | trials_killed_before_first_ack |
| Recovery kills (total / during Open with WAL data present) | 692 / 318 | recovery_kills, recovery_kills_mid_open |
| Durable but unacknowledged writes seen (total / max per trial) | 1,358 / 4 | total_unacked_durable_ops, max_unacked_durable_ops |
| Torn WAL tails injected | 123 | torn_tails_injected |
| Torn tails seen truncated by the final Open (a recovery child truncates the rest) | 43 | trials_with_truncated_wal_tail |
| Wall time | 214 s | wall_seconds |

## Raft simulation (`make sim` -> `results/sim.json`)

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

## Partitions on real containers (`cmd/chaos` -> `results/chaos.json`)

| Metric | Value | JSON field |
|---|---|---|
| Schedules (first seed 1) | 100 | runs |
| Histories Porcupine found not linearizable | 0 | violations |
| Checks that timed out / runs that broke | 0 / 0 | inconclusive, errors |
| Runs with no agreed leader 20 s after the final heal | 0 | not_recovered |
| Partitions: leader / follower / two nodes cut off | 150 / 58 / 39 | faults |
| Writes acknowledged / outcome unknown | 212,271 / 296 | totals.writes_* |
| Reads served / refused or timed out | 147,646 / 165 | totals.reads_* |
| Leader isolations measured / with no new leader before the heal | 150 / 0 | failover.measured, no_new_leader |
| Failover time: median / p95 / max | 584 ms / 874 ms / 1614 ms | failover.*_ms |
| Running time, not counting time the machine slept | 2754 s | wall_seconds |

With the `plain-leader-read` bug planted, Porcupine found 7 violations in 20 schedules (`results/audit/m5/planted-plain-leader-read.json`).

<!-- results:end -->
