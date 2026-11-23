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

## Partitions on real containers (`cmd/chaos` -> `results/chaos.json`)

Hardware: Apple M5, 10 cores, 16 GB RAM, macOS 26.5.1 arm64, filesystem apfs, go1.27.2. Collected 2026-11-02T22:11:16Z.

Note: On macOS, Go's File.Sync issues fcntl(F_FULLFSYNC), which forces the drive to flush its cache; fsync costs here are higher than Linux ext4 fsync on the same hardware.

Each schedule starts a fresh 3-node cluster on Docker Compose (Docker 29.7.2), runs 6 clients for 20 s while `docker network disconnect` cuts off the leader, a follower or two nodes, heals, and then checks the whole history with Porcupine. Clients run inside the nodes' containers, so a cut-off node still gets requests. The seed fixes the faults and every client's operations, not how real processes interleave. Failover time runs from Docker's disconnect event for the leader to the first log line in which another node becomes leader, both on the Docker VM's clock.

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

## Kubernetes on kind, deployed with Terraform (`make kube-audit` -> `results/k8s-*.json`)

Hardware: Apple M5, 10 cores, 16 GB RAM, macOS 26.5.1 arm64, filesystem apfs, go1.27.2. Collected 2026-11-23T20:14:41Z.

Note: The pods and their volumes are inside Docker's Linux VM. The filesystem named here is the host's, where only the harness runs.

Versions: Docker 29.7.2 with 10 CPUs and 7.7 GB for its VM, Kubernetes v1.37.0 (`kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`), kubectl v1.36.1, Terraform 1.17.0. Source commit `04d32c1e07049fc10d205bb6124d3e109837d4ee`.

One `terraform apply` creates a one-node kind cluster inside Docker and deploys a StatefulSet of 3 raftkv pods, each with its own volume claim. All three pods share one Kubernetes node, one VM and one disk, so these numbers describe this laptop setup and nothing larger. `cmd/kubebench` runs on the host and reaches the pods through ports kind maps: it starts every `kubectl` and `terraform` command itself, polls each node's Status over gRPC (the measured interval is below), and runs 6 clients that append and read on 4 keys, replaced every 2 s. Every time is read from that one process's clock. A request fails when the client library gives up on it after 4 s of redirects and retries.

### Leader failover: `kubectl delete pod` on the leader, under load

Each trial waits for 3 Ready pods and an agreed leader, runs the load for 3 s, then deletes the leader's pod with a normal `kubectl delete pod --wait=false`. Both times start just before kubectl is started. The time to a new leader ends at the first poll in which any node reports itself leader in a higher term. The time to the first successful write ends when the first write is acknowledged in a higher term, which every acknowledgement carries. It includes the client library's retry pauses, which double up to 500 ms with no jitter.

| Metric | Value | JSON field |
|---|---|---|
| Leader failover trials (`kubectl delete pod` on the leader, under load) | 100 | k8s-failover: summary.trials |
| Trials that elected a new leader, took a write, and settled with 3 Ready pods | 100 of 100 | summary.passed |
| Time to a new leader: median / p95 / max | 645 ms / 924 ms / 1033 ms | summary.new_leader_ms |
| Time to the first successful write: median / p95 / max | 674 ms / 1170 ms / 1203 ms | summary.first_write_ms |
| Acknowledged writes lost | 0 of 83,600 | durability.acknowledged_writes_lost |
| Writes applied twice / values nobody wrote | 0 / 0 | durability.writes_duplicated, values_never_written |
| Porcupine on the whole history (139,431 requests, 1,204 keys) | Ok | linearizable |
| Failed client requests across the 100 failovers | 0 of 139,431 | load.failed_requests |
| Replacement pods that started from their own volume | 100 of 100 | summary.replacement_did_not_start_from_its_volume |
| Elections that finished in one term | 94 of 100 | summary.elected_in_one_term |
| Time to a new leader: minimum | 528 ms | summary.new_leader_ms.min |
| Time to the first successful write: minimum | 657 ms | summary.first_write_ms.min |
| First write after the new leader was seen: median / max | 119 ms / 489 ms | summary.first_write_after_new_leader_ms |
| Trials whose first write took over 1 s | 44 of 100 | summary.first_write_took_over_1s |
| The old leader's last Status answer after the delete: median / max | 33 ms / 70 ms | summary.old_leader_last_answer_ms |
| `kubectl delete --wait=false` returned: median | 30 ms | summary.kubectl_delete_returned_ms |
| All 3 pods Ready again: median / p95 / max | 2873 ms / 3077 ms / 3891 ms | summary.all_pods_ready_ms |
| Longest stretch with no write acknowledged, per trial: median / max | 649 ms / 1154 ms | summary.longest_write_gap_ms |
| Trials in which the rejoining pod forced another election | 0 of 100 | summary.trials_with_extra_leader_changes |
| Requests that needed a retry / retries in all | 600 / 3,272 | load.requests_that_retried, retries |
| Writes with an unknown outcome | 0 | durability.writes_unknown |
| Status poll interval per node: median / p99 | 5.5 ms / 8.0 ms | status_poll_interval_ms |
| Running time | 602 s | wall_seconds |

### Every pod deleted at once

A failover leaves two nodes holding the data, so it would pass even without volumes. Here one `kubectl delete pod` removes all three pods, and what comes back can only come from the volume claims.

| Metric | Value | JSON field |
|---|---|---|
| Restarts of every pod at once that recovered, each pod from its volume | 5 of 5 | k8s-restart: summary.passed |
| Acknowledged writes lost across those restarts | 0 of 2,807 | durability.acknowledged_writes_lost |
| Writes applied twice / Porcupine | 0 / Ok | durability.writes_duplicated, linearizable |
| Time from the delete to a leader: median / max | 2203 ms / 2371 ms | summary.new_leader_ms |
| Time from the delete to the first successful write: median / max | 2219 ms / 3186 ms | summary.first_write_ms |
| All 3 pods Ready again: median / max | 2893 ms / 2982 ms | summary.all_pods_ready_ms |
| Failed client requests while no pod was up | 1 of 4,612 | load.failed_requests |

### Rolling update under load

Each round runs `terraform apply -var image_tag=...` to switch between two images built from the same source with different version strings. The StatefulSet controller replaces one pod at a time, highest ordinal first, and waits for each to be ready. The rollout time runs from the first watch event showing the new image in the StatefulSet to the first showing every replica updated and ready. The client numbers cover each round from that first event until the cluster is healthy again.

| Metric | Value | JSON field |
|---|---|---|
| Rolling updates under load that finished on the new version | 10 of 10 | k8s-rollout: summary.passed |
| Rollout time: median / min / max | 8.8 s / 8.3 s / 9.5 s | summary.rollout_seconds |
| Failed client requests during the rollouts | 0 of 36,924 | summary.failed_requests |
| Longest any request took during a rollout | 2630 ms | summary.longest_request_ms |
| Leader changes per rollout: median / max | 2 / 3 | summary.leader_changes_per_rollout |
| Requests that needed a retry / retries in all | 132 / 717 | summary.requests_that_retried, retries |
| Terms passed per rollout, counting elections nobody won: median / max | 2 / 5 | summary.terms_passed_per_rollout |
| Longest stretch with no write acknowledged, per rollout: median / max | 1134 ms / 2633 ms | summary.longest_write_gap_ms |
| `terraform apply` for the change: median / max | 18.6 s / 21.9 s | summary.terraform_apply_seconds |
| Acknowledged writes lost over the whole run | 0 of 32,442 | durability.acknowledged_writes_lost |
| Writes applied twice / Porcupine | 0 / Ok | durability.writes_duplicated, linearizable |

### Cold start

Each round runs `terraform destroy`, checks that no node container and no Terraform state are left, and then times one `terraform apply`. The clock stops at the first moment all 3 pods are Ready and all 3 nodes name the same leader. The kind node image was already in Docker's image store and Docker's build cache was warm, so this leaves out a first download of the node image and a first compile.

| Metric | Value | JSON field |
|---|---|---|
| Cold starts that reached 3 Ready pods and a leader with one `terraform apply` | 10 of 10 | k8s-coldstart: summary.passed |
| Cold start, `terraform apply` to 3 Ready pods with a leader: median / min / max | 34.8 s / 33.7 s / 37.4 s | summary.ready_with_leader_seconds |
| kind cluster created and its API server answering: median | 26.6 s | summary.api_server_answered_seconds |
| All 3 pods created: median | 28.0 s | summary.pods_created_seconds |
| `terraform apply` returned: median / max | 35.5 s / 44.3 s | summary.terraform_apply_seconds |
| `terraform destroy`: median / max | 14.1 s / 14.5 s | summary.terraform_destroy_seconds |

### Live checks

| Check | Result | What happened |
|---|---|---|
| A1 | pass | 3 of 3 pods Ready; every node names raftkv-2 (node 3) leader in term 1 |
| A2 | pass | StatefulSet: 3 replicas, Parallel start, RollingUpdate, 3 probes; headless Service raftkv-peers publishes not-ready addresses; client Service raftkv-client; budget maxUnavailable 1, 1 disruption allowed; 3 claims Bound |
| A4 | pass | put and get through raftkv-client.raftkv.svc.cluster.local:7000 from two pods, and through 127.0.0.1:17000 from the host |
| K1 | pass | raftkv-0 alone: not Ready 3.2 s after the scale-down, still Running with 0 restarts after 20s. The kubelet's last probe failure: Readiness probe failed: HTTP probe failed with statuscode: 503. Scaled back: 3 Ready, and the key written before reads back |
| K1-plan | pass | terraform plan -detailed-exitcode exited 0 |
| K2 | pass | the drain evicted raftkv-0 and was refused the other two for 30s ("Cannot evict pod as it would violate the pod's disruption budget"); 2 pods stayed Ready and a write succeeded meanwhile; after uncordon, 3 Ready |
| K2-plan | pass | terraform plan -detailed-exitcode exited 0 |

With the `no-dedup` bug planted, 20 failover trials left 10 writes applied twice, and Porcupine's answer was Illegal (`results/audit/k8s/planted-no-dedup.json`).

With the data on an `emptyDir` instead of the volume claims, deleting every pod 3 times lost 21,599 of 31,495 acknowledged writes (`results/audit/k8s/planted-emptydir.json`).

