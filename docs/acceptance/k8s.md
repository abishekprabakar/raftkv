# Kubernetes on kind, deployed with Terraform: acceptance criteria

Committed before any of this code was written or run. The runner comes after the
code it runs, and is committed before it runs. `results/audit/k8s/env.txt` records
the commit of the run.

Everything here is local and free: a kind cluster inside Docker, created by
Terraform. No cloud account is used anywhere.

## Scope

- **Terraform does all of it.** `terraform apply` in `deploy/terraform` creates a
  kind cluster (the `tehcyx/kind` provider), builds the node image and loads it into
  the cluster, and deploys raftkv (the `hashicorp/kubernetes` provider).
  `terraform destroy` removes all of it. Provider versions are pinned exactly, the
  node image is pinned by digest, and `.terraform.lock.hcl` is committed with hashes
  for macOS and Linux.
- **A StatefulSet of 3 pods**, `raftkv-0` to `raftkv-2`, which are Raft nodes 1 to 3.
  Membership stays fixed, as in M4.
- **A headless Service** gives each pod a DNS name that survives restarts. Those
  names are the `--peers` list.
- **A PersistentVolumeClaim per pod** holds the Raft log and the LSM store (WAL and
  SSTables). A pod that is deleted comes back with the same name and the same volume.
- **A client Service** that only routes to pods that are ready.
- **A PodDisruptionBudget** that lets voluntary disruptions take one pod at a time.
- **Probes.** Liveness says the process is working. Readiness says the node is
  useful to the cluster right now, as defined below.
- **A GitHub Actions workflow** that runs apply, the checks and a short version of
  the measurements, and destroy. It needs no secrets.

Timing is unchanged from Docker Compose: a tick every 10 ms, election timeouts drawn
from 500 ms to 1 s, a heartbeat every 50 ms.

Out of scope: TLS and authentication, membership changes, leadership transfer before
a pod stops, more than one Kubernetes node, and any cloud provider.

## What ready means

`raftkvd --health ADDR` serves two HTTP endpoints for the kubelet.

**`/livez`** answers 200 when the node's event loop takes a request within 1 s. It
says nothing about the cluster. A node with no leader is alive, and restarting it
would not help.

**`/readyz`** answers 200 only when one of these holds:

- **Follower:** it follows a leader of the current term, and it has applied what that
  leader had committed as of the leader's latest message, give or take a small
  allowance (64 entries by default, one AppendEntries worth). A node that has just
  restarted and is still catching up is not ready.
- **Leader:** it completes a ReadIndex round within 1 s. That needs an entry
  committed in its own term and answers from a majority, so a leader cut off from the
  others is not ready.

Anything else is 503 with the reason in the body: a candidate, a follower that has not
heard from a leader, a follower that is behind, a leader without a majority.

## T. Build and tests

| ID | Command | Pass criterion |
|---|---|---|
| T1 | `gofmt -l cmd internal` | prints nothing |
| T2 | `go vet ./...` | exit 0 |
| T3 | `go test -count=1 ./...` | exit 0: every test that existed before still passes |
| T4 | `go test -race -count=1 ./...` | exit 0 |
| T5 | `go list -deps` of the Raft, KV and storage packages | no third-party packages |
| T6 | `go list -deps ./cmd/raftkvd ./cmd/raftkv ./cmd/kvworker` | the same modules allowed in M4 |
| T7 | `terraform fmt -check -recursive deploy/terraform` | exit 0 |
| T8 | `terraform validate` in `deploy/terraform` | exit 0 |

## H. Health endpoints, tested without Kubernetes

These run inside T3 and T4, on 127.0.0.1 with real files and real gRPC.

| ID | Test | Pass criterion |
|---|---|---|
| H1 | `TestLonelyNodeIsLiveButNotReady` | one node of three, started alone: `/livez` is 200 and `/readyz` is 503, and stays 503 |
| H2 | `TestClusterBecomesReady` | three nodes: every `/readyz` turns 200 after an election, and a key written through the leader reads back |
| H3 | `TestFollowerBehindIsNotReady` | the readiness rule, given a follower whose applied index trails the leader's commit index by more than the allowance, says not ready; within the allowance it says ready; with no leader it says not ready |
| H4 | `TestLeaderWithoutMajorityIsNotReady` | three nodes, then both followers stop: the leader's `/readyz` turns 503 while its `/livez` stays 200 |
| H5 | `TestIDFromHostname` | `raftkv-0` gives node 1 and `raftkv-2` gives node 3; a hostname with no number at the end is an error |

## A. One apply from nothing

Run by `scripts/audit/k8s/run.sh`, starting with no kind cluster and no Terraform state.

| ID | What | Pass criterion |
|---|---|---|
| A1 | `terraform apply -auto-approve` once | exits 0. When it returns, all 3 pods are Ready, and all 3 nodes name the same leader in the same term |
| A2 | the objects | a StatefulSet with 3 replicas; a headless Service (`clusterIP: None`) that publishes addresses before pods are ready; a client Service; a PodDisruptionBudget with `maxUnavailable: 1`; 3 PersistentVolumeClaims, all Bound; liveness, readiness and startup probes on the container |
| A3 | pins | `required_providers` uses exact versions, the kind node image has a digest, and `terraform init -lockfile=readonly` succeeds from the committed lock file |
| A4 | a write and a read through the client Service, from inside the cluster and from the host | both succeed, and the value reads back |

## K. Readiness and the disruption budget on the live cluster

| ID | What | Pass criterion |
|---|---|---|
| K1 | **Ready follows Raft, not the process.** `kubectl scale statefulset raftkv --replicas=1` leaves one node without a majority. Hold for 20 s, then scale back to 3 | within 15 s the lone pod is Running but not Ready, and its restart count is unchanged at the end of the hold: readiness failed and liveness did not. After scaling back, all 3 are Ready within 120 s, and a key written before the test reads back |
| K2 | **The budget holds a majority.** `kubectl drain` the node with `--pod-selector` for raftkv and a 30 s timeout. The node is cordoned, so an evicted pod can't come back until `kubectl uncordon` | drain evicts exactly one raftkv pod and then fails on the budget. Two pods stay Ready for the whole 30 s, and a write through the client Service succeeds while the node is drained. After `uncordon`, all 3 are Ready within 120 s |

K1 and K2 change the cluster with kubectl and put it back. They don't change anything
Terraform manages: after each, `terraform plan -detailed-exitcode` must report no
changes.

## The client load

F, R and U run under the same load, driven from the host by `cmd/kubebench`.

- **6 clients**, each built on the client library, each given only the client
  Service's address. They find the leader by following redirects. One operation at a
  time per client: append 60%, get 40%. Pauses between operations are 0 to 40 ms. An
  operation gets 4 s, and each attempt at one node 1 s, as in M5.
- **Keys.** 4 keys at a time, replaced by 4 new ones every 2 s (`e<epoch>.k<n>`), so
  values stay short however long a run lasts. Every appended value is unique.
- **History.** Each operation's call and return are stamped by one clock, in the
  harness process. A write the client gave up on has an unknown outcome and is kept
  with no return time. A read that failed is dropped.
- **How the host reaches the pods.** kind maps four host ports to NodePort Services:
  one for the client Service and one per pod. The client library is told how to turn
  a pod's DNS name into its host port. This adds a hop through Docker's port
  forwarding to every request.
- **Seed.** The acceptance runs use seed 1. Development uses seeds from 900,000,000
  up. A seed fixes each client's operations, not how real processes interleave.

A **failed client request** is an operation for which the client library returned an
error: no node completed it within its 4 s, after every retry and redirect the
library makes. Failed attempts inside a request that then succeeded are counted
separately, as retries.

## F. Leader failover

`kubebench failover -trials 100 -seed 1 -out results/k8s-failover.json`, on the
cluster A1 made. One trial:

1. Wait until all 3 pods are Ready and all 3 nodes name the same leader, L, in the
   same term, T. Then let the load run for 3 s.
2. Read the clock: that is t0. Start `kubectl delete pod <L> --wait=false`. This is
   a normal deletion: the kubelet sends SIGTERM and the node shuts down.
3. Each node's Status is polled about every 5 ms on its own connection, for the
   whole run.
4. Wait until the StatefulSet has replaced the pod, all 3 are Ready, and all 3 agree
   on a leader again. Then the next trial.

Two times are recorded per trial, both from t0, both on the harness's clock:

- **Time to a new leader:** to the first poll in which any node reports itself
  leader in a term above T.
- **Time to the first successful write:** to the acknowledgement of the first
  write, by any client, that was acknowledged in a term above T. Every write
  acknowledgement carries the leader's term, so a write the old leader acknowledged
  after t0, before it got its signal, doesn't count.

Both include the time Kubernetes takes to deliver the deletion to the process. The
first write also includes the client library's retry pauses, which grow to 500 ms,
so it is what a client of this library sees, not the first instant a write was
possible.

| ID | Pass criterion |
|---|---|
| F1 | 100 trials run. In every one, a new leader is seen within 60 s and the cluster settles (step 4) within 180 s. A trial that doesn't is counted and reported, never dropped or rerun |
| F2 | every replacement pod starts from its own volume: its first log line reports a term above 0 and a log that is not empty |

Reported, with no pass threshold: median, 95th percentile, minimum and maximum of
both times; the time from t0 to the old leader's last answer; the time until all 3
pods are Ready again; and how many extra leader changes happened while the deleted
pod rejoined. Percentiles use the nearest-rank method, as in M5.

## D. Durability and linearizability across those trials

After the last trial the load stops, and one client reads every key the run used.

| ID | Pass criterion |
|---|---|
| D1 | **Acknowledged writes lost: 0.** Every acknowledged append's value is in the final read of its key |
| D2 | **Duplicates: 0.** No appended value is in a final read more than once, whether its write was acknowledged or not |
| D3 | every final read succeeds, so D1 and D2 covered every key |
| D4 | Porcupine, with the model from M5, finds the whole history linearizable, final reads included, with no check timing out |

## R. Restarting every pod at once

F deletes one pod at a time, so the other two always hold the data. R is the test
that only the volumes can pass.

`kubebench restart -rounds 5 -seed 1 -out results/k8s-restart.json`: under the same
load, `kubectl delete pod` all three pods in one command, then wait for 3 Ready pods
and an agreed leader.

| ID | Pass criterion |
|---|---|
| R1 | all 5 rounds recover within 180 s |
| R2 | D1 to D4 hold for this run's history |

Reported: the time from the delete to the first successful write, per round.

## U. Rolling update under load

`kubebench rollout -rounds 10 -seed 1 -out results/k8s-rollout.json`. Two images are
built from the same source with different version strings, `v1` and `v2`. One round
runs `terraform apply -var image_tag=<the other tag>` while the load runs, which
changes one field of the StatefulSet's pod template and lets the StatefulSet
controller replace the pods one at a time, highest ordinal first.

| ID | Pass criterion |
|---|---|
| U1 | all 10 rounds finish: every pod runs the new image and reports the new version, all 3 are Ready, and the nodes agree on a leader |
| U2 | D1 to D4 hold for this run's history |
| U3 | **Failed client requests: target 0.** The count is reported whatever it is |

Reported per round and in total: requests made, failed requests, retries, the
longest a request took, leader changes during the rollout, and the **rollout time**:
from the first moment the StatefulSet shows the new revision to the first moment it
reports every replica updated and ready. The time `terraform apply` took is reported
beside it.

## C. Cold start

`kubebench coldstart -rounds 10 -out results/k8s-coldstart.json`. One round runs
`terraform destroy`, checks nothing is left, then reads the clock and starts
`terraform apply`. The **cold start time** runs from there to the first moment all 3
pods are Ready and all 3 nodes name the same leader. The time until `apply` itself
returns is reported beside it.

What "cold" covers: no cluster, no containers, no volumes, no Terraform state. What
it doesn't: the kind node image is already in Docker's image store and Docker's build
cache is warm. A first-ever run also downloads the node image and compiles the
binaries, and that depends on the network.

| ID | Pass criterion |
|---|---|
| C1 | all 10 rounds reach 3 Ready pods and an agreed leader, with one `apply` each |

Reported: median, minimum and maximum.

## P. The checks can fail

| ID | What | Pass criterion |
|---|---|---|
| P1 | `TestDurabilityCountsLostAndDuplicatedWrites` | given hand-written histories, the D1 and D2 counters report a lost acknowledged write and a duplicated write, and report nothing for a clean history with an unknown write |
| P2 | `kubebench failover -trials 20` on an image built with `scripts/audit/m3/no-dedup.patch` | D2 or D4 fails: a write retried across a failover is applied twice |
| P3 | `kubebench restart -rounds 3` with the data directory on an `emptyDir` instead of the volume claim | D1 fails: acknowledged writes are lost, or the cluster comes back empty |

P3 is why the volume claims matter: with them R must lose nothing, and without them
the same test must lose data.

## X. Destroy

| ID | What | Pass criterion |
|---|---|---|
| X1 | `terraform destroy -auto-approve` | exits 0, no container of the cluster is left, and the Terraform state is empty |

## CI

`.github/workflows/k8s.yml` runs on every push and pull request: `terraform apply`,
A1, A2, A4, K1, K2, 5 failover trials with D1 to D4, one rolling update, and
`terraform destroy`, which runs even if a step before it failed. It can't run before
the repository is pushed, so it is not part of this acceptance run. Its first run
gets reported when it happens.

## Hardware and clocks

One laptop runs everything: the kind node is a container in Docker Desktop's Linux
VM, and all three raftkv pods share it. Disk and network are that VM's, so these
numbers describe this setup and nothing larger. Every run records the hardware, and
the Docker, Kubernetes, kind node and Terraform versions. Long runs are wrapped in
`caffeinate`, because Go's clock on macOS stops while the machine sleeps.

## If something fails

A failed criterion gets reported, not rerun until it passes. A target that isn't met
is reported with the real number and an explanation. No test is weakened to pass. A
real bug gets a regression test and an entry in `LEARNING.md`, and any code change
means everything here runs again on the final commit.
