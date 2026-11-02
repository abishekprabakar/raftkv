# Acceptance criteria: the scripted stale-read test

Committed before the test was written or run. In M3, random schedules caught the
`plain-leader-read` bug in only 16 of 2,000 runs (`results/audit/m3/q-plain-leader-read.json`),
because it needs a cut-off leader, a write on the other side and a read that lands
on the stale node, all at once. This test builds that case by hand, so the bug fails
every time.

`TestPartitionedLeaderRefusesStaleRead` in `internal/kv/staleread_test.go` runs five
real `kv.Server`s. Only the node a step names is ticked, and messages are delivered
oldest first, except that a message between the two sides of a partition is lost.
No randomness can affect the outcome.

| Step | What happens | What the test checks |
|---|---|---|
| (a) | S1 wins term 1 and commits `k=old` on all five | the client's write is acknowledged, and every node has applied it |
| (b) | The network splits into {S1, S2} and {S3, S4, S5}. S3 wins term 2 with votes from S4 and S5, and commits `k=new` on its side | the write is acknowledged. S1 still believes it leads term 1, and its store still holds `old`, so a plain read would return it |
| (c) | A client reads `k` through S1. S1 sends a round of messages, S2 answers it, and the other three never hear it | the read is not served, because 2 of 5 is not a majority |
| (d) | Only S1's clock runs, until the read expires | the read fails with `ErrRetry` and returns no value |
| (e) | The network heals and S3 sends a heartbeat | S1 steps down to follower in term 2 and names S3 as leader. A read through S3 returns `new` |

Five nodes rather than three, so the cut-off leader still has one follower answering
it. That checks a majority is counted, not just "someone answered".

| ID | Command | Pass criterion |
|---|---|---|
| SR1 | `go test -count=100 -run '^TestPartitionedLeaderRefusesStaleRead$' ./internal/kv` | passes all 100 runs |
| SR2 | the same on a copy with `scripts/audit/m3/plain-leader-read.patch` | fails all 100 runs |
| SR3 | `gofmt -l internal`, `go vet ./internal/kv`, `go test -count=1 ./internal/kv` | clean, and every test passes |

SR2 also has to show real harm, not just a broken rule: every failure must be S1
returning `old` in step (c), after `new` was acknowledged.

For the record, with no pass criterion: the runner also runs the existing
`TestReadIndexRefusesDeposedLeader` 100 times on the patched copy. M3 checked that
patch only against `TestSimSchedules`, so how the scripted M3 test behaves with it
was never measured.

`scripts/audit/stale-read/run.sh` runs all of this and writes
`results/audit/stale-read/`.
