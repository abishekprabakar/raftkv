# Acceptance criteria: fixes from the M0 audit

Committed before the fixes were written and before anything below was run.
The runner and its patches are committed after the code they apply to, and
before they run. `results/audit/m0-fixes/env.txt` records the commit of each
run.

## What changes

| ID | Change |
|---|---|
| F1 | `Get` and `Scan` load their snapshot after capturing the version (D5, D10) |
| F2 | Records too large for the WAL to replay are rejected before they are acknowledged (D7) |
| F3 | An exhausted iterator keeps returning false (D6) |
| F4 | Damage with a valid record after it is reported as corruption instead of being cut off as a torn tail, and tail truncation is made durable (D9). This was planned for M1. It moved here so the crash numbers below come from the final WAL code |
| F5 | The crash harness kills recovery while it has work to do, injects torn tails, and reads every ack before calling `cmd.Wait` (D3, D4) |
| F6 | Documentation corrections listed in `LEARNING.md` |

## T. Test suite on the fixed code

| ID | Command | Pass criterion |
|---|---|---|
| T1 | `gofmt -l cmd internal` | prints nothing |
| T2 | `go vet ./...` | exit 0 |
| T3 | `go test -count=1 ./...` | exit 0 |
| T4 | `go test -race -count=5 ./...` | exit 0 |

## R. Each regression test must fail without its fix

Each patch undoes one fix on a throwaway copy, then runs that fix's regression
tests. A nonzero exit is the expected result.

| ID | Patch | Tests | Pass criterion |
|---|---|---|---|
| R1 | `unfix-snapshot-order` | `TestGetSnapshotOrder`, `TestScanSnapshotOrder` | both fail |
| R2 | `unfix-oversize` | `TestOversizeRecordRejected` | fails |
| R3 | `unfix-iterator` | `TestExhaustedIteratorStaysExhausted` | fails |
| R4 | `unfix-wal-corruption` | `TestMidSegmentCorruptionIsAnError`, `TestMidSegmentCorruptionRefusesOpen` | both fail |

## H. Crash harness on the fixed code

| ID | Command | Seed | Pass criterion |
|---|---|---|---|
| H1 | `crashtest -trials 500 -seed 1 -out results/crash.json` | 1 | `passed == 500`, `failed == 0`, `recovery_kills_mid_open >= 100`, `torn_tails_injected >= 50` |
| H2 | `crashtest -trials 500 -seed 20261102 -out results/audit/m0-fixes/crash-seed20261102.json` | 20261102, never run before | same as H1 |

`recovery_kills_mid_open` counts kills where the WAL held data and `Open` had
started but not returned. If H1 or H2 misses one of the two minimums, that is a
failed criterion: it gets reported, and any new run uses a new seed.

H1 replaces `results/crash.json`, so the README shows the stricter test. The
2026-10-19 run stays in the initial commit, and the M0 reruns stay in
`results/audit/m0/`. The harness uses the seed differently now, so seed 1 does
not repeat the old kill delays.

## M. Does the harness catch what it now claims to?

All harness runs use 100 trials and seed 1. A prediction is reported whichever
way it falls.

| ID | Patch | Harness | Prediction |
|---|---|---|---|
| M1 | `recovery-drops-wal-early`: recovery deletes old WAL segments before flushing | old, from commit `d5eba3f` | passes: it never kills a recovery with work to do |
| M2 | same | new | fails: a kill between the delete and the flush loses acknowledged writes |
| M3 | `no-tail-truncation`: replay reports a torn tail but does not cut it off | new | fails: an injected tail becomes corruption in an older segment |
| M4 | `ack-before-durable` from `scripts/audit/m0/` | new | fails, as in D2 |

## V. Do the old headline numbers still hold?

The question is "500 trials, 151,604 acked writes, zero corruption". The answer
is decided by these rules, fixed now:

- "500 trials" holds if H1 and H2 each pass 500 of 500.
- "Zero corruption" holds if H1 and H2 report `failed == 0`.
- "151,604 acked writes" is the count from one run. Kill timing depends on the
  OS scheduler, so it cannot repeat exactly. It stays a true record of the
  2026-10-19 run, and the number to quote from now on is the H1 count.

## Left alone

The bloom benchmark shares one `rand.Rand` across goroutines, and `db.mu` is
held across manifest fsyncs, which stalls reads. Both affect benchmarks rather
than correctness. They are recorded here for M6, where the benchmarks are rerun.
