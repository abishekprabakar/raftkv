# Learning notes

One section per milestone, newest first: what was done, the ideas behind it, and
the questions worth being able to answer about it. Every number comes from a real
run. Criteria are in `docs/acceptance/`, and the runs are in `results/audit/`.

## M1: the Raft core

Criteria: `docs/acceptance/M1.md`. Results: `results/audit/m1/`. Code:
`internal/raft`.

### What was built

The core of Raft, by hand from Figure 2 of the paper, with no consensus library
and no third-party packages: leader election with randomized timeouts, log
replication, commit advancement, and a durable term, vote and log. It is not a
service yet. It has no real network and no client API, and it is not connected to
the storage engine. It runs only in tests.

### Raft in plain language

A cluster wants to agree on one ordered list of commands, even when machines die
and messages get lost. Raft does this with one leader at a time.

- **Terms** are numbered election periods, like a logical clock. A message from a
  newer term tells a node it is out of date, whatever it thought it was doing.
- **Election.** A follower that hears nothing for a while assumes the leader is
  dead, moves to a new term and asks for votes. Each node gets one vote per term,
  so at most one node can win a majority in a term.
- **Replication.** The leader appends a command to its log and sends it out. Each
  message includes the entry just before the new ones, and a follower accepts
  only if it has that entry. This is what keeps logs identical.
- **Commit.** Once a majority stores an entry, it is committed and can never be
  lost, because any future leader needs votes from a majority, and those two
  majorities must overlap in at least one node.

### Each rule, and what goes wrong without it

Each rule below was broken on purpose. The test named in `docs/acceptance/M1.md`
failed all 9 times.

| Rule | Without it |
|---|---|
| Save before replying | A crash forgets a promise already made, so a node can vote twice |
| `votedFor` is durable | A restarted node votes for a second candidate, and a term gets two leaders |
| Vote only for an up-to-date log | A node missing committed entries wins and erases them |
| Commit by counting only current-term entries (Figure 8) | An old entry on a majority gets committed, then a later leader overwrites it |
| Truncate only on a real conflict | A delayed duplicate message erases newer entries, possibly committed ones |
| Follower commit stops at the last new entry | A follower commits stale entries that only sit at the right position |
| Commit index never goes backwards | A reordered message un-commits entries |
| Random election timeouts | Everyone times out together and splits the vote. With fixed timeouts, the cluster tests never elected a leader |
| Apply only what is saved | The state machine runs ahead of what the node can recover |

### Design choices worth explaining

**A node is a plain state machine.** It has no goroutines, clock or network. Time
arrives through `Tick`, messages through `Receive`, and commands through
`Propose`. That makes a whole cluster deterministic, so M2 can replay any failure
from its seed. A mutex-and-goroutines design, the usual 6.5840 style, can't do
that.

**One exit.** Handlers never send anything. Every call ends in `finish`, which
saves, then applies, then sends. "Persist before reply" is then one fact about
one function, not a rule to remember in every handler.

**One save per input.** A message that changes the term, the vote and the log
costs one fsync, and all of it survives or none of it does.

**One-way messages.** Figure 2 describes RPCs, but here a request and its reply
are unrelated messages, because a network can lose or reorder each one. So a reply
has to carry `MatchIndex`: the leader can't rely on remembering what it asked.

**A no-op at the start of each term.** Figure 8 stops a leader committing old
entries by counting, so without a new entry from its own term, inherited entries
could stay uncommitted forever.

### Results

| Check | Result |
|---|---|
| gofmt, go vet | clean |
| Tests, normal and 5 repeats under the race detector | pass |
| Third-party packages | none |
| Raft test functions | 51, covering 93.1% of statements |
| Cluster runs | 605 across fixed seeds, all pass |
| Rules broken on purpose | 9 of 9 caught by their named test |

### What these tests do not show yet

The cluster tests are gentle. They isolate a node and restart nodes, but they
never drop, delay or duplicate a message. With each of the 9 bugs added, only 2
made a cluster test fail: `always-truncate` and `fixed-timeout`. The other 7 were
caught only by single-node tests that set up the exact scenario by hand. So
nothing yet shows the whole protocol staying safe under real fault patterns. M2
is built to show that, and those bugs are its first targets.

Other limits:

- An fsync blocks the node, and followers learn the commit index only on the next
  message. Throughput is unmeasured until M6.
- The log and its file only grow until M3 adds snapshots.
- No pre-vote: a node back from a partition with a high term forces an election.
- A partitioned leader keeps believing it leads until it hears a higher term. That
  is why a plain leader read is unsafe, which M3 deals with.
- `FileStorage` assumes one process per directory and doesn't lock it yet. M4 runs
  real processes, so it needs that lock.

### Questions and answers

**Q: Walk me through a leader election.**
A follower's timer runs out. It adds one to its term, votes for itself, saves that,
and asks everyone. A node grants its vote if it hasn't voted this term and the
candidate's log is at least as up to date as its own. A majority makes a leader,
which appends a no-op and starts heartbeats. A newer term, or a leader in the
current term, sends a candidate back to follower.

**Q: Why are the timeouts randomized?**
So one node usually times out first and wins before others start. When I fixed the
timeouts, every cluster test failed with no leader elected.

**Q: Why must `currentTerm` and `votedFor` be saved before replying?**
A reply is a promise. If a node voted, crashed and forgot, it could vote again in
the same term, and two candidates could each reach a majority. The term matters
too: a node that forgets it could accept messages from a deposed leader.

**Q: Why isn't `commitIndex` saved?**
It can be rebuilt. A restarted node starts at 0 and learns it from the leader, or a
new leader works it out again. Committed entries are safe because they are on a
majority, whatever any one node remembers.

**Q: What does the Log Matching property say, and how is it enforced?**
If two logs have an entry with the same index and term, the logs are identical up
to that point. A leader creates at most one entry per index in a term, and a
follower accepts new entries only if it holds the entry just before them, so
agreement extends one step at a time.

**Q: Explain Figure 8.**
An old-term entry can sit on a majority and still be replaced. A candidate whose
last entry has a newer term can win without that entry, because the log comparison
uses the last term first. So a leader commits by counting only entries from its
own term. Committing one commits everything before it, because logs match up to
that point.

**Q: Why not truncate everything after `PrevLogIndex`?**
A delayed duplicate may carry fewer entries than the log now holds, and the extra
entries could be committed. Only a real term conflict at an index proves the tail
is stale.

**Q: Figure 2 says `commitIndex = min(leaderCommit, last new index)`. Is that
enough?**
Not with reordering. A delayed message can have a last new index below the current
commit index, which would move it backwards, so the code keeps the larger value.
I have a test for that exact case.

**Q: What happens if the disk fails on a save?**
The node stops for good. Its memory is ahead of its disk, so anything it said next
could be a promise it can't keep. Nothing leaves it after that.

**Q: How is the log stored, given entries can be truncated?**
As an append-only WAL. A truncation is one more record, and replay applies records
in order. Each input is one checksummed record, so it survives whole or not at all.

**Q: Why write your own instead of using etcd/raft?**
To learn it and be able to defend every line. In production I would use a library
with years of testing behind it.

## M0 fixes: the audit's bugs, and a stricter crash test

Criteria: `docs/acceptance/M0-fixes.md`. Results: `results/audit/m0-fixes/` and
`results/crash.json`. The corrections listed under M0 below are now in the docs.

### What changed

- `Get` and `Scan` capture the files first and load the snapshot second.
- A record too big for the WAL to replay is rejected before it is acknowledged.
- An exhausted iterator stays exhausted.
- Damage with a valid record after it now refuses to open, instead of being cut
  off as a torn tail. Tail truncation is fsynced.
- The crash harness now kills recovery while it has work to do, adds torn WAL
  tails, reads every ack before `cmd.Wait`, and fails if a child exits by itself.

Each regression test was checked by undoing its fix: all of them failed without
the fix and pass with it.

### Do the old crash numbers still hold?

| Claim | Verdict | Evidence |
|---|---|---|
| 500 trials | Holds | 500 of 500 on seed 1 and on seed 20261102, on the stricter test |
| Zero corruption | Holds | 0 failures on both seeds |
| 151,604 acked writes | True of that one run, not repeatable | The count depends on OS scheduling. Seed 1 now gives 155,462, and seed 20261102 gives 170,535 |

The number to quote from now on is 155,462 (`results/crash.json`). The old
harness never killed a recovery with work to do. This one did so 321 times on
seed 1 and 414 times on seed 20261102, and it added 123 and 138 torn tails.

The harness still cannot detect a missing fsync, so do not claim power-loss
durability from it.

### Proof the stricter harness is stricter

A bug was added on purpose: recovery deletes the old WAL files before their data
reaches a table, so a crash in that gap loses acknowledged writes.

| Harness | Result |
|---|---|
| Old | Passed 100 of 100: it never saw the bug |
| New | Failed on trial 2 |

Removing tail truncation made the new harness fail on trial 6. A WAL segment with
a torn tail stopped being the newest one after a kill, so it turned into
corruption that recovery correctly refused to open. Acknowledging before the WAL
write still fails on trial 2.

### Questions and answers

**Q: Your crash test passed before and after you made it stricter. What did
making it stricter achieve?**
It now catches a whole class of recovery bugs it could not see. I showed that by
adding one such bug: the old harness passed it 100 of 100 times and the new one
failed it on trial 2. A passing test only means something when it can fail.

**Q: Why does the torn tail need to be truncated at all? It gets skipped anyway.**
Only while its segment is the newest one. Recovery starts a new segment, so if it
crashes before deleting the old ones, the torn tail is now in an older segment,
where damage means real corruption and the database refuses to open. Truncating
first keeps that from happening.

**Q: How does the WAL tell a torn write from corruption?**
Records reach the file in order, so a write that was cut off cannot have a valid
record after it. If one exists, the damage is in data that reached the disk. The
exception is the very last record: damage there looks the same as an unfinished
write, so it gets cut off.

## M0: audit of the storage engine

### What happened

The Phase 1 engine got its first commit, then an audit. The pass criteria,
seeds and predictions were committed before anything ran. The engine's code
was not changed.

| Check | Result |
|---|---|
| gofmt, go vet | clean |
| Unit tests, normal and race detector | pass, including 5 repeats under the race detector |
| Coverage | engine 84.2%, ikey 87.5%, memtable 93.6%, sstable 78.2%, wal 81.2% |
| Crash harness, seed 1 | 500/500 passed, 166,511 acked ops, 0 torn WAL tails |
| Crash harness, seed 20261101 | 500/500 passed, 154,813 acked ops, 0 torn WAL tails |
| README and RESULTS.md | regenerated byte for byte from the JSON |

The audit then asked a harder question: what does a passing run actually
prove? Those experiments ran on throwaway patched copies.

| Experiment | Result | What it means |
|---|---|---|
| D1: turn fsync off | harness still passes 100/100 | it cannot detect a missing fsync |
| D2: ack before the write reaches the WAL | fails on trial 2, in 0.3 s | it does catch lost acknowledged writes |
| D3: WAL bytes waiting when each child starts | 0 in 100 of 100 trials | it never kills a recovery that has work to do |
| D4: ack pipe closed before the reader finished | 0 of 100 trials | `cmd.Wait` runs too early, but no harm was seen |
| D5a: pause `Get` at the wrong moment | `ErrNotFound` for a key that always existed | real read bug |
| D5b: 60 s stress, no injected pause | 633 not-found and 10 stale out of 331,762,580 reads | the bug happens on its own under load |
| D6: `Next` on a finished iterator | panic | small API bug |
| D7: 65 MiB value | `Put` succeeds, key gone after reopen | acknowledged data lost |
| D8: bloom filter probes | 6, while the docs say 7 | docs are wrong |
| D9: bit flip early in the newest WAL segment | `Open` succeeds, 0 of 100 keys left | silent data loss |
| D10: D5 again with the snapshot load moved | 0 bad reads out of 307,570,031 | diagnosis confirmed |

### The ideas in plain language

**SIGKILL versus power loss.** When a process is killed, the data it already
handed to the operating system stays in the OS's memory and still reaches the
disk later. `fsync` matters only when the whole machine dies. So a kill test
checks that the program never acknowledges a write before handing it to the
OS. It says nothing about whether the program called `fsync`. D1 shows this
directly. The honest claim is "no acknowledged write lost across 500 process
kills", not "survives power loss".

**A test you have never seen fail proves little.** D1 and D2 are small
versions of mutation testing: break the code on purpose and see if the test
notices. D2 shows the harness has teeth. D1 shows exactly where they stop.

**The `Get` bug.** A read does two steps: (1) pick a snapshot, meaning "I can
see writes up to sequence number S", and (2) grab the current list of files.
A flush between those steps can break the read.

1. Key `k` has versions at sequence 1 and 2. `Get` picks snapshot S = 2.
2. Before `Get` grabs the file list, a write adds version 3 and a flush runs.
3. The flush knows version 3 is durable, so it throws away versions 1 and 2.
4. `Get` now looks for a version at or below 2. None exists, so it reports
   the key missing, or falls through to an older value in a deeper level.

The fix swaps the steps: grab the files first, then pick the snapshot. Any
flush that built those files used a snapshot no newer than the one read
afterwards, because `visible` only moves forward. So whatever it threw away
was already hidden by a version this reader can see. D10 confirms it: 0 bad
reads with the steps swapped, and the only change was the order of those lines.

The race detector could not catch this. It finds memory touched without
synchronization, and every access here is synchronized. The steps are safe
one at a time and wrong in that order.

**Reading D5b's rate correctly.** The workload was built to provoke the bug:
one key, a 4 KiB memtable, 16 readers on 2 CPUs. The 643 bad reads prove the
bug can happen. They do not predict how often it would happen on a real system.

**Torn tail versus corruption.** A crash can leave half a record at the end of
the newest WAL file. That record was never acknowledged, so cutting it off is
right. A flipped bit in the middle of the file looks the same to the current
code, so it cuts everything after that point too, including acknowledged
records (D9). This matters for Raft, where a node that forgets a log entry it
promised to keep can break safety.

**What a seed does and does not fix.** The harness seed fixes the kill delays
and the operation stream. It does not fix how far the child gets before each
kill, because the OS scheduler decides that. Seed 1 gave 151,604 acked ops on
2026-10-19 and 166,511 this time. M2 removes this gap with a simulated network
and clock, so one seed replays exactly.

### Corrections to earlier notes

- `docs/NOTES.md` says the harness verifies that a write is durable
  before it is acknowledged. It verifies that the write reached the OS.
- `cmd/crashtest` and the README say recovery itself is crashed. D3 shows it
  is not: the parent recovers and closes the directory cleanly before each
  child starts, so those early kills hit a start-up with nothing to recover.
- The torn-tail path has been hit 0 times by the harness in all runs. Only unit
  tests cover it.
- `docs/DESIGN.md` and `docs/NOTES.md` say k = 7. The code uses 6.
- `docs/NOTES.md` lists DynamoDB's storage layer as an LSM tree. An
  AWS primer describes DynamoDB storage replicas as a WAL plus a B-tree. Name
  RocksDB and Cassandra instead.
- `README.md` says `make race` runs `go vet`. It only runs the tests under the
  race detector.
- The bloom benchmark's comment says each lookup lands inside every table's key
  range. The data shows about 1 filter check per lookup (49,997 for 50,000), so
  the reduction factor is fair but the no-filter case is the easy one.

What the two runs support saying: "500 SIGKILL trials on two seeds with no
acknowledged write lost and no corrupt value."

### Gaps M3 will need closed

- No atomic multi-key write. M3 must store a key, the applied index and the
  client session table together.
- No way to export or load a whole database for install-snapshot, beyond
  reading it through `Scan`.
- The WAL cannot remove entries from its end, which a Raft log must do when a
  follower overwrites conflicting entries.

### Questions and answers

**Q: Your crash test passes with fsync turned off. What is it good for?**
It proves the code never acknowledges a write before handing it to the OS, and
that recovery rebuilds a correct state through flushes and compactions. It
cannot test power loss, because killing a process leaves the page cache alive.
Testing that needs a fake disk that drops unsynced writes, or real power cuts.

**Q: How do you know the test can fail at all?**
I broke the code on purpose. When writes were acknowledged before reaching the
WAL, the harness failed on trial 2.

**Q: Explain a real bug you found.**
Tell the `Get` story above, in this order: symptom, the two steps, why the
flush can drop the version, the swap, and the D10 result.

**Q: Why did the race detector and 500 crash trials miss it?**
The race detector looks for unsynchronized memory access, and there is none
here. The harness only reads after recovery, with no writes or flushes running.
Unit tests never overwrite a key while reading it during a flush.

**Q: Why does loading the snapshot second fix it?**
`visible` never decreases. A flush reads it before it builds files, and a
reader reads it after grabbing those files, so the reader's snapshot is at
least as new as the flush's. Everything the flush dropped was already shadowed
by a version the reader can see.

**Q: What is the difference between a torn write and corruption, and does your
code tell them apart?**
A torn write is an unfinished record at the end of the log and was never
acknowledged. Corruption damages data that was. Today the code treats any bad
record in the newest segment as a torn tail, so it can drop acknowledged data
without an error. That is a known gap.

**Q: What does a fixed seed give you?**
Here it fixes the delays and operations, not the OS scheduling, so counts vary
between runs. Full replay needs simulated time and a simulated network.

**Q: Why are there 6 bloom probes instead of 7?**
The best k at 10 bits per key is about 6.9. The code truncates `10 * 0.69` to 6.
The measured false positive rate was 0.86% (`results/storage.json`).

**Q: Why commit the criteria before running?**
So the result cannot quietly change what counts as passing. It also made me
write predictions down, which is why D5b stood out.
