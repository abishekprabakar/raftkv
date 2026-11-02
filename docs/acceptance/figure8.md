# Acceptance criteria: the scripted Figure 8 test

Committed before the test was written or run. In M2, random schedules hit the
`count-old-terms` bug in only 30 of 2,000 runs. This test builds the exact history
from Figure 8 of the Raft paper by hand, so that bug fails every time.

`TestFigure8` in `internal/raft/figure8_test.go` runs real `Node`s. It delivers
each message by hand, in a fixed order, and no randomness can affect the outcome.
It asserts the log on every server's disk after each step:

| Step | S1 | S2 | S3 | S4 | S5 | What happens |
|---|---|---|---|---|---|---|
| (a) | 1 2 | 1 2 | 1 | 1 | 1 | S1 leads term 2 and copies index 2 to S2 only |
| (b) | 1 2 | 1 2 | 1 | 1 | 1 3 | S1 crashes. S5 wins term 3 with votes from S3 and S4, and puts a different entry at index 2 |
| (c) | 1 2 4 | 1 2 | 1 2 | 1 | 1 3 | S5 crashes. S1 restarts, wins term 4, and copies index 2 to S3. The term 2 entry is on a majority but must not be committed |
| (d) | | 1 3 5 | 1 3 5 | 1 3 5 | 1 3 5 | S1 crashes. S5 wins term 5 with votes from S2, S3 and S4, and overwrites index 2. That is safe only because the entry was never committed |
| (e) | 1 2 4 | 1 2 4 | 1 2 4 | 1 | 1 3 | The other branch from (c): S1 gets its term 4 entry onto a majority, which commits everything through index 3, and then S5 can't win |

Each number is the term of the entry at that index. My leaders append a no-op when
they win, and those no-ops are the entries in the figure: S1's at index 2, S5's at
index 2 and S1's at index 3.

| ID | Command | Pass criterion |
|---|---|---|
| F1 | `go test -count=100 -run '^TestFigure8$' ./internal/raft` | passes all 100 runs |
| F2 | the same on a copy with `scripts/audit/m1/count-old-terms.patch` | fails all 100 runs |

F2 also has to show real harm, not just a broken rule: with the bug, branch (d) must
fail because two nodes applied different entries at index 2.
