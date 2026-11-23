# About these records

Each folder holds what one acceptance run wrote: the output of every step, its exit
code, and the environment it ran in. The criteria each run was held to are in
`docs/acceptance/`, and `LEARNING.md` says what each run showed.

## Commit hashes

The history of this repository was rewritten once, on 2026-11-23, before it was
shared with anyone. The rewrite changed the author address on every commit, changed
some wording in the notes, and renamed two tests, which are now
`TestWritesContinueAfterLeaderStops` and `TestHeartbeatContinuesAfterLostReply`.

Rewriting gives every commit a new hash. So that a recorded hash still names a commit
that exists, each hash in these files, in the notes and in the scripts was replaced
by the hash the same commit has now. `m4/g-tests.log` names one of the renamed tests,
and was updated the same way.

Nothing else in a recorded file was touched: no measured value, no pass or fail, no
timing.
