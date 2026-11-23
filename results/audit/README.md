# About these records

Each folder holds what one acceptance run wrote: the output of every step, its exit
code, and the environment it ran in. The criteria each run was held to are in
`docs/acceptance/`, and `LEARNING.md` says what each run showed.

## What was changed after the fact

The history of this repository was rewritten on 2026-11-23, before it was shared with
anyone. The rewrite changed the author address on every commit, changed some wording
in the notes, and renamed two tests, which are now
`TestWritesContinueAfterLeaderStops` and `TestHeartbeatContinuesAfterLostReply`.

Three things in these files changed with it, and nothing else did:

- **Commit hashes.** Rewriting gives every commit a new hash. So that a recorded hash
  still names a commit that exists, each hash in these files, in the notes and in the
  scripts was replaced by the hash the same commit has now.
- **A test name.** `m4/g-tests.log` names one of the renamed tests, and was updated to
  match.
- **Local paths.** Where a log printed the path of the checkout or of a temporary
  directory on the machine that ran it, the start of the path was replaced by `$HOME`
  or `$TMPDIR`.

No measured value, no pass or fail, and no timing was touched.
