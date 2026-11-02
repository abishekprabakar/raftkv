#!/bin/sh
# Runs every step in docs/acceptance/M1.md in order. Run it from the repository
# root on a clean checkout:
#
#   scripts/audit/m1/run.sh
#
# A failing step does not stop the script: every X step is expected to fail, and
# every exit code is recorded.
set -u

repo=$(pwd)
out="$repo/results/audit/m1"
patches="$repo/scripts/audit/m1"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-m1.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"
: >"$out/summary.txt"

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go: $(go version)"
	echo "uname: $(uname -srm)"
	echo "tree changes at start:"
	git status --porcelain
} >"$out/env.txt"

# step NAME COMMAND... runs a command, logs its output and records its exit code.
step() {
	name=$1
	shift
	echo "== $name: $*"
	t0=$(date +%s)
	"$@" >"$out/$name.log" 2>&1
	code=$?
	echo "$name exit=$code seconds=$(($(date +%s) - t0))" | tee -a "$out/summary.txt"
}

gofmt_clean() { test -z "$(gofmt -l cmd internal | tee /dev/stderr)"; }

# no_third_party prints every dependency outside the standard library and this
# module. Standard library paths have no dot in their first element.
no_third_party() {
	deps=$(go list -deps ./internal/raft | grep -v '^github.com/abishekprabakar/raftkv' | awk -F/ '$1 ~ /\./')
	echo "third-party packages: [$deps]"
	echo "go.mod require lines: $(grep -c '^require' go.mod)"
	test -z "$deps" && ! grep -q '^require' go.mod
}

# ---- T: build and tests ---------------------------------------------------------
step t1-gofmt gofmt_clean
step t2-vet go vet ./...
step t3-test go test -count=1 ./...
step t4-race-x5 go test -race -count=5 ./...
step t5-no-third-party no_third_party

# ---- C: cluster tests -----------------------------------------------------------
step c-cluster go test -count=1 -v -run 'TestElectsOneLeader|TestReplicatesToAllNodes|TestLeaderFailover|TestRestartKeepsCommittedEntries|TestClusterOnFileStorage' ./internal/raft/

# ---- X: each rule has a test that can fail --------------------------------------
# run_mutant DIR TEST runs the named test, which must fail, and then the whole
# package for information about what else notices. A panic ends a package run
# early, so the second count is a lower bound.
run_mutant() {
	(cd "$1" && go test -count=1 -run "^$2\$" ./internal/raft/)
}
run_suite() {
	(cd "$1" && go test -count=1 ./internal/raft/)
}
mutant() {
	id=$1
	name=$2
	tests=$3
	d="$work/$name"
	mkdir -p "$d"
	git archive HEAD | tar -x -C "$d"
	(cd "$d" && git apply "$patches/$name.patch") || exit 1
	step "$id-$name" run_mutant "$d" "$tests"
	step "$id-$name-full-suite" run_suite "$d"
}
mutant x1 send-before-save TestVoteIsDurableBeforeReply
mutant x2 forget-voted-for TestRestartCannotVoteTwice
mutant x3 count-old-terms TestLeaderDoesNotCountOldTermEntries
mutant x4 always-truncate TestDuplicateAppendDoesNotTruncate
mutant x5 commit-to-last-index TestFollowerCommitStopsAtLastNewEntry
mutant x6 no-up-to-date-check TestRejectVoteForStaleLog
mutant x7 commit-can-decrease TestCommitIndexNeverDecreases
mutant x8 fixed-timeout TestElectionTimeoutsAreRandomized
mutant x9 save-after-apply TestSingleNodeAppliesOnlyAfterSave

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
