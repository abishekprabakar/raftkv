#!/bin/sh
# Runs the checks in docs/acceptance/stale-read.md. Run it from the repository
# root on a clean checkout:
#
#   scripts/audit/stale-read/run.sh
set -u

repo=$(pwd)
out="$repo/results/audit/stale-read"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-stale-read.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"
test_name=TestPartitionedLeaderRefusesStaleRead

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go: $(go version)"
	echo "tree changes at start:"
	git status --porcelain
} >"$out/env.txt"

# count FILE TEST prints how many runs of TEST passed and failed in a -v log.
count() {
	echo "passed=$(grep -c "^--- PASS: $2 " "$1") failed=$(grep -c "^--- FAIL: $2 " "$1")"
}

echo "== SR1: correct code, 100 runs"
go test -count=100 -v -run "^$test_name\$" ./internal/kv >"$out/sr1-correct.log" 2>&1
echo "sr1 exit=$? $(count "$out/sr1-correct.log" $test_name)" | tee "$out/summary.txt"

echo "== SR2: with plain-leader-read, 100 runs"
git archive HEAD | tar -x -C "$work"
(cd "$work" && git apply "$repo/scripts/audit/m3/plain-leader-read.patch") || exit 1
(cd "$work" && go test -count=100 -v -run "^$test_name\$" ./internal/kv) >"$out/sr2-plain-leader-read.log" 2>&1
code=$?
harm=$(grep -c 'step (c): S1 served k = "old" after k = "new" was acknowledged' "$out/sr2-plain-leader-read.log")
echo "sr2 exit=$code $(count "$out/sr2-plain-leader-read.log" $test_name) runs_serving_old_after_new_was_acknowledged=$harm" | tee -a "$out/summary.txt"

echo "== SR3: format, vet and the kv tests"
{
	echo "gofmt: [$(gofmt -l internal)]"
	go vet ./internal/kv && echo "vet ok"
	go test -count=1 ./internal/kv
} >"$out/sr3-kv.log" 2>&1
echo "sr3 exit=$? $(head -1 "$out/sr3-kv.log")" | tee -a "$out/summary.txt"

echo "== For the record: the M3 scripted test with plain-leader-read, 100 runs"
(cd "$work" && go test -count=100 -v -run '^TestReadIndexRefusesDeposedLeader$' ./internal/kv) >"$out/record-m3-test-plain-leader-read.log" 2>&1
echo "record exit=$? $(count "$out/record-m3-test-plain-leader-read.log" TestReadIndexRefusesDeposedLeader)" | tee -a "$out/summary.txt"

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
