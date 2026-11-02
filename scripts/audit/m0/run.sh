#!/bin/sh
# Runs every M0 audit step in order. The criteria, seeds and predictions are in
# docs/acceptance/M0.md. Run it from the repository root on a clean checkout:
#
#   scripts/audit/m0/run.sh
#
# Steps run one at a time because the crash harness and the race detector are
# both sensitive to other disk and CPU load. A step that fails does not stop the
# script: the D experiments are expected to fail, and every exit code is recorded.
set -u

repo=$(pwd)
out="$repo/results/audit/m0"
patches="$repo/scripts/audit/m0"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-m0.XXXXXX")
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

# copy NAME [PATCH] exports HEAD to a throwaway directory and applies a patch.
copy() {
	d="$work/$1"
	mkdir -p "$d"
	git archive HEAD | tar -x -C "$d"
	if [ -n "${2:-}" ]; then
		(cd "$d" && git apply "$patches/$2.patch") || exit 1
	fi
	echo "$d"
}

gofmt_clean() { test -z "$(gofmt -l cmd internal | tee /dev/stderr)"; }

# ---- A: test suite ----------------------------------------------------------
step a1-gofmt gofmt_clean
step a2-vet go vet ./...
step a3-test go test -count=1 ./...
step a4-race go test -race -count=1 ./...
step a5-race-x5 go test -race -count=5 ./...
step a6-cover go test -cover -count=1 ./...

# ---- B: crash harness, unmodified --------------------------------------------
go build -o "$work/crashtest" ./cmd/crashtest || exit 1
step b1-crash-seed1 "$work/crashtest" -trials 500 -seed 1 -out "$out/b1-crash-seed1.json"
step b2-crash-seed20261101 "$work/crashtest" -trials 500 -seed 20261101 -out "$out/b2-crash-seed20261101.json"

# ---- C: published numbers ----------------------------------------------------
c1=$(copy c1)
c1_check() {
	(cd "$c1" && go run ./cmd/results) &&
		cmp "$c1/README.md" "$repo/README.md" &&
		cmp "$c1/results/RESULTS.md" "$repo/results/RESULTS.md"
}
step c1-results-reproduce c1_check

# ---- D: experiments on patched copies ----------------------------------------
d1=$(copy d1 no-fsync)
(cd "$d1" && go build -o "$work/crashtest-d1" ./cmd/crashtest) || exit 1
step d1-no-fsync "$work/crashtest-d1" -trials 100 -seed 1 -out "$out/d1-no-fsync.json"

d2=$(copy d2 ack-before-durable)
(cd "$d2" && go build -o "$work/crashtest-d2" ./cmd/crashtest) || exit 1
step d2-ack-before-durable "$work/crashtest-d2" -trials 100 -seed 1 -out "$out/d2-ack-before-durable.json"

d3=$(copy d3 harness-probes)
(cd "$d3" && go build -o "$work/crashtest-d3" ./cmd/crashtest) || exit 1
step d3-d4-harness-probes "$work/crashtest-d3" -trials 100 -seed 1 -out "$out/d3-d4-harness-probes.json"
{
	echo "trials_probed: $(grep -c 'wal_bytes_before_child=' "$out/d3-d4-harness-probes.log")"
	echo "trials_with_wal_bytes_above_zero: $(grep 'wal_bytes_before_child=' "$out/d3-d4-harness-probes.log" | grep -vc 'wal_bytes_before_child=0$')"
	echo "trials_reader_cut_off: $(grep -c 'reader_cut_off' "$out/d3-d4-harness-probes.log")"
} | tee "$out/d3-d4-counts.txt"

d5=$(copy d5 engine-probes)
d5_tests() {
	(cd "$d5" && AUDIT_STRESS_SECONDS=60 go test -count=1 -v -timeout 300s -run 'TestAudit' ./internal/storage/engine/)
}
step d5-d9-engine-probes d5_tests

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
