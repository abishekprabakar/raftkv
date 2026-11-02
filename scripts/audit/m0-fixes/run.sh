#!/bin/sh
# Runs every step in docs/acceptance/M0-fixes.md in order. Run it from the
# repository root on a clean checkout:
#
#   scripts/audit/m0-fixes/run.sh
#
# Steps run one at a time so disk-heavy steps never overlap. A failing step does
# not stop the script: the R and M steps are expected to fail, and every exit
# code is recorded.
set -u

repo=$(pwd)
out="$repo/results/audit/m0-fixes"
patches="$repo/scripts/audit/m0-fixes"
oldHarness=d5eba3f
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-fixes.XXXXXX")
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

# copy NAME COMMIT [PATCHFILE] exports a commit to a throwaway directory and
# applies a patch.
copy() {
	d="$work/$1"
	mkdir -p "$d"
	git archive "$2" | tar -x -C "$d"
	if [ -n "${3:-}" ]; then
		(cd "$d" && git apply "$3") || exit 1
	fi
	echo "$d"
}

gofmt_clean() { test -z "$(gofmt -l cmd internal | tee /dev/stderr)"; }

# ---- T: test suite ----------------------------------------------------------
step t1-gofmt gofmt_clean
step t2-vet go vet ./...
step t3-test go test -count=1 ./...
step t4-race-x5 go test -race -count=5 ./...

# ---- R: regression tests fail without their fix ------------------------------
regression() {
	name=$1
	tests=$2
	d=$(copy "$name" HEAD "$patches/$name.patch")
	# The run goes in a function so step can log it.
	eval "run_$name() { (cd '$d' && go test -count=1 -v -run '$tests' ./internal/storage/...); }"
	step "$name" "run_$name"
}
regression unfix-snapshot-order 'TestGetSnapshotOrder|TestScanSnapshotOrder'
regression unfix-oversize 'TestOversizeRecordRejected'
regression unfix-iterator 'TestExhaustedIteratorStaysExhausted'
regression unfix-wal-corruption 'TestMidSegmentCorruptionIsAnError|TestMidSegmentCorruptionRefusesOpen'

# ---- H: crash harness on the fixed code --------------------------------------
go build -o "$work/crashtest" ./cmd/crashtest || exit 1
step h1-crash-seed1 "$work/crashtest" -trials 500 -seed 1 -out "$repo/results/crash.json"
step h2-crash-seed20261102 "$work/crashtest" -trials 500 -seed 20261102 -out "$out/crash-seed20261102.json"

# ---- M: does the harness catch what it claims to? ----------------------------
harness() {
	name=$1
	commit=$2
	patch=$3
	d=$(copy "$name" "$commit" "$patch")
	(cd "$d" && go build -o "$work/crashtest-$name" ./cmd/crashtest) || exit 1
	step "$name" "$work/crashtest-$name" -trials 100 -seed 1 -out "$out/$name.json"
}
harness m1-old-harness-recovery-mutant "$oldHarness" "$patches/recovery-drops-wal-early.patch"
harness m2-new-harness-recovery-mutant HEAD "$patches/recovery-drops-wal-early.patch"
harness m3-new-harness-tail-mutant HEAD "$patches/no-tail-truncation.patch"
harness m4-new-harness-ack-mutant HEAD "$repo/scripts/audit/m0/ack-before-durable.patch"

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
