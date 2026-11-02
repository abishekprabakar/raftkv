#!/bin/sh
# Runs every step in docs/acceptance/M2.md in order. Run it from the repository
# root on a clean checkout:
#
#   scripts/audit/m2/run.sh
#
# A failing step does not stop the script: the P, Q, R and U steps are expected
# to fail, and every exit code is recorded.
set -u

repo=$(pwd)
out="$repo/results/audit/m2"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-m2.XXXXXX")
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
	step_name=$1
	shift
	echo "== $step_name: $*"
	t0=$(date +%s)
	"$@" >"$out/$step_name.log" 2>&1
	code=$?
	echo "$step_name exit=$code seconds=$(($(date +%s) - t0))" | tee -a "$out/summary.txt"
}

# patched NAME PATCHFILE exports HEAD to a throwaway directory and applies a patch.
patched() {
	d="$work/$1"
	mkdir -p "$d"
	git archive HEAD | tar -x -C "$d"
	(cd "$d" && git apply "$2") || exit 1
	echo "$d"
}

gofmt_clean() { test -z "$(gofmt -l cmd internal | tee /dev/stderr)"; }

no_third_party() {
	deps=$(go list -deps ./internal/raft/sim | grep -v '^github.com/abishekprabakar/raftkv' | awk -F/ '$1 ~ /\./')
	echo "third-party packages: [$deps]"
	test -z "$deps"
}

check_fault_totals() {
	python3 - "$repo/results/sim.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
t = d["totals"]
wanted = [
    ("schedules", d["schedules"], 10000),
    ("messages dropped", t["messages_dropped"], 100000),
    ("messages duplicated", t["messages_duplicated"], 10000),
    ("messages delayed past an election timeout", t["messages_delayed_past_election_timeout"], 1000),
    ("messages reordered", t["messages_reordered"], 10000),
    ("partitions", t["partitions"], 1000),
    ("one-way cuts", t["one_way_cuts"], 100),
    ("crash-restarts", t["crashes"], 1000),
    ("crashes during a save", t["crashes_during_save"], 100),
    ("crashes after a save", t["crashes_after_save"], 100),
]
ok = True
for name, got, minimum in wanted:
    passed = got >= minimum
    ok = ok and passed
    print(f"{'ok  ' if passed else 'FAIL'} {name}: {got} (minimum {minimum})")
for profile, n in sorted(d["schedules_by_profile"].items()):
    passed = n >= 500
    ok = ok and passed
    print(f"{'ok  ' if passed else 'FAIL'} profile {profile}: {n} schedules (minimum 500)")
sys.exit(0 if ok else 1)
PY
}

# ---- T: build and tests ---------------------------------------------------------
step t1-gofmt gofmt_clean
step t2-vet go vet ./...
step t3-test go test -count=1 ./...
step t4-race go test -race -count=1 ./...
step t5-no-third-party no_third_party

# ---- D: determinism -------------------------------------------------------------
step d-determinism go test -count=1 -v -run 'TestSameSeedSameRun|TestWorkerCountDoesNotChangeResults|TestOptionsDoNotChangeTheRun' ./internal/raft/sim

# ---- S and F: schedules, and proof the faults happened --------------------------
go build -o "$work/raftsim" ./cmd/raftsim || exit 1
step s1-seeds-1-10000 "$work/raftsim" -first 1 -count 10000 -out "$repo/results/sim.json"
step s2-fresh-seeds "$work/raftsim" -first 1000001 -count 10000 -out "$out/sim-fresh.json"
step f-fault-totals check_fault_totals

# ---- P, Q, R: every planted bug at cluster level ---------------------------------
sim_test() { (cd "$1" && go test -count=1 -run '^TestSimSchedules$' ./internal/raft/sim); }
for bug in send-before-save forget-voted-for count-old-terms always-truncate commit-to-last-index \
	no-up-to-date-check commit-can-decrease fixed-timeout save-after-apply; do
	d=$(patched "$bug" "$repo/scripts/audit/m1/$bug.patch")
	(cd "$d" && go build -o "$work/raftsim-$bug" ./cmd/raftsim) || exit 1
	step "p-$bug" sim_test "$d"
	step "q-$bug" "$work/raftsim-$bug" -first 1 -count 2000 -out "$out/q-$bug.json"
	step "r-$bug" "$work/raftsim-$bug" -first 1 -count 2000 -properties-only -out "$out/r-$bug.json"
done

# ---- U: the storm regression tests fail without the fix --------------------------
storm_tests() {
	(cd "$1" && go test -count=1 -v -run 'TestRepeatedReplySendsNothing|TestRepeatedRejectionSendsNothing|TestNoMessageStorm' ./internal/raft ./internal/raft/sim)
}
d=$(patched unfix-message-storm "$repo/scripts/audit/m2/unfix-message-storm.patch")
step u1-unfix-message-storm storm_tests "$d"

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
