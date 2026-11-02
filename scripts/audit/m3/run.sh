#!/bin/sh
# Runs every step in docs/acceptance/M3.md in order. Run it from the repository
# root on a clean checkout:
#
#   scripts/audit/m3/run.sh
#
# A failing step does not stop the script: every P step is expected to fail, and
# every exit code is recorded.
set -u

repo=$(pwd)
out="$repo/results/audit/m3"
patches="$repo/scripts/audit/m3"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-m3.XXXXXX")
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

gofmt_clean() { test -z "$(gofmt -l cmd internal | tee /dev/stderr)"; }

no_third_party() {
	deps=$(go list -deps ./internal/kv ./internal/raft/sim | grep -v '^github.com/abishekprabakar/raftkv' | awk -F/ '$1 ~ /\./')
	echo "third-party packages: [$deps]"
	test -z "$deps"
}

check_totals() {
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
    ("writes acknowledged", t["writes_acknowledged"], 100000),
    ("retried writes dropped", t["retried_writes_dropped"], 100),
    ("reads served", t["reads_served"], 50000),
    ("reads refused", t["reads_refused"], 100),
    ("log compactions", t["log_compactions"], 1000),
    ("snapshots installed", t["snapshots_installed"], 100),
    ("power-loss crashes", t["power_loss_crashes"], 1000),
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

# ---- R: scripted tests ----------------------------------------------------------
step r-scripted go test -count=1 -v -run 'TestPlainLeaderReadIsStale|TestReadIndexRefusesDeposedLeader|TestLaggingFollowerInstallsSnapshot|TestCompactionShrinksTheLog|TestFigure8' ./internal/kv ./internal/raft

# ---- S and F: schedules, and proof the new paths ran -----------------------------
go build -o "$work/raftsim" ./cmd/raftsim || exit 1
step s1-seeds-1-10000 "$work/raftsim" -first 1 -count 10000 -out "$repo/results/sim.json"
step s2-fresh-seeds "$work/raftsim" -first 2000001 -count 10000 -out "$out/sim-fresh.json"
step s3-real-engine "$work/raftsim" -first 3000001 -count 200 -lsm -out "$out/sim-lsm.json"
step f-totals check_totals

# ---- P: every planted bug --------------------------------------------------------
named_test() { (cd "$1" && go test -count=1 -run "^$2\$" "$3"); }
for bug in send-before-save forget-voted-for count-old-terms always-truncate commit-to-last-index \
	no-up-to-date-check commit-can-decrease fixed-timeout save-after-apply \
	plain-leader-read no-dedup sessions-not-in-snapshot session-outside-batch compact-without-flush \
	install-keeps-conflicting-log stale-snapshot-rolls-back restore-position-lost \
	read-skips-term-check save-before-restore; do
	d="$work/$bug"
	mkdir -p "$d"
	git archive HEAD | tar -x -C "$d"
	(cd "$d" && git apply "$patches/$bug.patch" && go build -o "$work/raftsim-$bug" ./cmd/raftsim) || exit 1
	case $bug in
	read-skips-term-check) step "p-$bug" named_test "$d" TestReadWaitsForCommitInCurrentTerm ./internal/raft ;;
	save-before-restore) step "p-$bug" named_test "$d" TestRestoreHappensBeforeSave ./internal/raft ;;
	*) step "p-$bug" named_test "$d" TestSimSchedules ./internal/raft/sim ;;
	esac
	step "q-$bug" "$work/raftsim-$bug" -first 1 -count 2000 -out "$out/q-$bug.json"
done

# ---- C: crash harness ------------------------------------------------------------
go build -o "$work/crashtest" ./cmd/crashtest || exit 1
step c-crash-seed1 "$work/crashtest" -trials 500 -seed 1 -out "$repo/results/crash.json"

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
