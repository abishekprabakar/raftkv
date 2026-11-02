#!/bin/sh
# Runs every step in docs/acceptance/M5.md in order. Run it from the repository
# root on a clean checkout:
#
#   scripts/audit/m5/run.sh
#
# A failing step does not stop the script, and every exit code is recorded. Steps
# that are meant to fail (S3, P2) are marked "expect failure". It ends with no
# raftkv containers, volumes or planted-bug image left behind.
set -u

repo=$(pwd)
out="$repo/results/audit/m5"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-m5.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"
: >"$out/summary.txt"

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go: $(go version)"
	echo "docker: $(docker version --format '{{.Server.Version}}')"
	echo "uname: $(uname -srm)"
	echo "tree changes at start:"
	git status --porcelain
} >"$out/env.txt"

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
third_party() { go list -deps "$@" | grep -v '^github.com/abishekprabakar/raftkv' | awk -F/ '$1 ~ /\./'; }

core_has_no_deps() {
	deps=$(third_party ./internal/raft/... ./internal/kv ./internal/storage/...)
	echo "third-party packages in the core: [$deps]"
	test -z "$deps"
}

binaries_use_allowed_modules() {
	deps=$(third_party ./cmd/raftkvd ./cmd/raftkv ./cmd/kvworker)
	echo "$deps"
	bad=$(echo "$deps" | grep -v -E '^(google\.golang\.org/grpc|google\.golang\.org/protobuf|google\.golang\.org/genproto/googleapis/rpc|golang\.org/x/net|golang\.org/x/sys|golang\.org/x/text)(/|$)')
	echo "outside the allowed modules: [$bad]"
	test -z "$bad"
}

check_minimums() {
	python3 - "$repo/results/chaos.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
t, f = d["totals"], d["faults"]
wanted = [
    ("schedules", d["runs"], 100),
    ("leader isolations", f.get("leader", 0), 100),
    ("follower isolations", f.get("follower", 0), 50),
    ("two-node isolations", f.get("two", 0), 25),
    ("writes acknowledged", t["writes_acknowledged"], 20000),
    ("reads served", t["reads_served"], 10000),
    ("writes with an unknown outcome", t["writes_unknown"], 100),
    ("reads refused or timed out", t["reads_refused_or_timed_out"], 100),
]
ok = True
for name, got, minimum in wanted:
    passed = got >= minimum
    ok = ok and passed
    print(f"{'ok  ' if passed else 'FAIL'} {name}: {got} (minimum {minimum})")
fo = d["failover"]
print(f"failover: {fo['measured']} measured of {fo['leader_isolations']} leader isolations, "
      f"median {fo['median_ms']} ms, p95 {fo['p95_ms']} ms, max {fo['max_ms']} ms, "
      f"{fo['no_new_leader']} with no new leader, {fo['unmatched']} unmatched")
sys.exit(0 if ok else 1)
PY
}

# ---- T and P1: build and tests -----------------------------------------------------
step t1-gofmt gofmt_clean
step t2-vet go vet ./...
step t3-test go test -count=1 ./...
step t4-race go test -race -count=1 ./...
step t5-core-deps core_has_no_deps
step t6-binary-deps binaries_use_allowed_modules
step p1-model go test -count=1 -v -run '^TestModelRejectsBadHistories$' ./internal/chaos

# ---- S1, S2: the scripted stale read, on this checkout -----------------------------
step s-stale-read env OUT="$out/stale-read" sh scripts/audit/m5/stale-read.sh 5

# ---- C1, C2, F: 100 fault schedules checked with Porcupine ---------------------------
go build -o "$work/chaos" ./cmd/chaos || exit 1
step c-chaos "$work/chaos" -first 1 -count 100 -out "$repo/results/chaos.json" \
	-artifacts "$out/runs" -log "$out/chaos-docker.log"
step f-minimums check_minimums

# ---- S3, P2: the same checks on a build with plain-leader-read (expect failure) -------
mkdir -p "$work/src" && git archive HEAD | tar -x -C "$work/src"
(cd "$work/src" && git apply "$repo/scripts/audit/m3/plain-leader-read.patch") || exit 1
docker build -q -t raftkv:plain-leader-read "$work/src" >"$out/planted-build.log" 2>&1 || exit 1
step s3-stale-read-planted env RAFTKV_IMAGE=raftkv:plain-leader-read NO_BUILD=1 OUT="$out/stale-read-planted" \
	sh scripts/audit/m5/stale-read.sh 5
step p2-chaos-planted "$work/chaos" -image raftkv:plain-leader-read -build=false -first 1 -count 20 \
	-out "$out/planted-plain-leader-read.json" -artifacts "$out/planted-runs" -log "$out/planted-docker.log"

# ---- Tear down -----------------------------------------------------------------------
docker compose down -v --remove-orphans >>"$out/teardown.log" 2>&1
docker image rm raftkv:plain-leader-read >>"$out/teardown.log" 2>&1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
