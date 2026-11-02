#!/bin/sh
# Runs every step in docs/acceptance/M4.md in order: T and G here, then D through
# compose.sh. Run it from the repository root on a clean checkout:
#
#   scripts/audit/m4/run.sh
#
# A failing step does not stop the script, and every exit code is recorded.
set -u

repo=$(pwd)
out="$repo/results/audit/m4"
mkdir -p "$out"
: >"$out/summary.txt"

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go: $(go version)"
	echo "protoc: $(protoc --version)"
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

third_party() { go list -deps "$@" | grep -v '^github.com/abishekprabakar/raftkv' | awk -F/ '$1 ~ /\./'; }

core_has_no_deps() {
	deps=$(third_party ./internal/raft/... ./internal/kv ./internal/storage/...)
	echo "third-party packages in the core: [$deps]"
	test -z "$deps"
}

binaries_use_allowed_modules() {
	deps=$(third_party ./cmd/raftkvd ./cmd/raftkv)
	echo "$deps"
	bad=$(echo "$deps" | grep -v -E '^(google\.golang\.org/grpc|google\.golang\.org/protobuf|google\.golang\.org/genproto/googleapis/rpc|golang\.org/x/net|golang\.org/x/sys|golang\.org/x/text)(/|$)')
	echo "outside the allowed modules: [$bad]"
	test -z "$bad"
}

# ---- T: build and tests ---------------------------------------------------------
step t1-gofmt gofmt_clean
step t2-vet go vet ./...
step t3-test go test -count=1 ./...
step t4-race go test -race -count=1 ./...
step t5-core-deps core_has_no_deps
step t6-binary-deps binaries_use_allowed_modules

# ---- G: the named tests over real gRPC, verbose -----------------------------------
step g-tests go test -count=1 -v -run 'TestMessagesRoundTrip|TestClusterOverGRPC|TestClientFollowsRedirect|TestWritesContinueAfterLeaderStops|TestRetriedWriteAppliesOnce' ./internal/server

# ---- D: Docker Compose ------------------------------------------------------------
step d-compose sh scripts/audit/m4/compose.sh

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
