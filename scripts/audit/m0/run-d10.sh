#!/bin/sh
# Runs experiment D10 from docs/acceptance/M0.md: the D5 tests again, with the
# snapshot-ordering fix applied to a throwaway copy. Run it from the repository
# root:
#
#   scripts/audit/m0/run-d10.sh
set -u

repo=$(pwd)
out="$repo/results/audit/m0"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-m0-d10.XXXXXX")
trap 'rm -rf "$work"' EXIT

git archive HEAD | tar -x -C "$work"
cd "$work" || exit 1
git apply "$repo/scripts/audit/m0/engine-probes.patch" || exit 1
git apply "$repo/scripts/audit/m0/get-snapshot-fix.patch" || exit 1

AUDIT_STRESS_SECONDS=60 go test -count=1 -v -timeout 300s -run 'TestAuditGet' \
	./internal/storage/engine/ >"$out/d10-get-snapshot-fix.log" 2>&1
code=$?
echo "d10-get-snapshot-fix exit=$code commit=$(cd "$repo" && git rev-parse HEAD)" | tee -a "$out/summary.txt"
cat "$out/d10-get-snapshot-fix.log"
