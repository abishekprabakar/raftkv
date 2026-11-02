#!/bin/sh
# Runs the checks in docs/acceptance/figure8.md. Run it from the repository root
# on a clean checkout:
#
#   scripts/audit/figure8/run.sh
set -u

repo=$(pwd)
out="$repo/results/audit/figure8"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-figure8.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go: $(go version)"
	echo "tree changes at start:"
	git status --porcelain
} >"$out/env.txt"

echo "== F1: correct code, 100 runs"
go test -count=100 -v -run '^TestFigure8$' ./internal/raft >"$out/f1-correct.log" 2>&1
code=$?
passed=$(grep -c '^--- PASS: TestFigure8 ' "$out/f1-correct.log")
failed=$(grep -c '^--- FAIL: TestFigure8 ' "$out/f1-correct.log")
echo "f1 exit=$code passed=$passed failed=$failed" | tee "$out/summary.txt"

echo "== F2: with count-old-terms, 100 runs"
git archive HEAD | tar -x -C "$work"
(cd "$work" && git apply "$repo/scripts/audit/m1/count-old-terms.patch") || exit 1
(cd "$work" && go test -count=100 -v -run '^TestFigure8$' ./internal/raft) >"$out/f2-count-old-terms.log" 2>&1
code=$?
passed=$(grep -c '^--- PASS: TestFigure8 ' "$out/f2-count-old-terms.log")
failed=$(grep -c '^--- FAIL: TestFigure8 ' "$out/f2-count-old-terms.log")
harm=$(grep -c 'applied.*at index 2, another node applied' "$out/f2-count-old-terms.log")
echo "f2 exit=$code passed=$passed failed=$failed runs_with_conflicting_apply_at_index_2=$harm" | tee -a "$out/summary.txt"

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
