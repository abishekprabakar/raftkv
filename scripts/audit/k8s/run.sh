#!/bin/sh
# Runs every step in docs/acceptance/k8s.md in order. Run it from the repository
# root on a clean checkout, with Docker running:
#
#   scripts/audit/k8s/run.sh
#
# A failing step does not stop the script, and every exit code is recorded. Steps
# that are meant to fail (P2, P3) are marked "expect failure". It starts by
# destroying any cluster this configuration made before, and ends with none.
set -u

# Go's clock on macOS stops while the machine sleeps, so keep it awake.
if [ -z "${RAFTKV_AWAKE:-}" ] && command -v caffeinate >/dev/null 2>&1; then
	RAFTKV_AWAKE=1 exec caffeinate -i "$0" "$@"
fi

repo=$(pwd)
tf="$repo/deploy/terraform"
out="$repo/results/audit/k8s"
work=$(mktemp -d "${TMPDIR:-/tmp}/raftkv-k8s.XXXXXX")
trap 'rm -rf "$work"' EXIT
mkdir -p "$out"
: >"$out/summary.txt"

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "go: $(go version)"
	echo "docker: $(docker version --format '{{.Server.Version}}'), $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes')"
	echo "terraform: $(terraform version | head -1)"
	echo "kubectl: $(kubectl version --client 2>/dev/null | head -1)"
	echo "uname: $(uname -srm)"
	command -v pmset >/dev/null 2>&1 && echo "power: $(pmset -g batt | head -1)"
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

# Terraform through the wrapper that hides the cluster's credentials: a destroy
# prints the client key, and these logs are committed.
terraform_() { "$repo/scripts/k8s/terraform.sh" "$@"; }
apply() { terraform_ apply -auto-approve -input=false -no-color; }

# A3: exact provider versions, a node image pinned by digest, and a lock file
# that needs no change.
pins() {
	ok=0
	echo "provider versions in versions.tf:"
	grep -E '^\s*version\s*=' "$tf/versions.tf"
	if grep -E '^\s*version\s*=' "$tf/versions.tf" | grep -v -E '=\s*"[0-9]+\.[0-9]+\.[0-9]+"\s*$'; then
		echo "FAIL: a provider version is not exact"
		ok=1
	fi
	echo "node image in variables.tf:"
	grep -E 'kindest/node:v[0-9.]+@sha256:[0-9a-f]{64}' "$tf/variables.tf" || { echo "FAIL: the node image has no digest"; ok=1; }
	terraform_ init -input=false -no-color -lockfile=readonly || ok=1
	git diff --exit-code -- "$tf/.terraform.lock.hcl" || { echo "FAIL: the lock file changed"; ok=1; }
	git ls-files --error-unmatch "$tf/.terraform.lock.hcl" || ok=1
	return $ok
}

# Nothing of the cluster exists: no Terraform state and no node container.
nothing_left() {
	state=$(terraform_ state list 2>/dev/null)
	containers=$(docker ps --all --quiet --filter 'name=^raftkv-control-plane$')
	echo "resources in the Terraform state: [$state]"
	echo "node containers: [$containers]"
	test -z "$state" && test -z "$containers"
}

destroy_all() {
	terraform_ destroy -auto-approve -input=false -no-color && nothing_left
}

# S1: nothing the run recorded holds a private key or a kubeconfig's credentials.
no_credentials() {
	found=$(grep -r -l -E 'PRIVATE KEY|(client-key|client-certificate|certificate-authority)-data: *[A-Za-z0-9+/=]{16,}' "$out" "$repo"/results/k8s-*.json)
	echo "files holding credentials: [$found]"
	test -z "$found"
}

# Reads the reports and prints one line per criterion.
criteria() {
	python3 - "$repo/results" "$out" <<'PY'
import json, sys, os
results, audit = sys.argv[1], sys.argv[2]
ok = True
def line(passed, text):
    global ok
    ok = ok and passed
    print(f"{'ok  ' if passed else 'FAIL'} {text}")
def load(path):
    try:
        return json.load(open(path))
    except Exception as e:
        line(False, f"{path}: {e}")
        return None
def durability(tag, d):
    du = d["durability"]
    line(du["acknowledged_writes_lost"] == 0, f"{tag} D1 acknowledged writes lost: {du['acknowledged_writes_lost']} of {du['writes_acknowledged']}")
    line(du["writes_duplicated"] == 0 and du["values_never_written"] == 0, f"{tag} D2 duplicated: {du['writes_duplicated']}, never written: {du['values_never_written']}")
    line(du["keys_whose_final_read_failed"] == 0, f"{tag} D3 final reads failed: {du['keys_whose_final_read_failed']} of {du['keys']} keys")
    line(d["linearizable"] == "Ok", f"{tag} D4 Porcupine: {d['linearizable']} in {d['check_seconds']:.1f} s")

c = load(os.path.join(results, "k8s-checks.json"))
if c:
    for ch in c["checks"]:
        line(ch["pass"], f"{ch['id']} {ch['what']}")

f = load(os.path.join(results, "k8s-failover.json"))
if f:
    s = f["summary"]
    line(s["trials"] == 100 and s["no_new_leader_within_60s"] == 0 and s["not_settled_within_180s"] == 0 and s["errors"] == 0,
         f"F1 {s['trials']} trials: {s['no_new_leader_within_60s']} without a new leader, {s['not_settled_within_180s']} not settled, {s['errors']} errors")
    line(s["replacement_did_not_start_from_its_volume"] == 0, f"F2 replacements that did not start from their volume: {s['replacement_did_not_start_from_its_volume']} of {s['replacement_pods']}")
    durability("failover", f)
    n, w = s["new_leader_ms"], s["first_write_ms"]
    print(f"     time to a new leader: median {n['median']:.0f} ms, p95 {n['p95']:.0f} ms, min {n['min']:.0f}, max {n['max']:.0f} (n={n['n']})")
    print(f"     time to the first successful write: median {w['median']:.0f} ms, p95 {w['p95']:.0f} ms, min {w['min']:.0f}, max {w['max']:.0f} (n={w['n']})")

r = load(os.path.join(results, "k8s-restart.json"))
if r:
    s = r["summary"]
    line(s["trials"] == 5 and s["passed"] == 5, f"R1 {s['passed']} of {s['trials']} rounds recovered, every pod from its volume")
    durability("restart", r)

u = load(os.path.join(results, "k8s-rollout.json"))
if u:
    s = u["summary"]
    line(s["rounds"] == 10 and s["passed"] == 10, f"U1 {s['passed']} of {s['rounds']} rollouts finished on the new version")
    durability("rollout", u)
    line(s["failed_requests"] == 0, f"U3 failed client requests (target 0): {s['failed_requests']} of {s['requests']}")
    ro = s["rollout_seconds"]
    print(f"     rollout time: median {ro['median']:.1f} s, min {ro['min']:.1f}, max {ro['max']:.1f}")

cs = load(os.path.join(results, "k8s-coldstart.json"))
if cs:
    s = cs["summary"]
    line(s["rounds"] == 10 and s["passed"] == 10, f"C1 {s['passed']} of {s['rounds']} cold starts reached 3 Ready pods and a leader")
    rd = s["ready_with_leader_seconds"]
    print(f"     cold start: median {rd['median']:.1f} s, min {rd['min']:.1f}, max {rd['max']:.1f}")

p2 = load(os.path.join(audit, "planted-no-dedup.json"))
if p2:
    du = p2["durability"]
    line(du["writes_duplicated"] > 0 or p2["linearizable"] == "Illegal",
         f"P2 with no-dedup planted: {du['writes_duplicated']} duplicated writes, Porcupine {p2['linearizable']}")
p3 = load(os.path.join(audit, "planted-emptydir.json"))
if p3:
    du = p3["durability"]
    line(du["acknowledged_writes_lost"] > 0, f"P3 on an emptyDir: {du['acknowledged_writes_lost']} of {du['writes_acknowledged']} acknowledged writes lost")
sys.exit(0 if ok else 1)
PY
}

# ---- T, H, P1: build and tests -------------------------------------------------------
step t1-gofmt gofmt_clean
step t2-vet go vet ./...
step t3-test go test -count=1 ./...
step t4-race go test -race -count=1 ./...
step t5-core-deps core_has_no_deps
step t6-binary-deps binaries_use_allowed_modules
step t7-terraform-fmt terraform_ fmt -check -recursive
step a3-pins pins
step t8-terraform-validate terraform_ validate -no-color
step h-health go test -count=1 -v -run '^(TestLonelyNodeIsLiveButNotReady|TestClusterBecomesReady|TestFollowerBehindIsNotReady|TestLeaderWithoutMajorityIsNotReady|TestIDFromHostname|TestPeerReopensAStreamThatDiedWhileIdle)$' ./internal/server ./cmd/raftkvd
step p1-durability go test -count=1 -v -run '^TestDurabilityCountsLostAndDuplicatedWrites$' ./internal/kube

go build -o "$work/kubebench" ./cmd/kubebench || exit 1
bench() { "$work/kubebench" "$@"; }

# ---- A: one apply from nothing ---------------------------------------------------------
step a0-nothing destroy_all
step a1-apply apply
step ak-checks bench check -out "$repo/results/k8s-checks.json" -log "$out/checks-commands.log"

# ---- F, D: leader failover -------------------------------------------------------------
step f-failover bench failover -trials 100 -seed 1 -out "$repo/results/k8s-failover.json" \
	-artifacts "$out/failover" -log "$out/failover-commands.log"

# ---- R: every pod at once ----------------------------------------------------------------
step r-restart bench restart -rounds 5 -seed 1 -out "$repo/results/k8s-restart.json" \
	-artifacts "$out/restart" -log "$out/restart-commands.log"

# ---- U: rolling update under load ---------------------------------------------------------
step u-rollout bench rollout -rounds 10 -seed 1 -out "$repo/results/k8s-rollout.json" \
	-artifacts "$out/rollout" -log "$out/rollout-commands.log"

# ---- C: cold start ---------------------------------------------------------------------
step c-coldstart bench coldstart -rounds 10 -out "$repo/results/k8s-coldstart.json" -log "$out/coldstart-commands.log"

# ---- P2: failover on a build with no-dedup planted (expect failure) -------------------------
mkdir -p "$work/src" && git archive HEAD | tar -x -C "$work/src"
(cd "$work/src" && git apply "$repo/scripts/audit/m3/no-dedup.patch") || exit 1
planted_no_dedup() {
	export TF_VAR_build_context="$work/src" TF_VAR_image_tag=no-dedup
	apply && bench failover -trials 20 -seed 1 -out "$out/planted-no-dedup.json" \
		-artifacts "$out/planted-no-dedup" -log "$out/planted-no-dedup-commands.log"
	code=$?
	unset TF_VAR_build_context TF_VAR_image_tag
	return $code
}
step p2-planted-no-dedup planted_no_dedup

# ---- P3: restart every pod with the data on an emptyDir (expect failure) --------------------
planted_emptydir() {
	export TF_VAR_persistent=false
	apply && bench restart -rounds 3 -seed 1 -out "$out/planted-emptydir.json" \
		-artifacts "$out/planted-emptydir" -log "$out/planted-emptydir-commands.log"
	code=$?
	unset TF_VAR_persistent
	return $code
}
step p3-planted-emptydir planted_emptydir

# ---- X: destroy ------------------------------------------------------------------------
step x1-destroy destroy_all
docker image rm raftkv:no-dedup >>"$out/x1-destroy.log" 2>&1

step s1-no-credentials no_credentials
step z-criteria criteria
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
