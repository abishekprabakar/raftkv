#!/bin/sh
# Runs D1 to D4 of docs/acceptance/M4.md against Docker Compose. Run it from the
# repository root:
#
#   scripts/audit/m4/compose.sh
#
# It starts from no raftkv containers or volumes, and removes them at the end.
# Every check is recorded in results/audit/m4/compose-summary.txt; a failed check
# does not stop the script.
set -u

repo=$(pwd)
out="${M4_OUT:-$repo/results/audit/m4}" # M4_OUT moves development runs elsewhere
mkdir -p "$out"
log="$out/compose.log"
summary="$out/compose-summary.txt"
rounds_file="$out/failover-rounds.tsv"
: >"$log"
: >"$summary"
printf 'round\tkilled\told_term\tnew_leader\tnew_term\tseconds_kill_to_write\tput\tnew_leader_ok\treads_ok\tcaught_up\n' >"$rounds_file"

all=node1:7000,node2:7000,node3:7000
rounds=5

now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }
since() { python3 -c "print(f'{$(now) - $1:.3f}')"; }
record() { echo "$*" | tee -a "$summary"; }

# kv NODE ARGS... runs the CLI inside NODE's container. That is the Compose
# network, where node names resolve, and starts faster than `docker compose run`,
# which matters when timing a failover.
kv() {
	node=$1
	shift
	docker compose exec -T "$node" /usr/local/bin/raftkv "$@"
}

# agreed VIA NEED prints "LEADER TERM" if at least NEED nodes answer and all of them
# name the same leader in the same term. Unreachable nodes print no " id=" line.
agreed() {
	kv "$1" --addr "$all" --timeout 2s status 2>>"$log" | awk -v need="$2" '
		/ id=/ {
			for (i = 2; i <= NF; i++) { split($i, kv, "="); f[kv[1]] = kv[2] }
			n++
			if (f["leader"] == 0) bad = 1
			seen[f["leader"] " " f["term"]] = 1
		}
		END {
			c = 0
			for (k in seen) { c++; last = k }
			if (n >= need && c == 1 && !bad) print last; else exit 1
		}'
}

# wait_agreed VIA NEED SECONDS retries agreed until it holds or time runs out.
wait_agreed() {
	deadline=$(($(date +%s) + $3))
	while [ "$(date +%s)" -le "$deadline" ]; do
		if agreed "$1" "$2"; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}

# field NODE NAME VIA prints one field of NODE's status line, asked from VIA.
field() {
	kv "$3" --addr "$1:7000" --timeout 2s status 2>>"$log" | tr ' ' '\n' | sed -n "s/^$2=//p"
}

{
	echo "commit: $(git rev-parse HEAD)"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "docker: $(docker version --format '{{.Server.Version}}')"
	echo "compose: $(docker compose version --short)"
	echo "uname: $(uname -srm)"
} >"$out/compose-env.txt"

# ---- D1: one command -----------------------------------------------------------
docker compose down -v --remove-orphans >>"$log" 2>&1
docker compose up -d --build --wait >>"$log" 2>&1
code=$?
t0=$(now)
if info=$(wait_agreed node1 3 30); then
	set -- $info
	leader=$1
	record "d1 pass up_exit=$code leader=$1 term=$2 seconds_until_agreed=$(since "$t0")"
else
	leader=0
	record "d1 FAIL up_exit=$code: the three nodes did not agree on a leader within 30 s"
fi
[ "$code" -eq 0 ] || record "d1 FAIL docker compose up exited $code"

# ---- D2: any node works --------------------------------------------------------
keys=""
for i in 1 2 3; do
	text=$(docker compose run --rm -T cli --addr "node$i:7000" -v put "m4-key-$i" "value-$i" 2>&1)
	code=$?
	echo "$text" >>"$log"
	redirects=$(echo "$text" | grep -c "redirected to node $leader at node$leader:7000")
	if [ "$i" -eq "$leader" ]; then want=0; else want=1; fi
	if [ "$code" -eq 0 ] && [ "$redirects" -ge "$want" ]; then verdict=pass; else verdict=FAIL; fi
	record "d2 put via node$i $verdict exit=$code redirects_to_leader=$redirects (leader is node$leader)"
	keys="$keys m4-key-$i=value-$i"
done
for i in 1 2 3; do
	for j in 1 2 3; do
		got=$(docker compose run --rm -T cli --addr "node$j:7000" get "m4-key-$i" 2>>"$log")
		if [ "$got" = "value-$i" ]; then verdict=pass; else verdict=FAIL; fi
		record "d2 get m4-key-$i via node$j $verdict got=[$got]"
	done
done

# ---- D3: killing the leader ----------------------------------------------------
for r in $(seq 1 $rounds); do
	if ! info=$(wait_agreed node1 3 30) && ! info=$(wait_agreed node2 3 30); then
		record "d3 round $r FAIL: no agreed leader before the kill"
		continue
	fi
	set -- $info
	old=$1
	old_term=$2
	survivor=$((old % 3 + 1))

	docker compose kill "node$old" >>"$log" 2>&1
	t_kill=$(now)
	text=$(kv "node$survivor" --addr "node$survivor:7000" --timeout 20s -v put "round-$r" "value-$r" 2>&1)
	put_code=$?
	took=$(since "$t_kill")
	echo "$text" >>"$log"
	keys="$keys round-$r=value-$r"
	[ "$put_code" -eq 0 ] && put=pass || put=FAIL

	# 2. The survivors name a new leader in a higher term.
	new=0
	new_term=0
	if info=$(wait_agreed "node$survivor" 2 10); then
		set -- $info
		new=$1
		new_term=$2
	fi
	if [ "$new" -ne 0 ] && [ "$new" -ne "$old" ] && [ "$new_term" -gt "$old_term" ]; then leader_ok=pass; else leader_ok=FAIL; fi
	commit=$(field "node$new" commit "node$survivor")

	# 3. Every key written so far reads back through a survivor.
	reads_ok=pass
	for kvpair in $keys; do
		key=${kvpair%%=*}
		want=${kvpair#*=}
		got=$(kv "node$survivor" --addr "node$survivor:7000" get "$key" 2>>"$log")
		if [ "$got" != "$want" ]; then
			reads_ok=FAIL
			echo "round $r: $key got [$got] want [$want]" >>"$log"
		fi
	done

	# 4. The killed node restarts from its volume and catches up.
	docker compose start "node$old" >>"$log" 2>&1
	caught_up=FAIL
	deadline=$(($(date +%s) + 30))
	while [ "$(date +%s)" -le "$deadline" ]; do
		applied=$(field "node$old" applied "node$survivor")
		if [ -n "$applied" ] && [ -n "$commit" ] && [ "$applied" -ge "$commit" ]; then
			caught_up=pass
			break
		fi
		sleep 0.5
	done

	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$r" "node$old" "$old_term" "node$new" "$new_term" "$took" \
		"$put" "$leader_ok" "$reads_ok" "$caught_up" >>"$rounds_file"
	if [ $put$leader_ok$reads_ok$caught_up = passpasspasspass ]; then verdict=pass; else verdict=FAIL; fi
	record "d3 round $r $verdict killed=node$old term=$old_term new_leader=node$new term=$new_term seconds_kill_to_write=$took put=$put new_leader=$leader_ok reads=$reads_ok caught_up=$caught_up (commit $commit)"
done

# ---- D4: tear down -------------------------------------------------------------
docker compose logs --no-color >"$out/compose-node-logs.txt" 2>&1
docker compose down -v >>"$log" 2>&1
code=$?
[ "$code" -eq 0 ] && verdict=pass || verdict=FAIL
record "d4 $verdict down_exit=$code"

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/compose-env.txt"
