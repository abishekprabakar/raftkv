#!/bin/sh
# The scripted stale read of docs/acceptance/M5.md (S1 and S2), on Docker Compose.
# Run it from the repository root:
#
#   scripts/audit/m5/stale-read.sh [ROUNDS]       # default 5 rounds
#
# RAFTKV_IMAGE picks the image to run (default raftkv:dev, which this script builds
# from the checkout unless NO_BUILD=1). OUT picks the results directory.
#
# Each round: start 3 nodes, write x=1, cut the leader off with docker network
# disconnect, wait for a new leader, write x=2 through it, then read x from the old
# leader. Passing means the old leader refused: no value printed, non-zero exit.
set -u

repo=$(pwd)
out="${OUT:-$repo/results/audit/m5/stale-read}"
rounds=${1:-5}
net=raftkv_default
all=node1:7000,node2:7000,node3:7000
mkdir -p "$out"
log="$out/log.txt"
summary="$out/summary.txt"
: >"$log"
: >"$summary"

record() { echo "$*" | tee -a "$summary"; }

# kv NODE ARGS... runs the CLI inside NODE's container. docker exec goes through the
# Docker daemon, not the network, so it reaches a node however it's cut off.
kv() {
	node=$1
	shift
	docker compose exec -T "$node" /usr/local/bin/raftkv "$@"
}

# own NODE FIELD prints one field of NODE's status, asked of NODE itself.
own() {
	kv "$1" --addr localhost:7000 --timeout 2s status 2>>"$log" | tr ' ' '\n' | sed -n "s/^$2=//p"
}

# leader prints the node that calls itself leader in the highest term.
leader() {
	best=0
	best_term=0
	for i in 1 2 3; do
		if [ "$(own "node$i" role)" = leader ]; then
			term=$(own "node$i" term)
			if [ "$term" -gt "$best_term" ]; then
				best=$i
				best_term=$term
			fi
		fi
	done
	echo "$best $best_term"
}

{
	echo "commit: $(git rev-parse HEAD)"
	echo "image: ${RAFTKV_IMAGE:-raftkv:dev}"
	echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$out/env.txt"
[ "${NO_BUILD:-0}" = 1 ] || docker compose build >>"$log" 2>&1

failed=0
for r in $(seq 1 "$rounds"); do
	echo "== round $r" >>"$log"
	docker compose down -v --remove-orphans >>"$log" 2>&1
	docker compose up -d --wait >>"$log" 2>&1

	# 1. Three nodes and x=1, once they agree on a leader.
	set -- 0 0
	for _ in $(seq 1 40); do
		set -- $(leader)
		[ "$1" -ne 0 ] && break
		sleep 0.5
	done
	old=$1
	old_term=$2
	kv node1 --addr "$all" --timeout 10s put x 1 >>"$log" 2>&1
	x1_index=$(own "node$old" commit)

	# 2. Cut the leader off.
	docker network disconnect "$net" "$(docker compose ps -q "node$old")" >>"$log" 2>&1

	# 3. The other two elect a new leader, and x=2 goes through it.
	new=0
	for _ in $(seq 1 40); do
		for i in 1 2 3; do
			if [ "$i" -ne "$old" ] && [ "$(own "node$i" role)" = leader ] && [ "$(own "node$i" term)" -gt "$old_term" ]; then
				new=$i
			fi
		done
		[ "$new" -ne 0 ] && break
		sleep 0.5
	done
	if [ "$new" -eq 0 ]; then
		record "round $r FAIL: no new leader within 20 s of cutting off node$old"
		failed=$((failed + 1))
		continue
	fi
	kv "node$new" --addr localhost:7000 --timeout 10s put x 2 >>"$log" 2>&1
	x2_code=$?

	# 4. The trap: the old leader still thinks it leads, and holds x=1.
	trap_role=$(own "node$old" role)
	trap_term=$(own "node$old" term)
	trap_applied=$(own "node$old" applied)

	# 5. Read x from the old leader, and only from it.
	value=$(kv "node$old" --addr localhost:7000 --timeout 5s -v get x 2>"$out/round-$r-read-stderr.txt")
	read_code=$?
	cat "$out/round-$r-read-stderr.txt" >>"$log"
	if [ "$read_code" -ne 0 ] && [ -z "$value" ]; then s1=pass; else s1=FAIL; fi

	# 6. Heal. The old leader steps down and x reads back as 2 through every node.
	docker network connect --alias "node$old" "$net" "$(docker compose ps -q "node$old")" >>"$log" 2>&1
	s2=FAIL
	for _ in $(seq 1 40); do
		if [ "$(own "node$old" role)" = follower ] && [ "$(own "node$old" term)" -gt "$old_term" ]; then
			s2=pass
			break
		fi
		sleep 0.5
	done
	reads=""
	for i in 1 2 3; do
		got=$(kv "node$i" --addr localhost:7000 --timeout 10s get x 2>>"$log")
		reads="$reads node$i=$got"
		[ "$got" = 2 ] || s2=FAIL
	done

	if [ "$trap_role" = leader ] && [ "$trap_term" = "$old_term" ] && [ "$trap_applied" -ge "$x1_index" ]; then
		trap_set=yes
	else
		trap_set=no
	fi
	[ "$s1$s2" = passpass ] || failed=$((failed + 1))
	record "round $r s1=$s1 s2=$s2 old_leader=node$old term=$old_term new_leader=node$new x2_write_exit=$x2_code" \
		"trap_set=$trap_set (old leader role=$trap_role term=$trap_term applied=$trap_applied, x=1 at index $x1_index)" \
		"read_from_old_leader: exit=$read_code value=[$value]; after heal:$reads"
done

docker compose logs --no-color >"$out/node-logs-last-round.txt" 2>&1
docker compose down -v >>"$log" 2>&1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$out/env.txt"
record "rounds=$rounds failed=$failed"
[ "$failed" -eq 0 ]
