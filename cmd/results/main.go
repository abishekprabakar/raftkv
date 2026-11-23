// Command results renders results/*.json into results/RESULTS.md and refreshes
// the results block in README.md. It is the only path by which a number
// reaches the documentation.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
)

var (
	resultsDir = flag.String("results", "results", "directory holding *.json")
	readme     = flag.String("readme", "README.md", "README to update between the results markers")
)

const (
	startMarker = "<!-- results:start -->"
	endMarker   = "<!-- results:end -->"
)

type doc map[string]any

func load(path string) (doc, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	var d doc
	if err := json.Unmarshal(data, &d); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
		return nil, false
	}
	return d, true
}

// get walks a dotted path; array indexes are plain numbers.
func (d doc) get(path string) any {
	var cur any = map[string]any(d)
	for _, part := range strings.Split(path, ".") {
		switch c := cur.(type) {
		case map[string]any:
			cur = c[part]
		case []any:
			var i int
			fmt.Sscanf(part, "%d", &i)
			if i < 0 || i >= len(c) {
				return nil
			}
			cur = c[i]
		default:
			return nil
		}
	}
	return cur
}

func (d doc) num(path string) float64 {
	if v, ok := d.get(path).(float64); ok {
		return v
	}
	return 0
}

func (d doc) str(path string) string {
	switch v := d.get(path).(type) {
	case string:
		return v
	case nil:
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func (d doc) ints(path string) string {
	arr, ok := d.get(path).([]any)
	if !ok {
		return ""
	}
	parts := make([]string, len(arr))
	for i, v := range arr {
		parts[i] = fmt.Sprintf("%.0f", v)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func commas(v float64) string {
	s := fmt.Sprintf("%.0f", v)
	n := len(s)
	if n <= 3 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (n-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}

func mb(bytes float64) string { return fmt.Sprintf("%.1f MB", bytes/(1<<20)) }

type row struct{ metric, value, source string }

func table(w *strings.Builder, header []string, rows [][]string) {
	w.WriteString("| " + strings.Join(header, " | ") + " |\n")
	w.WriteString("|" + strings.Repeat("---|", len(header)) + "\n")
	for _, r := range rows {
		w.WriteString("| " + strings.Join(r, " | ") + " |\n")
	}
	w.WriteString("\n")
}

func hardware(w *strings.Builder, d doc) {
	w.WriteString(fmt.Sprintf("Hardware: %s, %s cores, %.0f GB RAM, %s %s, filesystem %s, %s. Collected %s.\n\n",
		d.str("hardware.cpu"), d.str("hardware.cores"), d.num("hardware.memory_bytes")/(1<<30),
		d.str("hardware.os_version"), d.str("hardware.arch"), d.str("hardware.filesystem"), d.str("hardware.go_version"),
		d.str("hardware.collected_at")))
	if n := d.str("hardware.note"); n != "" {
		w.WriteString("Note: " + n + "\n\n")
	}
}

func storageSection(w *strings.Builder, d doc, full bool) {
	w.WriteString("## Storage engine (`make bench-storage` -> `results/storage.json`)\n\n")
	if full {
		hardware(w, d)
		w.WriteString(fmt.Sprintf("Workload: %s-byte keys, %s-byte values. Engine defaults: %s. Group commit window %s us.\n\n",
			d.str("workload.key_bytes"), d.str("workload.value_bytes"), d.str("workload.engine_defaults"), d.str("workload.group_commit_window_us")))
	}
	rows := [][]string{
		{"Durable write throughput (group commit, " + d.str("write.clients") + " clients)",
			commas(d.num("write.latency.ops_per_sec")) + " ops/s", "write.latency.ops_per_sec"},
		{"Durable write latency p50 / p99",
			fmt.Sprintf("%.1f ms / %.1f ms", d.num("write.latency.p50_us")/1000, d.num("write.latency.p99_us")/1000), "write.latency.p50_us, p99_us"},
		{"Records per fsync during the write run", fmt.Sprintf("%.1f", d.num("write.records_per_fsync")), "write.records_per_fsync"},
		{"Point read throughput (" + d.str("read.clients") + " clients, " + commas(d.num("read.keys_in_db")) + " keys, levels " + d.ints("read.level_files") + ")",
			commas(d.num("read.latency.ops_per_sec")) + " ops/s", "read.latency.ops_per_sec"},
		{"Point read latency p50 / p99",
			fmt.Sprintf("%.0f us / %.0f us", d.num("read.latency.p50_us"), d.num("read.latency.p99_us")), "read.latency.p50_us, p99_us"},
	}
	if full {
		table(w, []string{"Metric", "Value", "JSON field"}, rows)
	} else {
		table(w, []string{"Metric", "Value", "Field in results/storage.json"}, rows)
	}

	w.WriteString("### fsync per write vs group commit (" + d.str("workload.sync_mode_seconds_each") + " s each)\n\n")
	var sm [][]string
	if arr, ok := d.get("sync_modes").([]any); ok {
		for i := range arr {
			p := fmt.Sprintf("sync_modes.%d.", i)
			sm = append(sm, []string{
				d.str(p + "mode"), d.str(p + "clients"),
				commas(d.num(p + "latency.ops_per_sec")),
				fmt.Sprintf("%.2f", d.num(p+"latency.p50_us")/1000),
				fmt.Sprintf("%.2f", d.num(p+"latency.p99_us")/1000),
				fmt.Sprintf("%.1f", d.num(p+"records_per_fsync")),
				fmt.Sprintf("%.0f", d.num(p+"fsyncs_per_sec")),
			})
		}
	}
	table(w, []string{"Mode", "Clients", "ops/s", "p50 ms", "p99 ms", "records/fsync", "fsync/s"}, sm)

	w.WriteString("### Bloom filters: lookups for missing keys (" + commas(d.num("bloom.keys_in_db")) + " keys in DB, " +
		commas(d.num("bloom.with_bloom.lookups")) + " lookups, " + d.str("bloom.bits_per_key") + " bits/key)\n\n")
	table(w, []string{"Filter", "Levels", "Block reads", "Block reads / lookup", "False positive rate", "p50 us", "p99 us", "ops/s"}, [][]string{
		{"with bloom", d.ints("bloom.with_bloom.level_files"), commas(d.num("bloom.with_bloom.block_reads")),
			fmt.Sprintf("%.3f", d.num("bloom.with_bloom.block_reads_per_lookup")), fmt.Sprintf("%.4f", d.num("bloom.with_bloom.false_positive_rate")),
			fmt.Sprintf("%.0f", d.num("bloom.with_bloom.latency.p50_us")), fmt.Sprintf("%.0f", d.num("bloom.with_bloom.latency.p99_us")),
			commas(d.num("bloom.with_bloom.latency.ops_per_sec"))},
		{"without bloom", d.ints("bloom.without_bloom.level_files"), commas(d.num("bloom.without_bloom.block_reads")),
			fmt.Sprintf("%.3f", d.num("bloom.without_bloom.block_reads_per_lookup")), "n/a",
			fmt.Sprintf("%.0f", d.num("bloom.without_bloom.latency.p50_us")), fmt.Sprintf("%.0f", d.num("bloom.without_bloom.latency.p99_us")),
			commas(d.num("bloom.without_bloom.latency.ops_per_sec"))},
	})
	w.WriteString(fmt.Sprintf("Bloom filters cut disk block reads for missing keys by %.1fx (`bloom.disk_read_reduction_factor`).\n\n", d.num("bloom.disk_read_reduction_factor")))

	w.WriteString("### Write and space amplification (" + commas(d.num("amplification.writes")) + " writes over " +
		commas(d.num("amplification.unique_keys")) + " unique keys, " + d.str("amplification.sync_mode") + ")\n\n")
	table(w, []string{"Metric", "Value", "JSON field"}, [][]string{
		{"User bytes written", mb(d.num("amplification.user_bytes_written")), "amplification.user_bytes_written"},
		{"Live user bytes (unique keys)", mb(d.num("amplification.live_user_bytes")), "amplification.live_user_bytes"},
		{"WAL bytes", mb(d.num("amplification.wal_bytes")), "amplification.wal_bytes"},
		{"Flush bytes", mb(d.num("amplification.flush_bytes")), "amplification.flush_bytes"},
		{"Compaction bytes written (" + d.str("amplification.compactions") + " compactions, " + d.str("amplification.trivial_moves") + " trivial moves)",
			mb(d.num("amplification.compaction_bytes_written")), "amplification.compaction_bytes_written"},
		{"Write amplification, total (WAL + flush + compaction)", fmt.Sprintf("%.2fx", d.num("amplification.write_amplification_total")), "amplification.write_amplification_total"},
		{"Write amplification, SSTables only", fmt.Sprintf("%.2fx", d.num("amplification.write_amplification_sst_only")), "amplification.write_amplification_sst_only"},
		{"Space amplification, steady state (levels " + d.ints("amplification.steady_state.level_files") + ")",
			fmt.Sprintf("%.2fx", d.num("amplification.steady_state.space_amplification")), "amplification.steady_state.space_amplification"},
		{"Space amplification after full compaction (levels " + d.ints("amplification.after_full_compaction.level_files") + ")",
			fmt.Sprintf("%.2fx", d.num("amplification.after_full_compaction.space_amplification")), "amplification.after_full_compaction.space_amplification"},
	})

	w.WriteString("### WAL recovery\n\n")
	table(w, []string{"Metric", "Value", "JSON field"}, [][]string{
		{"WAL replayed", commas(d.num("recovery.wal_records")) + " records, " + mb(d.num("recovery.wal_bytes")), "recovery.wal_records, wal_bytes"},
		{"Replay time (read + checksum + rebuild memtable)", fmt.Sprintf("%.0f ms", d.num("recovery.replay_ms")), "recovery.replay_ms"},
		{"Replay rate", fmt.Sprintf("%.0f MB/s, %s records/s", d.num("recovery.replay_mb_per_sec"), commas(d.num("recovery.replay_records_per_sec"))), "recovery.replay_mb_per_sec"},
		{"Open() including flush of recovered data to L0", fmt.Sprintf("%.0f ms", d.num("recovery.open_ms_including_flush")), "recovery.open_ms_including_flush"},
	})
}

// roughly rounds down to a figure that holds across runs. The acknowledged-write
// count depends on OS scheduling, so the README must not present one run's exact
// count as a property of the system. RESULTS.md keeps the exact record.
func roughly(v float64) string { return commas(math.Floor(v/10000) * 10000) }

func crashSection(w *strings.Builder, d doc, full bool) {
	w.WriteString("## Crash recovery harness (`make crash-test` -> `results/crash.json`)\n\n")
	if full {
		hardware(w, d)
		w.WriteString(fmt.Sprintf("Each trial starts a child process with %s writer goroutines over %s keys each (memtable %s bytes, L0 trigger %s, %s) and kills it with %s after a random delay (%s). It adds a torn WAL tail in %s, kills recovery %s times in a row (%s), then reopens the directory and verifies acknowledged writes, prefix consistency per writer and every value checksum. Seed %s.\n\n",
			d.str("config.writers"), d.str("config.keys_per_writer"), d.str("config.memtable_bytes"), d.str("config.l0_compaction_trigger"),
			d.str("config.sync_mode"), d.str("config.kill_signal"), d.str("config.kill_delay_ms"),
			d.str("config.torn_tail_chance"), d.str("config.recovery_kills_per_trial"), d.str("config.recovery_kill_delay_ms"), d.str("config.seed")))
		w.WriteString("SIGKILL leaves the OS page cache intact, so this harness cannot detect a missing fsync and says nothing about power loss.\n\n")
	}
	ackedWrites := commas(d.num("total_acked_ops"))
	if !full {
		ackedWrites = "more than " + roughly(d.num("total_acked_ops")) + " per run; the exact count varies with OS scheduling"
	}
	table(w, []string{"Metric", "Value", "JSON field"}, [][]string{
		{"Trials (SIGKILL at a random point)", commas(d.num("trials")), "trials"},
		{"Passed / failed", commas(d.num("passed")) + " / " + commas(d.num("failed")), "passed, failed"},
		{"Acknowledged writes checked against the recovered state", ackedWrites, "total_acked_ops"},
		{"Stored values checksum-validated", commas(d.num("total_values_validated")), "total_values_validated"},
		{"Trials with data already compacted below L0 at the kill", commas(d.num("trials_with_data_below_l0")), "trials_with_data_below_l0"},
		{"Trials killed before the first ack", commas(d.num("trials_killed_before_first_ack")), "trials_killed_before_first_ack"},
		{"Recovery kills (total / during Open with WAL data present)", commas(d.num("recovery_kills")) + " / " + commas(d.num("recovery_kills_mid_open")), "recovery_kills, recovery_kills_mid_open"},
		{"Durable but unacknowledged writes seen (total / max per trial)", commas(d.num("total_unacked_durable_ops")) + " / " + commas(d.num("max_unacked_durable_ops")), "total_unacked_durable_ops, max_unacked_durable_ops"},
		{"Torn WAL tails injected", commas(d.num("torn_tails_injected")), "torn_tails_injected"},
		{"Torn tails seen truncated by the final Open (a recovery child truncates the rest)", commas(d.num("trials_with_truncated_wal_tail")), "trials_with_truncated_wal_tail"},
		{"Wall time", fmt.Sprintf("%.0f s", d.num("wall_seconds")), "wall_seconds"},
	})
}

// plantedBugs are the rules broken on purpose in scripts/audit/m3, in the order
// docs/acceptance/M2.md and M3.md list them.
var plantedBugs = []string{
	"send-before-save", "forget-voted-for", "count-old-terms", "always-truncate", "commit-to-last-index",
	"no-up-to-date-check", "commit-can-decrease", "fixed-timeout", "save-after-apply",
	"plain-leader-read", "read-skips-term-check", "no-dedup", "sessions-not-in-snapshot", "session-outside-batch",
	"compact-without-flush", "install-keeps-conflicting-log", "stale-snapshot-rolls-back", "restore-position-lost",
	"save-before-restore",
}

// checksFired lists which checks fired in a raftsim report, most frequent first.
func checksFired(d doc) string {
	arr, ok := d.get("by_check").([]any)
	if !ok || len(arr) == 0 {
		return "none"
	}
	var parts []string
	for i := range arr {
		p := fmt.Sprintf("by_check.%d.", i)
		parts = append(parts, fmt.Sprintf("%s (%s)", d.str(p+"check"), commas(d.num(p+"schedules"))))
	}
	return strings.Join(parts, ", ")
}

func simSection(w *strings.Builder, d doc, full bool) {
	w.WriteString("## Raft simulation (`make sim` -> `results/sim.json`)\n\n")
	if full {
		hardware(w, d)
		w.WriteString("Each schedule runs real Raft nodes and KV servers on a simulated network with a virtual clock, with simulated clients reading and writing, so one seed fixes the whole run and any failure replays exactly. After the faults stop, every schedule must elect a leader, bring every node up to date and end with identical state machines. The simulator stands in for both disks and models crashes during a save and power cuts, so this says nothing about real fsync behavior.\n\n")
	}
	table(w, []string{"Metric", "Value", "JSON field"}, [][]string{
		{"Schedules (first seed " + d.str("first_seed") + ")", commas(d.num("schedules")), "schedules"},
		{"Schedules that failed a check", commas(d.num("violations")), "violations"},
		{"Messages sent", commas(d.num("totals.messages_sent")), "totals.messages_sent"},
		{"Messages dropped", commas(d.num("totals.messages_dropped")), "totals.messages_dropped"},
		{"Messages duplicated", commas(d.num("totals.messages_duplicated")), "totals.messages_duplicated"},
		{"Messages delayed past an election timeout", commas(d.num("totals.messages_delayed_past_election_timeout")), "totals.messages_delayed_past_election_timeout"},
		{"Messages reordered", commas(d.num("totals.messages_reordered")), "totals.messages_reordered"},
		{"Partitions (one-way cuts among them)", commas(d.num("totals.partitions")) + " (" + commas(d.num("totals.one_way_cuts")) + ")", "totals.partitions, one_way_cuts"},
		{"Crash-restarts (during a save / after a save, before sending)", commas(d.num("totals.crashes")) + " (" + commas(d.num("totals.crashes_during_save")) + " / " + commas(d.num("totals.crashes_after_save")) + ")", "totals.crashes, crashes_during_save, crashes_after_save"},
		{"Power-loss crashes", commas(d.num("totals.power_loss_crashes")), "totals.power_loss_crashes"},
		{"Leaders elected", commas(d.num("totals.leaders_elected")), "totals.leaders_elected"},
		{"Entries committed", commas(d.num("totals.entries_committed")), "totals.entries_committed"},
		{"Writes acknowledged to clients", commas(d.num("totals.writes_acknowledged")), "totals.writes_acknowledged"},
		{"Retried writes dropped by a session", commas(d.num("totals.retried_writes_dropped")), "totals.retried_writes_dropped"},
		{"Reads served through ReadIndex", commas(d.num("totals.reads_served")), "totals.reads_served"},
		{"Reads refused", commas(d.num("totals.reads_refused")), "totals.reads_refused"},
		{"Log compactions", commas(d.num("totals.log_compactions")), "totals.log_compactions"},
		{"Snapshots installed", commas(d.num("totals.snapshots_installed")), "totals.snapshots_installed"},
		{"Wall time", fmt.Sprintf("%.0f s", d.num("wall_seconds")), "wall_seconds"},
	})

	var rows [][]string
	for _, bug := range plantedBugs {
		if all, ok := load(filepath.Join(*resultsDir, "audit", "m3", "q-"+bug+".json")); ok {
			rows = append(rows, []string{bug, checksFired(all)})
		}
	}
	if len(rows) > 0 {
		w.WriteString("### Planted bugs: which check caught each one (schedules failed, out of 2,000)\n\n")
		table(w, []string{"Bug", "Checks that fired"}, rows)
	}
}

func chaosSection(w *strings.Builder, d doc, full bool) {
	w.WriteString("## Partitions on real containers (`cmd/chaos` -> `results/chaos.json`)\n\n")
	if full {
		hardware(w, d)
		w.WriteString("Each schedule starts a fresh 3-node cluster on Docker Compose (Docker " + d.str("docker") + "), runs 6 clients for 20 s while `docker network disconnect` cuts off the leader, a follower or two nodes, heals, and then checks the whole history with Porcupine. Clients run inside the nodes' containers, so a cut-off node still gets requests. The seed fixes the faults and every client's operations, not how real processes interleave. Failover time runs from Docker's disconnect event for the leader to the first log line in which another node becomes leader, both on the Docker VM's clock.\n\n")
	}
	ms := func(path string) string { return fmt.Sprintf("%.0f ms", d.num(path)) }
	table(w, []string{"Metric", "Value", "JSON field"}, [][]string{
		{"Schedules (first seed " + d.str("first_seed") + ")", commas(d.num("runs")), "runs"},
		{"Histories Porcupine found not linearizable", commas(d.num("violations")), "violations"},
		{"Checks that timed out / runs that broke", commas(d.num("inconclusive")) + " / " + commas(d.num("errors")), "inconclusive, errors"},
		{"Runs with no agreed leader 20 s after the final heal", commas(d.num("not_recovered")), "not_recovered"},
		{"Partitions: leader / follower / two nodes cut off", commas(d.num("faults.leader")) + " / " + commas(d.num("faults.follower")) + " / " + commas(d.num("faults.two")), "faults"},
		{"Writes acknowledged / outcome unknown", commas(d.num("totals.writes_acknowledged")) + " / " + commas(d.num("totals.writes_unknown")), "totals.writes_*"},
		{"Reads served / refused or timed out", commas(d.num("totals.reads_served")) + " / " + commas(d.num("totals.reads_refused_or_timed_out")), "totals.reads_*"},
		{"Leader isolations measured / with no new leader before the heal", commas(d.num("failover.measured")) + " / " + commas(d.num("failover.no_new_leader")), "failover.measured, no_new_leader"},
		{"Failover time: median / p95 / max", ms("failover.median_ms") + " / " + ms("failover.p95_ms") + " / " + ms("failover.max_ms"), "failover.*_ms"},
		// Go's clock on macOS stops while the machine sleeps, and the Mac slept
		// during the M5 run, so this is time spent running, not time on the wall.
		{"Running time, not counting time the machine slept", fmt.Sprintf("%.0f s", d.num("wall_seconds")), "wall_seconds"},
	})
	if planted, ok := load(filepath.Join(*resultsDir, "audit", "m5", "planted-plain-leader-read.json")); ok {
		fmt.Fprintf(w, "With the `plain-leader-read` bug planted, Porcupine found %s violations in %s schedules (`results/audit/m5/planted-plain-leader-read.json`).\n\n",
			commas(planted.num("violations")), commas(planted.num("runs")))
	}
}

// k8sDocs are the reports cmd/kubebench writes, one per measurement.
type k8sDocs struct {
	checks, failover, restart, rollout, cold doc
}

func loadK8s() (k8sDocs, bool) {
	var k k8sDocs
	found := false
	for name, dst := range map[string]*doc{
		"k8s-checks.json": &k.checks, "k8s-failover.json": &k.failover, "k8s-restart.json": &k.restart,
		"k8s-rollout.json": &k.rollout, "k8s-coldstart.json": &k.cold,
	} {
		if d, ok := load(filepath.Join(*resultsDir, name)); ok {
			*dst, found = d, true
		}
	}
	return k, found
}

func k8sSection(w *strings.Builder, k k8sDocs, full bool) {
	w.WriteString("## Kubernetes on kind, deployed with Terraform (`make kube-audit` -> `results/k8s-*.json`)\n\n")
	msOf := func(d doc, path string, parts ...string) string {
		vals := make([]string, len(parts))
		for i, part := range parts {
			vals[i] = fmt.Sprintf("%.0f ms", d.num(path+"."+part))
		}
		return strings.Join(vals, " / ")
	}
	secondsOf := func(d doc, path string, parts ...string) string {
		vals := make([]string, len(parts))
		for i, part := range parts {
			vals[i] = fmt.Sprintf("%.1f s", d.num(path+"."+part))
		}
		return strings.Join(vals, " / ")
	}
	of := func(d doc, a, b string) string { return commas(d.num(a)) + " of " + commas(d.num(b)) }

	if f := k.failover; f != nil {
		if full {
			hardware(w, f)
			fmt.Fprintf(w, "Versions: Docker %s with %s CPUs and %.1f GB for its VM, Kubernetes %s (`%s`), kubectl %s, Terraform %s. Source commit `%s`.\n\n",
				f.str("versions.docker"), f.str("versions.docker_cpus"), f.num("versions.docker_memory_bytes")/(1<<30),
				f.str("versions.kubernetes"), f.str("versions.kind_node_image"), f.str("versions.kubectl"), f.str("versions.terraform"),
				f.str("versions.source_commit"))
			w.WriteString("One `terraform apply` creates a one-node kind cluster inside Docker and deploys a StatefulSet of 3 raftkv pods, each with its own volume claim. All three pods share one Kubernetes node, one VM and one disk, so these numbers describe this laptop setup and nothing larger. " +
				"`cmd/kubebench` runs on the host and reaches the pods through ports kind maps: it starts every `kubectl` and `terraform` command itself, polls each node's Status over gRPC (the measured interval is below), and runs 6 clients that append and read on 4 keys, replaced every 2 s. Every time is read from that one process's clock. " +
				"A request fails when the client library gives up on it after 4 s of redirects and retries.\n\n")
			w.WriteString("### Leader failover: `kubectl delete pod` on the leader, under load\n\n")
			w.WriteString("Each trial waits for 3 Ready pods and an agreed leader, runs the load for 3 s, then deletes the leader's pod with a normal `kubectl delete pod --wait=false`. " +
				"Both times start just before kubectl is started. The time to a new leader ends at the first poll in which any node reports itself leader in a higher term. " +
				"The time to the first successful write ends when the first write is acknowledged in a higher term, which every acknowledgement carries. It includes the client library's retry pauses, which double up to 500 ms with no jitter.\n\n")
		}
		rows := [][]string{
			{"Leader failover trials (`kubectl delete pod` on the leader, under load)", commas(f.num("summary.trials")), "k8s-failover: summary.trials"},
			{"Trials that elected a new leader, took a write, and settled with 3 Ready pods", of(f, "summary.passed", "summary.trials"), "summary.passed"},
			{"Time to a new leader: median / p95 / max", msOf(f, "summary.new_leader_ms", "median", "p95", "max"), "summary.new_leader_ms"},
			{"Time to the first successful write: median / p95 / max", msOf(f, "summary.first_write_ms", "median", "p95", "max"), "summary.first_write_ms"},
			{"Acknowledged writes lost", of(f, "durability.acknowledged_writes_lost", "durability.writes_acknowledged"), "durability.acknowledged_writes_lost"},
			{"Writes applied twice / values nobody wrote", commas(f.num("durability.writes_duplicated")) + " / " + commas(f.num("durability.values_never_written")), "durability.writes_duplicated, values_never_written"},
			{"Porcupine on the whole history (" + commas(f.num("load.requests")) + " requests, " + commas(f.num("durability.keys")) + " keys)", f.str("linearizable"), "linearizable"},
			{"Failed client requests across the " + commas(f.num("summary.trials")) + " failovers", of(f, "load.failed_requests", "load.requests"), "load.failed_requests"},
			{"Replacement pods that started from their own volume", fmt.Sprintf("%s of %s", commas(f.num("summary.replacement_pods")-f.num("summary.replacement_did_not_start_from_its_volume")), commas(f.num("summary.replacement_pods"))), "summary.replacement_did_not_start_from_its_volume"},
		}
		if full {
			rows = append(rows, [][]string{
				{"Time to a new leader: minimum", msOf(f, "summary.new_leader_ms", "min"), "summary.new_leader_ms.min"},
				{"Time to the first successful write: minimum", msOf(f, "summary.first_write_ms", "min"), "summary.first_write_ms.min"},
				{"The old leader's last Status answer after the delete: median / max", msOf(f, "summary.old_leader_last_answer_ms", "median", "max"), "summary.old_leader_last_answer_ms"},
				{"`kubectl delete --wait=false` returned: median", msOf(f, "summary.kubectl_delete_returned_ms", "median"), "summary.kubectl_delete_returned_ms"},
				{"All 3 pods Ready again: median / p95 / max", msOf(f, "summary.all_pods_ready_ms", "median", "p95", "max"), "summary.all_pods_ready_ms"},
				{"Longest stretch with no write acknowledged, per trial: median / max", msOf(f, "summary.longest_write_gap_ms", "median", "max"), "summary.longest_write_gap_ms"},
				{"Trials in which the rejoining pod forced another election", of(f, "summary.trials_with_extra_leader_changes", "summary.trials"), "summary.trials_with_extra_leader_changes"},
				{"Requests that needed a retry / retries in all", commas(f.num("load.requests_that_retried")) + " / " + commas(f.num("load.retries")), "load.requests_that_retried, retries"},
				{"Writes with an unknown outcome", commas(f.num("durability.writes_unknown")), "durability.writes_unknown"},
				{"Status poll interval per node: median / p99", fmt.Sprintf("%.1f ms / %.1f ms", f.num("status_poll_interval_ms.median"), f.num("status_poll_interval_ms.p99")), "status_poll_interval_ms"},
				{"Running time", fmt.Sprintf("%.0f s", f.num("wall_seconds")), "wall_seconds"},
			}...)
		}
		table(w, []string{"Metric", "Value", "JSON field"}, rows)
	}

	if r := k.restart; r != nil {
		if full {
			w.WriteString("### Every pod deleted at once\n\n")
			w.WriteString("A failover leaves two nodes holding the data, so it would pass even without volumes. Here one `kubectl delete pod` removes all three pods, and what comes back can only come from the volume claims.\n\n")
		}
		rows := [][]string{
			{"Restarts of every pod at once that recovered, each pod from its volume", of(r, "summary.passed", "summary.trials"), "k8s-restart: summary.passed"},
			{"Acknowledged writes lost across those restarts", of(r, "durability.acknowledged_writes_lost", "durability.writes_acknowledged"), "durability.acknowledged_writes_lost"},
		}
		if full {
			rows = append(rows, [][]string{
				{"Writes applied twice / Porcupine", commas(r.num("durability.writes_duplicated")) + " / " + r.str("linearizable"), "durability.writes_duplicated, linearizable"},
				{"Time from the delete to a leader: median / max", msOf(r, "summary.new_leader_ms", "median", "max"), "summary.new_leader_ms"},
				{"Time from the delete to the first successful write: median / max", msOf(r, "summary.first_write_ms", "median", "max"), "summary.first_write_ms"},
				{"All 3 pods Ready again: median / max", msOf(r, "summary.all_pods_ready_ms", "median", "max"), "summary.all_pods_ready_ms"},
				{"Failed client requests while no pod was up", of(r, "load.failed_requests", "load.requests"), "load.failed_requests"},
			}...)
		}
		table(w, []string{"Metric", "Value", "JSON field"}, rows)
	}

	if u := k.rollout; u != nil {
		if full {
			w.WriteString("### Rolling update under load\n\n")
			w.WriteString("Each round runs `terraform apply -var image_tag=...` to switch between two images built from the same source with different version strings. The StatefulSet controller replaces one pod at a time, highest ordinal first, and waits for each to be ready. " +
				"The rollout time runs from the first watch event showing the new image in the StatefulSet to the first showing every replica updated and ready. The client numbers cover each round from that first event until the cluster is healthy again.\n\n")
		}
		rows := [][]string{
			{"Rolling updates under load that finished on the new version", of(u, "summary.passed", "summary.rounds"), "k8s-rollout: summary.passed"},
			{"Rollout time: median / min / max", secondsOf(u, "summary.rollout_seconds", "median", "min", "max"), "summary.rollout_seconds"},
			{"Failed client requests during the rollouts", of(u, "summary.failed_requests", "summary.requests"), "summary.failed_requests"},
			{"Longest any request took during a rollout", fmt.Sprintf("%.0f ms", u.num("summary.longest_request_ms")), "summary.longest_request_ms"},
			{"Leader changes per rollout: median / max", fmt.Sprintf("%.0f / %.0f", u.num("summary.leader_changes_per_rollout.median"), u.num("summary.leader_changes_per_rollout.max")), "summary.leader_changes_per_rollout"},
		}
		if full {
			rows = append(rows, [][]string{
				{"Requests that needed a retry / retries in all", commas(u.num("summary.requests_that_retried")) + " / " + commas(u.num("summary.retries")), "summary.requests_that_retried, retries"},
				{"Longest stretch with no write acknowledged, per rollout: median / max", msOf(u, "summary.longest_write_gap_ms", "median", "max"), "summary.longest_write_gap_ms"},
				{"`terraform apply` for the change: median / max", secondsOf(u, "summary.terraform_apply_seconds", "median", "max"), "summary.terraform_apply_seconds"},
				{"Acknowledged writes lost over the whole run", of(u, "durability.acknowledged_writes_lost", "durability.writes_acknowledged"), "durability.acknowledged_writes_lost"},
				{"Writes applied twice / Porcupine", commas(u.num("durability.writes_duplicated")) + " / " + u.str("linearizable"), "durability.writes_duplicated, linearizable"},
			}...)
		}
		table(w, []string{"Metric", "Value", "JSON field"}, rows)
	}

	if c := k.cold; c != nil {
		if full {
			w.WriteString("### Cold start\n\n")
			w.WriteString("Each round runs `terraform destroy`, checks that no node container and no Terraform state are left, and then times one `terraform apply`. The clock stops at the first moment all 3 pods are Ready and all 3 nodes name the same leader. " +
				"The kind node image was already in Docker's image store and Docker's build cache was warm, so this leaves out a first download of the node image and a first compile.\n\n")
		}
		rows := [][]string{
			{"Cold starts that reached 3 Ready pods and a leader with one `terraform apply`", of(c, "summary.passed", "summary.rounds"), "k8s-coldstart: summary.passed"},
			{"Cold start, `terraform apply` to 3 Ready pods with a leader: median / min / max", secondsOf(c, "summary.ready_with_leader_seconds", "median", "min", "max"), "summary.ready_with_leader_seconds"},
		}
		if full {
			rows = append(rows, [][]string{
				{"kind cluster created and its API server answering: median", secondsOf(c, "summary.api_server_answered_seconds", "median"), "summary.api_server_answered_seconds"},
				{"All 3 pods created: median", secondsOf(c, "summary.pods_created_seconds", "median"), "summary.pods_created_seconds"},
				{"`terraform apply` returned: median / max", secondsOf(c, "summary.terraform_apply_seconds", "median", "max"), "summary.terraform_apply_seconds"},
				{"`terraform destroy`: median / max", secondsOf(c, "summary.terraform_destroy_seconds", "median", "max"), "summary.terraform_destroy_seconds"},
			}...)
		}
		table(w, []string{"Metric", "Value", "JSON field"}, rows)
	}

	if c := k.checks; c != nil {
		row := []string{"Live checks passed: objects, client Service, readiness without a majority, disruption budget under a drain",
			fmt.Sprintf("%s of %s", commas(c.num("passed")), commas(c.num("passed")+c.num("failed"))), "k8s-checks: passed, failed"}
		if !full {
			table(w, []string{"Metric", "Value", "JSON field"}, [][]string{row})
		} else {
			w.WriteString("### Live checks\n\n")
			var rows [][]string
			if arr, ok := c.get("checks").([]any); ok {
				for i := range arr {
					p := fmt.Sprintf("checks.%d.", i)
					verdict := "pass"
					if c.get(p+"pass") != true {
						verdict = "FAIL"
					}
					rows = append(rows, []string{c.str(p + "id"), verdict, c.str(p + "detail")})
				}
			}
			table(w, []string{"Check", "Result", "What happened"}, rows)
		}
	}

	audit := filepath.Join(*resultsDir, "audit", "k8s")
	if p, ok := load(filepath.Join(audit, "planted-no-dedup.json")); ok {
		fmt.Fprintf(w, "With the `no-dedup` bug planted, %s failover trials left %s writes applied twice, and Porcupine's answer was %s (`results/audit/k8s/planted-no-dedup.json`).\n\n",
			commas(p.num("summary.trials")), commas(p.num("durability.writes_duplicated")), p.str("linearizable"))
	}
	if p, ok := load(filepath.Join(audit, "planted-emptydir.json")); ok {
		fmt.Fprintf(w, "With the data on an `emptyDir` instead of the volume claims, deleting every pod %s times lost %s of %s acknowledged writes (`results/audit/k8s/planted-emptydir.json`).\n\n",
			commas(p.num("summary.trials")), commas(p.num("durability.acknowledged_writes_lost")), commas(p.num("durability.writes_acknowledged")))
	}
}

func main() {
	flag.Parse()
	storage, haveStorage := load(filepath.Join(*resultsDir, "storage.json"))
	crash, haveCrash := load(filepath.Join(*resultsDir, "crash.json"))
	simulation, haveSim := load(filepath.Join(*resultsDir, "sim.json"))
	chaosRuns, haveChaos := load(filepath.Join(*resultsDir, "chaos.json"))
	k8s, haveK8s := loadK8s()

	var full, short strings.Builder
	full.WriteString("# Measured results\n\nEvery number below is read from a JSON file produced by a tool in `cmd/`; regenerate this file with `make results`.\n\n")
	short.WriteString(startMarker + "\n")
	short.WriteString("_Generated from `results/*.json` by `go run ./cmd/results`; see [results/RESULTS.md](results/RESULTS.md) for hardware and methodology._\n\n")
	if haveStorage {
		storageSection(&full, storage, true)
		storageSection(&short, storage, false)
	}
	if haveCrash {
		crashSection(&full, crash, true)
		crashSection(&short, crash, false)
	}
	if haveSim {
		simSection(&full, simulation, true)
		simSection(&short, simulation, false)
	}
	if haveChaos {
		chaosSection(&full, chaosRuns, true)
		chaosSection(&short, chaosRuns, false)
	}
	if haveK8s {
		k8sSection(&full, k8s, true)
		k8sSection(&short, k8s, false)
	}
	short.WriteString(endMarker)

	if err := os.WriteFile(filepath.Join(*resultsDir, "RESULTS.md"), []byte(full.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("wrote", filepath.Join(*resultsDir, "RESULTS.md"))

	if data, err := os.ReadFile(*readme); err == nil {
		s := string(data)
		i, j := strings.Index(s, startMarker), strings.Index(s, endMarker)
		if i >= 0 && j > i {
			s = s[:i] + short.String() + s[j+len(endMarker):]
			if err := os.WriteFile(*readme, []byte(s), 0o644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			fmt.Println("updated results block in", *readme)
		}
	}
}
