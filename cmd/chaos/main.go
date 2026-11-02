// Command chaos runs seeded fault schedules against a 3-node raftkv cluster on
// Docker Compose and checks each run's history with Porcupine. See
// docs/acceptance/M5.md for what one schedule does.
//
//	go run ./cmd/chaos -first 1 -count 100 -out results/chaos.json
//
// Each schedule starts a fresh cluster and tears it down after. Logs, Docker's
// network events and the full history of every run go under -artifacts. It exits
// with status 1 if any run was not linearizable, couldn't be checked, or broke.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/abishekprabakar/raftkv/internal/chaos"
	"github.com/abishekprabakar/raftkv/internal/hwinfo"
)

type report struct {
	GeneratedAt string      `json:"generated_at"`
	Hardware    hwinfo.Info `json:"hardware"`
	Docker      string      `json:"docker"`
	Image       string      `json:"image"`
	FirstSeed   uint64      `json:"first_seed"`
	Count       int         `json:"count"`
	WallSeconds float64     `json:"wall_seconds"`
	chaos.Summary
	Results []chaos.RunResult `json:"results"`
}

func main() {
	first := flag.Uint64("first", 1, "first seed")
	count := flag.Int("count", 1, "number of seeds")
	image := flag.String("image", "raftkv:dev", "image to run the nodes on")
	build := flag.Bool("build", true, "build the image from this checkout first (with docker compose build)")
	out := flag.String("out", "", "write a JSON report here, updated after every run")
	artifacts := flag.String("artifacts", "results/audit/m5/runs", "directory for each run's logs and history")
	logPath := flag.String("log", "", "write Docker's output here (default: discard)")
	flag.Parse()

	logw, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if *logPath != "" {
		logw, err = os.Create(*logPath)
	}
	if err != nil {
		fatal(err)
	}
	defer logw.Close()

	if *build {
		cmd := exec.Command("docker", "compose", "build")
		cmd.Env = append(os.Environ(), "RAFTKV_IMAGE="+*image)
		cmd.Stdout, cmd.Stderr = logw, logw
		if err := cmd.Run(); err != nil {
			fatal(fmt.Errorf("docker compose build: %w", err))
		}
	}
	dockerVersion, _ := exec.Command("docker", "version", "--format", "{{.Server.Version}}").Output()

	runner := &chaos.Runner{Dir: ".", Image: *image, Artifacts: *artifacts, Log: logw}
	rep := report{
		Hardware: hwinfo.Collect("."), Docker: strings.TrimSpace(string(dockerVersion)),
		Image: *image, FirstSeed: *first, Count: *count,
	}
	start := time.Now()
	for i := 0; i < *count; i++ {
		seed := *first + uint64(i)
		r := runner.Run(seed)
		rep.Results = append(rep.Results, r)
		fmt.Printf("seed %d: %s, %d faults, writes %d acked / %d unknown, reads %d served / %d failed, recovered %v (%.1fs), final reads ok %v, check %.1fs%s\n",
			seed, r.Linearizable, len(r.Faults), r.Counts.WritesAcked, r.Counts.WritesUnknown, r.Counts.ReadsServed, r.Counts.ReadsFailed,
			r.Recovered, r.RecoverySeconds, r.FinalReadsOK, r.CheckSeconds, errText(r.Error))
		rep.Summary = chaos.Summarize(rep.Results)
		rep.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
		rep.WallSeconds = time.Since(start).Seconds()
		if *out != "" {
			write(*out, rep)
		}
	}
	s := rep.Summary
	fmt.Printf("%d runs: %d violations, %d inconclusive, %d errors, %d not recovered, %d with failed final reads\n",
		s.Runs, s.Violations, s.Inconclusive, s.Errors, s.NotRecovered, s.FinalReadsFailed)
	fmt.Printf("faults %v (%d skipped); failover over %d leader isolations: %d measured, median %.0f ms, p95 %.0f ms, max %.0f ms, %d with no new leader\n",
		s.Faults, s.FaultsSkipped, s.Failover.LeaderIsolations, s.Failover.Measured, s.Failover.MedianMs, s.Failover.P95Ms, s.Failover.MaxMs, s.Failover.NoNewLeader)
	if s.Violations+s.Inconclusive+s.Errors > 0 {
		os.Exit(1)
	}
}

func errText(e string) string {
	if e == "" {
		return ""
	}
	return ", ERROR: " + e
}

func write(path string, rep report) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatal(err)
	}
	data, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "chaos:", err)
	os.Exit(1)
}
