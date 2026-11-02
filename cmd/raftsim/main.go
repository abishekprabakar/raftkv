// Command raftsim runs seeded simulation schedules against the Raft core and
// reports what happened. Every run is reproducible from its seed.
//
//	go run ./cmd/raftsim -first 1 -count 10000 -out results/sim.json
//	go run ./cmd/raftsim -first 4242 -count 1 -trace     # replay one seed
//
// It exits with status 1 if any schedule failed a check.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/abishekprabakar/raftkv/internal/hwinfo"
	"github.com/abishekprabakar/raftkv/internal/raft/sim"
)

var (
	first          = flag.Uint64("first", 1, "first seed")
	count          = flag.Int("count", 1000, "number of seeds")
	workers        = flag.Int("workers", 0, "parallel workers (0: one per CPU)")
	propertiesOnly = flag.Bool("properties-only", false, "switch off the Figure 2 monitors")
	trace          = flag.Bool("trace", false, "print every event (one seed only)")
	out            = flag.String("out", "", "write a JSON summary here")
)

type failure struct {
	Seed    uint64 `json:"seed"`
	Profile string `json:"profile"`
	sim.Violation
}

type report struct {
	GeneratedAt    string      `json:"generated_at"`
	Hardware       hwinfo.Info `json:"hardware"`
	FirstSeed      uint64      `json:"first_seed"`
	PropertiesOnly bool        `json:"properties_only"`
	WallSeconds    float64     `json:"wall_seconds"`
	sim.Summary
	Failures []failure `json:"failures,omitempty"` // the first few, for replaying
}

func main() {
	flag.Parse()
	opts := sim.Options{PropertiesOnly: *propertiesOnly}
	if *trace {
		if *count != 1 {
			fatal(fmt.Errorf("-trace needs -count 1"))
		}
		opts.Trace = os.Stdout
		fmt.Printf("%+v\n", sim.NewSchedule(*first))
	}
	start := time.Now()
	results := sim.RunRange(*first, *count, *workers, opts)
	rep := report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Hardware:    hwinfo.Collect("."), FirstSeed: *first, PropertiesOnly: *propertiesOnly,
		WallSeconds: time.Since(start).Seconds(),
		Summary:     sim.Summarize(results),
	}
	for _, r := range results {
		if r.Violation != nil && len(rep.Failures) < 20 {
			rep.Failures = append(rep.Failures, failure{r.Seed, r.Profile, *r.Violation})
		}
	}

	t := rep.Totals
	fmt.Printf("%d schedules, %d failed, %.1fs: %d events, %d sent, %d dropped, %d duplicated, %d long delays, %d reordered, %d partitions (%d one-way), %d crashes (%d during save, %d after save), %d leaders, %d entries committed\n",
		rep.Schedules, rep.Violations, rep.WallSeconds, t.Events, t.Sent, t.Dropped, t.Duplicated, t.LongDelayed, t.Reordered,
		t.Partitions, t.OneWayCuts, t.Crashes, t.CrashesDuringSave, t.CrashesAfterSave, t.LeadersElected, t.Committed)
	for _, c := range rep.ByCheck {
		fmt.Printf("  %-22s %6d schedules, first at seed %d\n", c.Check, c.Schedules, c.FirstSeed)
	}
	for i, f := range rep.Failures {
		if i == 3 {
			break
		}
		fmt.Printf("  seed %d (%s): %s at %dus on node %d: %s\n", f.Seed, f.Profile, f.Check, f.AtUs, f.Node, f.Detail)
	}
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			fatal(err)
		}
		data, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			fatal(err)
		}
		fmt.Println("wrote", *out)
	}
	if rep.Violations > 0 {
		os.Exit(1)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
