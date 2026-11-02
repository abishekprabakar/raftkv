package chaos

import (
	"math"
	"slices"

	"github.com/anishathalye/porcupine"
)

// Summary totals a set of runs. Every number in results/chaos.json comes from
// here, computed from the per-run results stored beside it.
type Summary struct {
	Runs             int               `json:"runs"`
	Violations       int               `json:"violations"`   // Porcupine found no valid order
	Inconclusive     int               `json:"inconclusive"` // Porcupine timed out
	Errors           int               `json:"errors"`       // the run itself broke
	NotRecovered     int               `json:"not_recovered"`
	FinalReadsFailed int               `json:"final_reads_failed"`
	Faults           map[FaultKind]int `json:"faults"`
	FaultsSkipped    int               `json:"faults_skipped"`
	Totals           Counts            `json:"totals"`
	Failover         FailoverSummary   `json:"failover"`
	CheckSecondsMax  float64           `json:"check_seconds_max"`
}

// FailoverSummary describes the leader isolations whose new leader was found in
// the logs. NoNewLeader counts the ones where no other node took over before the
// partition healed.
type FailoverSummary struct {
	LeaderIsolations int     `json:"leader_isolations"`
	Measured         int     `json:"measured"`
	NoNewLeader      int     `json:"no_new_leader"`
	Unmatched        int     `json:"unmatched"` // no Docker event to measure from
	MinMs            float64 `json:"min_ms"`
	MedianMs         float64 `json:"median_ms"`
	P95Ms            float64 `json:"p95_ms"`
	MaxMs            float64 `json:"max_ms"`
}

func Summarize(runs []RunResult) Summary {
	s := Summary{Runs: len(runs), Faults: map[FaultKind]int{}}
	var ms []float64
	for _, r := range runs {
		switch {
		case r.Error != "":
			s.Errors++
		case r.Linearizable == porcupine.Illegal:
			s.Violations++
		case r.Linearizable == porcupine.Unknown:
			s.Inconclusive++
		}
		if r.Error == "" && !r.Recovered {
			s.NotRecovered++
		}
		if r.Error == "" && !r.FinalReadsOK {
			s.FinalReadsFailed++
		}
		for _, f := range r.Faults {
			if f.Skipped != "" {
				s.FaultsSkipped++
			} else {
				s.Faults[f.Kind]++
			}
		}
		s.Totals.add(r.Counts)
		s.CheckSecondsMax = max(s.CheckSecondsMax, r.CheckSeconds)
		for _, f := range r.Failovers {
			s.Failover.LeaderIsolations++
			switch {
			case f.NewLeader != 0:
				ms = append(ms, f.Ms)
			case f.Note == "no new leader before the heal":
				s.Failover.NoNewLeader++
			default:
				s.Failover.Unmatched++
			}
		}
	}
	slices.Sort(ms)
	s.Failover.Measured = len(ms)
	if len(ms) > 0 {
		s.Failover.MinMs = ms[0]
		s.Failover.MedianMs = percentile(ms, 50)
		s.Failover.P95Ms = percentile(ms, 95)
		s.Failover.MaxMs = ms[len(ms)-1]
	}
	return s
}

// percentile uses the nearest-rank method: the smallest value with at least p% of
// the values at or below it. It never invents a value between two real ones.
func percentile(sorted []float64, p float64) float64 {
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	return sorted[max(rank, 1)-1]
}
