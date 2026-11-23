package kube

import (
	"math"
	"slices"
	"time"
)

// Dist summarizes a set of measurements. Percentiles use the nearest-rank
// method, as in M5: the smallest value with at least p% of the values at or
// below it. It never reports a value that was not measured.
type Dist struct {
	N      int     `json:"n"`
	Min    float64 `json:"min"`
	Median float64 `json:"median"`
	P95    float64 `json:"p95"`
	P99    float64 `json:"p99"`
	Max    float64 `json:"max"`
}

// Summarize computes a Dist. An empty set gives all zeros, with N saying so.
func Summarize(values []float64) Dist {
	if len(values) == 0 {
		return Dist{}
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return Dist{
		N: len(sorted), Min: sorted[0], Median: percentile(sorted, 50),
		P95: percentile(sorted, 95), P99: percentile(sorted, 99), Max: sorted[len(sorted)-1],
	}
}

func percentile(sorted []float64, p float64) float64 {
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	return sorted[max(rank, 1)-1]
}

// ms turns a duration into milliseconds, kept to the microsecond.
func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
