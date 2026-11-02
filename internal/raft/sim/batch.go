package sim

import (
	"runtime"
	"sort"
	"sync"
)

// RunRange runs count schedules from first, on workers goroutines, and returns
// the results in seed order. Each schedule is self-contained, so the number of
// workers changes how fast this goes and nothing else. workers <= 0 uses every
// CPU.
func RunRange(first uint64, count, workers int, opts Options) []Result {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	results := make([]Result, count)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = Run(first+uint64(i), opts)
			}
		}()
	}
	for i := 0; i < count; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results
}

// CheckCount is how often one check fired first, with the first seed it fired on.
type CheckCount struct {
	Check     string `json:"check"`
	Schedules int    `json:"schedules"`
	FirstSeed uint64 `json:"first_seed"`
}

// Summary totals a batch of results.
type Summary struct {
	Schedules  int            `json:"schedules"`
	Violations int            `json:"violations"`
	ByCheck    []CheckCount   `json:"by_check"`
	ByProfile  map[string]int `json:"schedules_by_profile"`
	Totals     Stats          `json:"totals"`
}

// Summarize totals results, which must be in seed order.
func Summarize(results []Result) Summary {
	sum := Summary{Schedules: len(results), ByProfile: map[string]int{}, ByCheck: []CheckCount{}}
	counts := map[string]*CheckCount{}
	for _, r := range results {
		sum.ByProfile[r.Profile]++
		sum.Totals.add(r.Stats)
		if r.Violation == nil {
			continue
		}
		sum.Violations++
		c := counts[r.Violation.Check]
		if c == nil {
			c = &CheckCount{Check: r.Violation.Check, FirstSeed: r.Seed}
			counts[r.Violation.Check] = c
		}
		c.Schedules++
	}
	for _, c := range counts {
		sum.ByCheck = append(sum.ByCheck, *c)
	}
	sort.Slice(sum.ByCheck, func(i, j int) bool {
		a, b := sum.ByCheck[i], sum.ByCheck[j]
		if a.Schedules != b.Schedules {
			return a.Schedules > b.Schedules
		}
		return a.Check < b.Check
	})
	return sum
}
