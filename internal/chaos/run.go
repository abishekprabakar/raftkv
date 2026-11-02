package chaos

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// FaultRecord is what the harness actually did for one Fault, with times in
// milliseconds since the run started, on the host's clock.
type FaultRecord struct {
	Kind    FaultKind `json:"kind"`
	Targets []int     `json:"targets"`
	Leader  int       `json:"leader_before"` // 0 if no node claimed to lead
	StartMs int64     `json:"start_ms"`
	EndMs   int64     `json:"end_ms"`
	Skipped string    `json:"skipped,omitempty"`
}

// Counts are what the clients did in one run.
type Counts struct {
	WritesAcked   int `json:"writes_acknowledged"`
	WritesUnknown int `json:"writes_unknown"`
	ReadsServed   int `json:"reads_served"`
	ReadsFailed   int `json:"reads_refused_or_timed_out"`
}

func (c *Counts) add(o Counts) {
	c.WritesAcked += o.WritesAcked
	c.WritesUnknown += o.WritesUnknown
	c.ReadsServed += o.ReadsServed
	c.ReadsFailed += o.ReadsFailed
}

// RunResult is one schedule's outcome.
type RunResult struct {
	Seed   uint64        `json:"seed"`
	Faults []FaultRecord `json:"faults"`
	Counts Counts        `json:"counts"`
	// Linearizable is Porcupine's answer: Ok, Illegal, or Unknown if it timed out.
	Linearizable    porcupine.CheckResult `json:"linearizable"`
	CheckSeconds    float64               `json:"check_seconds"`
	Recovered       bool                  `json:"recovered"` // the nodes agreed on a leader after the final heal
	RecoverySeconds float64               `json:"recovery_seconds"`
	FinalReadsOK    bool                  `json:"final_reads_ok"`
	Failovers       []Failover            `json:"failovers"`
	Error           string                `json:"error,omitempty"`
}

// Runner runs schedules on the Compose project in Dir, with the image Image.
// Everything a run leaves behind (logs, Docker events, the full history, and a
// drawing of any violation) goes under Artifacts/seed-N.
type Runner struct {
	Dir       string
	Image     string
	Artifacts string
	Log       io.Writer
}

// Run runs one schedule from start to finish, and always tears the cluster down.
func (r *Runner) Run(seed uint64) RunResult {
	res := RunResult{Seed: seed}
	dir := filepath.Join(r.Artifacts, fmt.Sprintf("seed-%d", seed))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		res.Error = err.Error()
		return res
	}
	c := &cluster{dir: r.Dir, image: r.Image, log: r.Log}
	defer c.down()
	if err := c.up(); err != nil {
		res.Error = err.Error()
		return res
	}
	stopEvents, err := c.watchEvents()
	if err != nil {
		res.Error = err.Error()
		return res
	}
	history, err := r.drive(c, seed, &res)
	if err != nil {
		res.Error = err.Error()
	}

	// The logs, and Docker's record of every disconnect and reconnect.
	events := stopEvents()
	logs, _ := c.logs()
	writeGzip(filepath.Join(dir, "node-logs.txt.gz"), func(w io.Writer) error { _, err := w.Write(logs); return err })
	os.WriteFile(filepath.Join(dir, "network-events.jsonl"), events, 0o644)
	res.Failovers = failovers(res.Faults, parseEvents(events, c.ids), parseStates(logs))
	err = writeGzip(filepath.Join(dir, "history.json.gz"), func(w io.Writer) error { return json.NewEncoder(w).Encode(history) })
	if err != nil && res.Error == "" {
		res.Error = err.Error()
	}
	if res.Error != "" {
		return res
	}

	t0 := time.Now()
	result, info := porcupine.CheckOperationsVerbose(Model, Operations(history), porcupineTimeout)
	res.Linearizable, res.CheckSeconds = result, time.Since(t0).Seconds()
	if result == porcupine.Illegal {
		// An interactive drawing of the operations that can't be ordered. Open it
		// in a browser after gunzip.
		writeGzip(filepath.Join(dir, "violation.html.gz"), func(w io.Writer) error { return porcupine.Visualize(Model, info, w) })
	}
	return res
}

// drive starts the clients on a running cluster, applies the faults, and collects
// the history.
func (r *Runner) drive(c *cluster, seed uint64, res *RunResult) ([]Op, error) {
	if !waitFor(settleTimeout, c.agreed) {
		return nil, fmt.Errorf("no leader agreed by all three nodes within %v of starting", settleTimeout)
	}
	var workers []*worker
	defer func() {
		for _, w := range workers {
			w.close()
		}
	}()
	for node := 1; node <= Nodes; node++ {
		for range ClientsPerNode {
			w, err := c.startWorker(node)
			if err != nil {
				return nil, err
			}
			workers = append(workers, w)
		}
	}

	start := time.Now()
	since := func() int64 { return time.Since(start).Nanoseconds() }
	histories := make([][]Op, len(workers))
	var wg sync.WaitGroup
	var failure error
	var mu sync.Mutex
	fail := func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if failure == nil {
			failure = err
		}
	}

	// The clients. Each runs one operation at a time until the workload ends.
	for i, w := range workers {
		wg.Go(func() {
			load := NewWorkload(seed, i)
			for {
				in, pause := load.Next()
				time.Sleep(pause)
				if time.Since(start) >= WorkloadTime {
					return
				}
				op := Op{Client: i, Kind: in.Kind, Key: in.Key, Value: in.Value, Call: since()}
				out, err := w.do(in)
				op.Return = since()
				if err != nil {
					fail(fmt.Errorf("client %d: %w", i, err))
					return
				}
				op.OK, op.Err = out.OK, out.Err
				if in.Kind == Get {
					op.Value = out.Value
				}
				histories[i] = append(histories[i], op)
			}
		})
	}

	// The faults, then a heal at the end of the workload.
	res.Faults = r.applyFaults(c, Faults(seed), start)
	time.Sleep(time.Until(start.Add(WorkloadTime)))
	wg.Wait()
	if failure != nil {
		return nil, failure
	}

	t0 := time.Now()
	res.Recovered = waitFor(settleTimeout, c.agreed)
	res.RecoverySeconds = time.Since(t0).Seconds()

	// Every client reads every key once, with the network whole again.
	res.FinalReadsOK = true
	for i, w := range workers {
		wg.Go(func() {
			for k := range Keys {
				in := Input{Kind: Get, Key: fmt.Sprintf("k%d", k)}
				op := Op{Client: i, Kind: Get, Key: in.Key, Call: since()}
				out, err := w.do(in)
				op.Return = since()
				if err != nil {
					fail(fmt.Errorf("client %d: %w", i, err))
					return
				}
				op.OK, op.Value, op.Err = out.OK, out.Value, out.Err
				histories[i] = append(histories[i], op)
				if !out.OK {
					mu.Lock()
					res.FinalReadsOK = false
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	if failure != nil {
		return nil, failure
	}

	var history []Op
	for _, h := range histories {
		history = append(history, h...)
	}
	slices.SortFunc(history, func(a, b Op) int { return int(a.Call - b.Call) })
	for _, op := range history {
		switch {
		case op.Kind == Get && op.OK:
			res.Counts.ReadsServed++
		case op.Kind == Get:
			res.Counts.ReadsFailed++
		case op.OK:
			res.Counts.WritesAcked++
		default:
			res.Counts.WritesUnknown++
		}
	}
	return history, nil
}

// applyFaults cuts nodes off and reconnects them on schedule, and returns what it
// did. It returns with every node connected.
func (r *Runner) applyFaults(c *cluster, faults []Fault, start time.Time) []FaultRecord {
	ms := func() int64 { return time.Since(start).Milliseconds() }
	var records []FaultRecord
	for _, f := range faults {
		time.Sleep(time.Until(start.Add(f.At)))
		rec := FaultRecord{Kind: f.Kind}
		var leader int
		waitFor(3*time.Second, func() bool { leader, _ = c.leader(); return leader != 0 })
		rec.Leader = leader
		switch f.Kind {
		case IsolateLeader:
			if leader == 0 {
				rec.Skipped = "no node claimed to lead"
			} else {
				rec.Targets = []int{leader}
			}
		case IsolateFollower:
			var followers []int
			for n := 1; n <= Nodes; n++ {
				if n != leader {
					followers = append(followers, n)
				}
			}
			rec.Targets = []int{followers[f.Pick%len(followers)]}
		case IsolateTwo:
			keep := f.Pick%Nodes + 1
			for n := 1; n <= Nodes; n++ {
				if n != keep {
					rec.Targets = append(rec.Targets, n)
				}
			}
		}
		rec.StartMs = ms()
		for _, n := range rec.Targets {
			if err := c.disconnect(n); err != nil {
				fmt.Fprintln(r.Log, "disconnect:", err)
			}
		}
		time.Sleep(time.Until(start.Add(f.At + f.Hold)))
		for _, n := range rec.Targets {
			if err := c.connect(n); err != nil {
				fmt.Fprintln(r.Log, "connect:", err)
			}
		}
		rec.EndMs = ms()
		records = append(records, rec)
	}
	return records
}

// waitFor polls cond every 250 ms until it holds or time runs out.
func waitFor(limit time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(limit); ; time.Sleep(250 * time.Millisecond) {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// writeGzip writes a compressed file. A run's logs and history are kept for every
// schedule, so anyone can check the failover times or rerun Porcupine.
func writeGzip(path string, write func(io.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	if err := write(zw); err != nil {
		return err
	}
	return zw.Close()
}
