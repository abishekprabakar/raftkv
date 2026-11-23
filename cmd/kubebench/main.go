// Command kubebench measures the raftkv cluster that `terraform apply` in
// deploy/terraform creates. See docs/acceptance/k8s.md for what each number means.
//
//	kubebench check                                  # A1, A2, A4, K1, K2
//	kubebench failover  -trials 100 -out results/k8s-failover.json
//	kubebench restart   -rounds 5   -out results/k8s-restart.json
//	kubebench rollout   -rounds 10  -out results/k8s-rollout.json
//	kubebench coldstart -rounds 10  -out results/k8s-coldstart.json
//
// Every subcommand needs the cluster up first, and coldstart destroys and
// recreates it each round. Each exits with status 1 if a pass criterion failed,
// after writing its report: a failure is recorded, never hidden.
package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/abishekprabakar/raftkv/internal/hwinfo"
	"github.com/abishekprabakar/raftkv/internal/kube"
)

// header is the part every report starts with: what ran, and on what.
type header struct {
	GeneratedAt string        `json:"generated_at"`
	Command     string        `json:"command"`
	Hardware    hwinfo.Info   `json:"hardware"`
	Versions    kube.Versions `json:"versions"`
	Image       string        `json:"image"`
	WallSeconds float64       `json:"wall_seconds"`
}

// loadReport is what a run with clients adds.
type loadReport struct {
	Seed    uint64 `json:"seed"`
	Clients int    `json:"clients"`
	// ProbeIntervalMs is the gap between one Status answer and the next from the
	// same node: how finely "time to a new leader" can be read.
	ProbeIntervalMs kube.Dist      `json:"status_poll_interval_ms"`
	Load            kube.LoadStats `json:"load"`
	kube.Verdict
}

type options struct {
	dir, out, artifacts, logPath string
	seed                         uint64
	count                        int
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	mode := os.Args[1]
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	var o options
	fs.StringVar(&o.dir, "tf", "deploy/terraform", "the Terraform directory")
	fs.StringVar(&o.out, "out", "", "write the JSON report here")
	fs.StringVar(&o.artifacts, "artifacts", "", "directory for the history and the pods' logs (default: none kept)")
	fs.StringVar(&o.logPath, "log", "", "write every kubectl and terraform command and its output here (default: discard)")
	fs.Uint64Var(&o.seed, "seed", 1, "seed for the clients' operations")
	trials := fs.Int("trials", 100, "failover: number of trials")
	rounds := fs.Int("rounds", 10, "restart, rollout, coldstart: number of rounds")
	fs.Parse(os.Args[2:])
	o.count = *rounds
	if mode == "failover" {
		o.count = *trials
	}

	logw := io.Discard
	if o.logPath != "" {
		if err := os.MkdirAll(filepath.Dir(o.logPath), 0o755); err != nil {
			fatal(err)
		}
		f, err := os.Create(o.logPath)
		if err != nil {
			fatal(err)
		}
		defer f.Close()
		logw = f
	}
	target, err := kube.LoadTarget(o.dir, logw)
	if err != nil {
		fatal(err)
	}
	ok := false
	switch mode {
	case "check":
		ok = check(target, o)
	case "failover", "restart":
		ok = faults(target, o, mode)
	case "rollout":
		ok = rollout(target, o)
	case "coldstart":
		ok = coldstart(target, o)
	default:
		usage()
	}
	if !ok {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kubebench check|failover|restart|rollout|coldstart [flags]")
	os.Exit(2)
}

func newHeader(t *kube.Target, mode string) header {
	h := header{
		Command: "kubebench " + mode, Hardware: hwinfo.Collect("."), Image: t.Image,
		Versions: t.CollectVersions(context.Background()),
	}
	// hwinfo's own note is about fsync on the host's filesystem, which nothing
	// measured here touches.
	h.Hardware.Note = "The pods and their volumes are inside Docker's Linux VM. The filesystem named here is the host's, where only the harness runs."
	return h
}

func (h *header) finish(start time.Time) {
	h.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	h.WallSeconds = time.Since(start).Seconds()
}

func check(t *kube.Target, o options) bool {
	start := time.Now()
	rep := struct {
		header
		Passed int          `json:"passed"`
		Failed int          `json:"failed"`
		Checks []kube.Check `json:"checks"`
	}{header: newHeader(t, "check")}
	rep.Checks = kube.Checks(t, func(c kube.Check) {
		verdict := "ok  "
		if !c.Pass {
			verdict = "FAIL"
		}
		fmt.Printf("%s %s: %s\n     %s\n", verdict, c.ID, c.What, c.Detail)
	})
	for _, c := range rep.Checks {
		if c.Pass {
			rep.Passed++
		} else {
			rep.Failed++
		}
	}
	rep.finish(start)
	write(o.out, rep)
	fmt.Printf("%d checks passed, %d failed\n", rep.Passed, rep.Failed)
	return rep.Failed == 0
}

// finishLoad stops the clients, reads every key back, and checks the history.
func finishLoad(r *kube.Run, t *kube.Target, o options) (loadReport, bool) {
	lr := loadReport{Seed: o.seed, Clients: kube.Clients, ProbeIntervalMs: r.Prober.Resolution()}
	// The final reads need a cluster that can answer them.
	if !r.WaitHealthy(3 * time.Minute) {
		fmt.Println("the cluster was not healthy for the final reads")
	}
	history, err := r.Load.Finish()
	if err != nil {
		fatal(err)
	}
	lr.Load = kube.Stats(history)
	if lr.Verdict, err = kube.Verify(history, o.artifacts); err != nil {
		fatal(err)
	}
	saveLogs(t, o)
	d := lr.Durability
	fmt.Printf("clients: %d requests, %d failed, %d retries; writes %d acknowledged, %d unknown\n",
		lr.Load.Requests, lr.Load.Failed, lr.Load.Retries, d.WritesAcked, d.WritesUnknown)
	fmt.Printf("durability over %d keys: %d acknowledged writes lost, %d duplicated, %d values never written, %d keys not read\n",
		d.Keys, d.AckedLost, d.Duplicated, d.NeverWritten, d.KeysNotRead)
	fmt.Printf("Porcupine: %s in %.1f s\n", lr.Linearizable, lr.CheckSeconds)
	if lr.Linearizable == porcupine.Illegal && o.artifacts != "" {
		fmt.Println("a drawing of the violation is in", filepath.Join(o.artifacts, "violation.html.gz"))
	}
	return lr, lr.Verdict.OK()
}

// saveLogs keeps each pod's log and the cluster's events beside the history.
func saveLogs(t *kube.Target, o options) {
	if o.artifacts == "" {
		return
	}
	ctx := context.Background()
	f, err := os.Create(filepath.Join(o.artifacts, "pods.log.gz"))
	if err != nil {
		return
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	defer zw.Close()
	quiet := *t
	quiet.Log = nil
	for _, n := range t.Nodes {
		out, _ := quiet.Kubectl(ctx, "logs", n.Pod, "--tail=-1", "--timestamps=false")
		fmt.Fprintf(zw, "==== %s\n%s\n", n.Pod, out)
	}
	out, _ := quiet.Kubectl(ctx, "get", "events", "--sort-by=.lastTimestamp")
	fmt.Fprintf(zw, "==== events\n%s\n", out)
}

func faults(t *kube.Target, o options, mode string) bool {
	start := time.Now()
	rep := struct {
		header
		Requested int               `json:"trials_requested"`
		Summary   kube.TrialSummary `json:"summary"`
		loadReport
		Trials []kube.Trial `json:"trials"`
	}{header: newHeader(t, mode), Requested: o.count}
	r, err := kube.NewRun(t, mode[:1], o.seed, true, os.Stdout)
	if err != nil {
		fatal(err)
	}
	defer r.Prober.Stop()
	for n := 1; n <= o.count; n++ {
		var tr kube.Trial
		if mode == "restart" {
			tr = r.RestartTrial(n)
		} else {
			tr = r.FailoverTrial(n)
		}
		rep.Trials = append(rep.Trials, tr)
		fmt.Printf("trial %d: deleted %v in term %d; new leader %s in term %d after %.0f ms; first write after %.0f ms; pods ready after %.0f ms; settled after %.0f ms; %d extra leader changes; from volume %v%s\n",
			n, tr.Deleted, tr.OldTerm, tr.NewLeader, tr.NewTerm, tr.NewLeaderMs, tr.FirstWriteMs, tr.PodsReadyMs, tr.SettledMs,
			tr.ExtraLeaderChanges, tr.FromDisk, errText(tr.Error))
		rep.Summary = kube.SummarizeTrials(rep.Trials)
		rep.finish(start)
		write(o.out, rep) // after every trial, so a run that dies keeps what it measured
		if tr.Error != "" && !tr.Settled {
			fmt.Println("stopping: the cluster did not come back")
			break
		}
	}
	var ok bool
	rep.loadReport, ok = finishLoad(r, t, o)
	rep.finish(start)
	write(o.out, rep)
	s := rep.Summary
	fmt.Printf("%d trials, %d passed: %d without a new leader, %d without a first write, %d not settled, %d not from the volume\n",
		s.Trials, s.Passed, s.NoNewLeader, s.NoFirstWrite, s.NotSettled, s.NotFromDisk)
	fmt.Printf("time to a new leader: median %.0f ms, p95 %.0f ms, min %.0f, max %.0f\n", s.NewLeaderMs.Median, s.NewLeaderMs.P95, s.NewLeaderMs.Min, s.NewLeaderMs.Max)
	fmt.Printf("time to the first successful write: median %.0f ms, p95 %.0f ms, min %.0f, max %.0f\n", s.FirstWriteMs.Median, s.FirstWriteMs.P95, s.FirstWriteMs.Min, s.FirstWriteMs.Max)
	return ok && s.Passed == o.count
}

func rollout(t *kube.Target, o options) bool {
	start := time.Now()
	rep := struct {
		header
		Requested int                 `json:"rounds_requested"`
		Summary   kube.RolloutSummary `json:"summary"`
		loadReport
		Rounds []kube.RolloutRound `json:"rounds"`
	}{header: newHeader(t, "rollout"), Requested: o.count}
	r, err := kube.NewRun(t, "u", o.seed, true, os.Stdout)
	if err != nil {
		fatal(err)
	}
	defer r.Prober.Stop()
	image := t.Image
	for n := 1; n <= o.count; n++ {
		from, to := kube.OtherTag(image)
		rr := r.RolloutRound(n, from, to)
		rep.Rounds = append(rep.Rounds, rr)
		fmt.Printf("round %d: %s -> %s; rollout %.1f s (apply %.1f s); %d requests, %d failed, %d retries, longest %.0f ms; longest write gap %.0f ms; %d leader changes; new version everywhere %v%s\n",
			n, rr.From, rr.To, rr.RolloutSeconds, rr.ApplySeconds, rr.Load.Requests, rr.Load.Failed, rr.Load.Retries,
			rr.Load.LatencyMs.Max, rr.LongestWriteGapMs, rr.LeaderChanges, rr.VersionOK, errText(rr.Error))
		rep.Summary = kube.SummarizeRollouts(rep.Rounds)
		rep.finish(start)
		write(o.out, rep)
		if !rr.Healthy {
			fmt.Println("stopping: the cluster did not come back")
			break
		}
		image = rr.To
	}
	var ok bool
	rep.loadReport, ok = finishLoad(r, t, o)
	rep.finish(start)
	write(o.out, rep)
	s := rep.Summary
	fmt.Printf("%d rounds, %d passed; rollout time median %.1f s (min %.1f, max %.1f); %d of %d requests failed (%.3f%%)\n",
		s.Rounds, s.Passed, s.RolloutSeconds.Median, s.RolloutSeconds.Min, s.RolloutSeconds.Max, s.Failed, s.Requests, s.FailedPercent)
	return ok && s.Passed == o.count
}

func coldstart(t *kube.Target, o options) bool {
	start := time.Now()
	rep := struct {
		header
		Requested int              `json:"rounds_requested"`
		Summary   kube.ColdSummary `json:"summary"`
		Rounds    []kube.ColdRound `json:"rounds"`
	}{header: newHeader(t, "coldstart"), Requested: o.count}
	for n := 1; n <= o.count; n++ {
		c := kube.ColdStartRound(t, n, os.Stdout)
		rep.Rounds = append(rep.Rounds, c)
		fmt.Printf("round %d: destroy %.1f s, nothing left %v; API server after %.1f s; pods created after %.1f s; 3 Ready with a leader after %.1f s; apply returned after %.1f s%s\n",
			n, c.DestroySeconds, c.NothingLeft, c.APIServerSeconds, c.PodsCreatedSeconds, c.ReadySeconds, c.ApplySeconds, errText(c.Error))
		rep.Summary = kube.SummarizeColdStarts(rep.Rounds)
		rep.finish(start)
		write(o.out, rep)
		if !c.ApplyOK {
			fmt.Println("stopping: terraform apply failed")
			break
		}
	}
	s := rep.Summary
	fmt.Printf("%d rounds, %d passed; cold start median %.1f s (min %.1f, max %.1f)\n",
		s.Rounds, s.Passed, s.ReadySeconds.Median, s.ReadySeconds.Min, s.ReadySeconds.Max)
	return s.Passed == o.count
}

func errText(e string) string {
	if e == "" {
		return ""
	}
	return "; ERROR: " + e
}

func write(path string, rep any) {
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		fatal(err)
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "kubebench:", err)
	os.Exit(1)
}
