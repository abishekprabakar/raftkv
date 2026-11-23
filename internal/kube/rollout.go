package kube

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// stsView is what a rolling update changes on a StatefulSet, as one watch event
// shows it.
type stsView struct {
	at                              time.Duration
	image                           string
	generation, observed            int64
	replicas, ready, updated        int
	currentRevision, updateRevision string
}

// rolledOut is `kubectl rollout status`'s test: the controller has seen the
// newest spec, and every replica is on the new revision and ready.
func (v stsView) rolledOut() bool {
	return v.observed >= v.generation && v.updated == v.replicas && v.ready == v.replicas &&
		v.currentRevision == v.updateRevision
}

// stsWatch follows the StatefulSet with `kubectl get --watch`, stamping each
// change with the run's clock as it arrives.
type stsWatch struct {
	cmd *exec.Cmd
	mu  sync.Mutex
	all []stsView
}

func (r *Run) watchStatefulSet() (*stsWatch, error) {
	w := &stsWatch{cmd: exec.Command("kubectl", r.T.kubectlArgs([]string{"get", "statefulset", r.T.StatefulSet, "--watch", "-o", "json"})...)}
	out, err := w.cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := w.cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		// kubectl prints one JSON object per change, one after another.
		dec := json.NewDecoder(bufio.NewReader(out))
		for {
			var o struct {
				Metadata struct {
					Generation int64 `json:"generation"`
				} `json:"metadata"`
				Spec struct {
					Replicas int `json:"replicas"`
					Template struct {
						Spec struct {
							Containers []struct {
								Image string `json:"image"`
							} `json:"containers"`
						} `json:"spec"`
					} `json:"template"`
				} `json:"spec"`
				Status struct {
					ObservedGeneration int64  `json:"observedGeneration"`
					ReadyReplicas      int    `json:"readyReplicas"`
					UpdatedReplicas    int    `json:"updatedReplicas"`
					CurrentRevision    string `json:"currentRevision"`
					UpdateRevision     string `json:"updateRevision"`
				} `json:"status"`
			}
			if dec.Decode(&o) != nil {
				return
			}
			v := stsView{
				at: r.Since(), generation: o.Metadata.Generation, observed: o.Status.ObservedGeneration,
				replicas: o.Spec.Replicas, ready: o.Status.ReadyReplicas, updated: o.Status.UpdatedReplicas,
				currentRevision: o.Status.CurrentRevision, updateRevision: o.Status.UpdateRevision,
			}
			if c := o.Spec.Template.Spec.Containers; len(c) > 0 {
				v.image = c[0].Image
			}
			w.mu.Lock()
			w.all = append(w.all, v)
			w.mu.Unlock()
		}
	}()
	return w, nil
}

func (w *stsWatch) stop() {
	w.cmd.Process.Kill()
	w.cmd.Wait()
}

// span finds the rollout to image: from the first event that shows the new image
// in the pod template, to the first event after it that shows the rollout
// complete.
func (w *stsWatch) span(image string) (start, end time.Duration, started, finished bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, v := range w.all {
		switch {
		case !started && v.image == image:
			start, started = v.at, true
			// An event can show the new spec with the old status still in it,
			// which looks complete. Only an event the controller has caught up
			// with, and that isn't the very first, can end the rollout.
		case started && v.rolledOut():
			return start, v.at, true, true
		}
	}
	return start, 0, started, false
}

// RolloutRound is one rolling update under load.
type RolloutRound struct {
	N    int    `json:"round"`
	From string `json:"from_image"`
	To   string `json:"to_image"`
	// ApplySeconds is how long `terraform apply` took, which includes building
	// and loading the image before the StatefulSet changes.
	ApplySeconds float64 `json:"terraform_apply_seconds"`
	// RolloutSeconds runs from the first moment the StatefulSet shows the new
	// image to the first moment it reports every replica updated and ready.
	RolloutSeconds float64 `json:"rollout_seconds"`
	RolloutSeen    bool    `json:"rollout_seen"`
	Healthy        bool    `json:"healthy_after"`
	// VersionOK means every pod's spec names the new image and the binary in
	// every pod reports the new version.
	VersionOK bool              `json:"every_pod_runs_the_new_version"`
	Versions  map[string]string `json:"version_by_pod"`

	LeaderChanges int `json:"leader_changes"`
	// TermsPassed is how far the term moved during the rollout. Every election
	// that was started raises it, whether or not anyone won, so this is at least
	// LeaderChanges, and the difference is elections that failed.
	TermsPassed       uint64    `json:"terms_passed"`
	Load              LoadStats `json:"load"`
	LongestWriteGapMs float64   `json:"longest_write_gap_ms"`
	Error             string    `json:"error,omitempty"`
}

// OK reports whether the round did what U1 asks.
func (rr RolloutRound) OK() bool {
	return rr.Error == "" && rr.RolloutSeen && rr.Healthy && rr.VersionOK
}

// RolloutRound changes the image tag with terraform and watches the StatefulSet
// replace its pods while the clients keep going.
func (r *Run) RolloutRound(n int, from, to string) RolloutRound {
	ctx := context.Background()
	rr := RolloutRound{N: n, From: "raftkv:" + from, To: "raftkv:" + to, Versions: map[string]string{}}
	if !r.WaitHealthy(settleLimit) {
		rr.Error = "the cluster was not healthy before the round"
		return rr
	}
	time.Sleep(loadBeforeFault)
	_, termBefore, _ := r.Prober.Agreed()
	watch, err := r.watchStatefulSet()
	if err != nil {
		rr.Error = err.Error()
		return rr
	}
	defer watch.stop()

	t0 := r.Since()
	_, err = r.T.Terraform(ctx, "apply", "-auto-approve", "-input=false", "-no-color", "-var", "image_tag="+to)
	rr.ApplySeconds = (r.Since() - t0).Seconds()
	if err != nil {
		rr.Error = err.Error()
	}

	// terraform waits for the rollout itself, so the events are normally all
	// here already.
	var start, end time.Duration
	waitUntil(60*time.Second, 50*time.Millisecond, func() bool {
		var started bool
		start, end, started, rr.RolloutSeen = watch.span(rr.To)
		return rr.RolloutSeen || (!started && rr.Error != "")
	})
	if rr.RolloutSeen {
		rr.RolloutSeconds = (end - start).Seconds()
	} else {
		start, end = t0, r.Since()
	}
	rr.Healthy = r.WaitHealthy(settleLimit)
	settled := r.Since()
	if _, termAfter, ok := r.Prober.Agreed(); ok && termBefore != 0 {
		rr.TermsPassed = termAfter - termBefore
	}

	rr.VersionOK = true
	pods, err := r.T.Pods(ctx)
	if err != nil || len(pods) != len(r.T.Nodes) {
		rr.VersionOK = false
	}
	for _, p := range pods {
		out, err := r.T.Kubectl(ctx, "exec", p.Name, "--", "/usr/local/bin/raftkvd", "--version")
		rr.Versions[p.Name] = strings.TrimSpace(string(out))
		if err != nil || rr.Versions[p.Name] != to || p.Image != rr.To {
			rr.VersionOK = false
		}
	}

	rr.LeaderChanges = len(r.Prober.LeadersBetween(start, settled))
	rr.Load = r.Load.Window(start, settled)
	rr.LongestWriteGapMs = ms(r.Load.LongestWriteGap(start, settled))
	return rr
}

// RolloutSummary totals a set of rounds.
type RolloutSummary struct {
	Rounds int `json:"rounds"`
	Passed int `json:"passed"`

	RolloutSeconds Dist `json:"rollout_seconds"`
	ApplySeconds   Dist `json:"terraform_apply_seconds"`
	// The clients, over every round's rollout: from the StatefulSet's change to
	// the cluster healthy again.
	Requests            int     `json:"requests"`
	Failed              int     `json:"failed_requests"`
	FailedPercent       float64 `json:"failed_requests_percent"`
	Retries             int     `json:"retries"`
	RequestsWithRetries int     `json:"requests_that_retried"`
	MaxLatencyMs        float64 `json:"longest_request_ms"`
	LongestWriteGapMs   Dist    `json:"longest_write_gap_ms"`
	LeaderChanges       Dist    `json:"leader_changes_per_rollout"`
	TermsPassed         Dist    `json:"terms_passed_per_rollout"`
}

// SummarizeRollouts computes every number reported about the rounds.
func SummarizeRollouts(rounds []RolloutRound) RolloutSummary {
	s := RolloutSummary{Rounds: len(rounds)}
	var rollout, apply, gap, leaders, terms []float64
	for _, rr := range rounds {
		if rr.OK() {
			s.Passed++
		}
		if rr.RolloutSeen {
			rollout = append(rollout, rr.RolloutSeconds)
		}
		apply = append(apply, rr.ApplySeconds)
		gap = append(gap, rr.LongestWriteGapMs)
		leaders = append(leaders, float64(rr.LeaderChanges))
		terms = append(terms, float64(rr.TermsPassed))
		s.Requests += rr.Load.Requests
		s.Failed += rr.Load.Failed
		s.Retries += rr.Load.Retries
		s.RequestsWithRetries += rr.Load.RequestsWithRetries
		s.MaxLatencyMs = max(s.MaxLatencyMs, rr.Load.LatencyMs.Max)
	}
	if s.Requests > 0 {
		s.FailedPercent = 100 * float64(s.Failed) / float64(s.Requests)
	}
	s.RolloutSeconds, s.ApplySeconds = Summarize(rollout), Summarize(apply)
	s.LongestWriteGapMs, s.LeaderChanges, s.TermsPassed = Summarize(gap), Summarize(leaders), Summarize(terms)
	return s
}

// OtherTag picks the tag to roll to: v2 from v1, and v1 from anything else.
func OtherTag(image string) (from, to string) {
	_, from, _ = strings.Cut(image, ":")
	if from == "v1" {
		return from, "v2"
	}
	return from, "v1"
}

// errorf keeps error text on one line, for the JSON reports.
func errorf(format string, args ...any) string {
	return strings.Join(strings.Fields(fmt.Sprintf(format, args...)), " ")
}
