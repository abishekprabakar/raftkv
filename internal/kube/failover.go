package kube

import (
	"context"
	"fmt"
	"io"
	"time"
)

// Limits and pauses, fixed in docs/acceptance/k8s.md.
const (
	loadBeforeFault = 3 * time.Second   // steady load before each fault
	newLeaderLimit  = 60 * time.Second  // a new leader must be seen within this
	settleLimit     = 180 * time.Second // and the cluster whole again within this
	pollPods        = 200 * time.Millisecond
)

// Run is one measurement in progress: a cluster, the clock everything is timed
// by, the prober and the clients.
type Run struct {
	T      *Target
	Prober *Prober
	Load   *Load
	Out    io.Writer // progress, one line per trial

	start time.Time
}

// NewRun starts the clock and the prober, and, if seed is not zero, the clients.
func NewRun(t *Target, prefix string, seed uint64, withLoad bool, out io.Writer) (*Run, error) {
	r := &Run{T: t, Out: out, start: time.Now()}
	r.Prober = StartProber(t, r.Since)
	if withLoad {
		var err error
		if r.Load, err = StartLoad(t, fmt.Sprintf("%s%d", prefix, time.Now().Unix()), seed, r.Since); err != nil {
			r.Prober.Stop()
			return nil, err
		}
	}
	return r, nil
}

// Since is the run's clock: time since the run started. Go reads it from the
// monotonic clock, so it can't jump.
func (r *Run) Since() time.Duration { return time.Since(r.start) }

// Healthy reports whether every pod is Ready and every node names the same
// leader in the same term.
func (r *Run) Healthy() bool {
	if _, _, ok := r.Prober.Agreed(); !ok {
		return false
	}
	pods, err := r.T.Pods(context.Background())
	return err == nil && r.T.AllReady(pods)
}

// WaitHealthy waits for Healthy.
func (r *Run) WaitHealthy(limit time.Duration) bool {
	return waitUntil(limit, pollPods, r.Healthy)
}

// Trial is one fault and the recovery from it. Times are in milliseconds from
// t0, the moment before the kubectl command was started.
type Trial struct {
	N       int      `json:"trial"`
	Deleted []string `json:"deleted_pods"`
	OldTerm uint64   `json:"old_term"`
	// T0Ms places the trial in the run, in milliseconds since the run started.
	T0Ms float64 `json:"t0_since_run_start_ms"`
	// KubectlMs is how long `kubectl delete --wait=false` took to return.
	KubectlMs float64 `json:"kubectl_delete_returned_ms"`
	// OldLeaderLastAnswerMs is the old leader's last Status answer. It keeps
	// serving until the kubelet's signal reaches it.
	OldLeaderLastAnswerMs float64 `json:"old_leader_last_answer_ms"`
	OldLeaderSeenGone     bool    `json:"old_leader_seen_gone"`

	NewLeaderSeen bool    `json:"new_leader_seen"`
	NewLeader     string  `json:"new_leader_pod,omitempty"`
	NewTerm       uint64  `json:"new_term,omitempty"`
	NewLeaderMs   float64 `json:"new_leader_ms"`

	FirstWriteSeen bool    `json:"first_write_seen"`
	FirstWriteMs   float64 `json:"first_write_ms"`
	FirstWriteTerm uint64  `json:"first_write_term,omitempty"`

	Settled     bool    `json:"settled"`
	PodsReadyMs float64 `json:"all_pods_ready_ms"`
	SettledMs   float64 `json:"settled_ms"`
	// ExtraLeaderChanges counts leaders first seen after the new one and before
	// the cluster settled: elections forced while the deleted pod rejoined.
	ExtraLeaderChanges int `json:"extra_leader_changes"`
	// LongestWriteGapMs is the longest stretch with no write acknowledged, from
	// any client, that ended between t0 and the cluster settling.
	LongestWriteGapMs float64 `json:"longest_write_gap_ms"`
	// Started is what each replacement pod found on its disk.
	Started  map[string]Started `json:"replacement_started_with"`
	FromDisk bool               `json:"every_replacement_started_from_its_volume"`
	Error    string             `json:"error,omitempty"`
}

// OK reports whether the trial did everything F1 and F2, or R1, ask of it.
func (tr Trial) OK() bool {
	return tr.Error == "" && tr.NewLeaderSeen && tr.FirstWriteSeen && tr.Settled && tr.FromDisk
}

// FailoverTrial deletes the leader's pod and watches the cluster recover.
func (r *Run) FailoverTrial(n int) Trial {
	return r.trial(n, false)
}

// RestartTrial deletes every pod at once.
func (r *Run) RestartTrial(n int) Trial {
	return r.trial(n, true)
}

func (r *Run) trial(n int, everyPod bool) Trial {
	ctx := context.Background()
	tr := Trial{N: n, Started: map[string]Started{}}

	// 1. A healthy cluster under steady load, with a known leader and term.
	var leader Node
	for attempt := 0; ; attempt++ {
		if !r.WaitHealthy(settleLimit) {
			tr.Error = "the cluster was not healthy before the trial"
			return tr
		}
		time.Sleep(loadBeforeFault)
		var ok bool
		if leader, tr.OldTerm, ok = r.Prober.Agreed(); ok {
			break
		}
		if attempt == 5 {
			tr.Error = "the nodes kept changing leader before the trial"
			return tr
		}
	}
	before, err := r.T.Pods(ctx)
	if err != nil {
		tr.Error = err.Error()
		return tr
	}
	uids := map[string]string{}
	for _, p := range before {
		uids[p.Name] = p.UID
	}

	// 2. The fault. t0 is read before kubectl starts, so both times include
	// everything Kubernetes does between the command and the process's signal.
	args := []string{"delete", "pod", leader.Pod, "--wait=false"}
	tr.Deleted = []string{leader.Pod}
	if everyPod {
		args = []string{"delete", "pod", "-l", Selector, "--wait=false"}
		tr.Deleted = nil
		for _, node := range r.T.Nodes {
			tr.Deleted = append(tr.Deleted, node.Pod)
		}
	}
	t0 := r.Since()
	tr.T0Ms = ms(t0)
	if _, err := r.T.Kubectl(ctx, args...); err != nil {
		tr.Error = err.Error()
		return tr
	}
	tr.KubectlMs = ms(r.Since() - t0)

	// 3. A new leader: any node leading in a term above the old one.
	var elected LeaderEvent
	tr.NewLeaderSeen = waitUntil(newLeaderLimit, time.Millisecond, func() bool {
		var ok bool
		elected, ok = r.Prober.LeaderAbove(tr.OldTerm, t0)
		return ok
	})
	if tr.NewLeaderSeen {
		tr.NewLeader, tr.NewTerm, tr.NewLeaderMs = elected.Pod, elected.Term, ms(elected.At-t0)
	}

	// 4. Whole again: every deleted pod replaced by a new one, all ready, and
	// every node naming the same leader.
	replaced := func(pods []Pod) bool {
		now := map[string]string{}
		for _, p := range pods {
			now[p.Name] = p.UID
		}
		for _, name := range tr.Deleted {
			if now[name] == "" || now[name] == uids[name] {
				return false
			}
		}
		return true
	}
	podsReady := false
	tr.Settled = waitUntil(settleLimit, pollPods, func() bool {
		pods, err := r.T.Pods(ctx)
		if err != nil || !replaced(pods) || !r.T.AllReady(pods) {
			return false
		}
		if !podsReady {
			podsReady, tr.PodsReadyMs = true, ms(r.Since()-t0)
		}
		_, _, ok := r.Prober.Agreed()
		return ok
	})
	settled := r.Since()
	if tr.Settled {
		tr.SettledMs = ms(settled - t0)
	}

	// What the prober and the clients saw in between.
	if at, ok := r.Prober.LastAnswerBeforeOutage(leader.Pod, t0); ok {
		tr.OldLeaderSeenGone, tr.OldLeaderLastAnswerMs = true, ms(at-t0)
	}
	if tr.NewLeaderSeen {
		for _, e := range r.Prober.LeadersBetween(elected.At, settled) {
			if e.Term > elected.Term {
				tr.ExtraLeaderChanges++
			}
		}
	}
	if r.Load != nil {
		// A client may still be in a retry pause when the cluster settles.
		waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
			at, term, ok := r.Load.FirstAckAbove(tr.OldTerm, t0)
			if ok {
				tr.FirstWriteSeen, tr.FirstWriteMs, tr.FirstWriteTerm = true, ms(at-t0), term
			}
			return ok
		})
		tr.LongestWriteGapMs = ms(r.Load.LongestWriteGap(t0, r.Since()))
	}

	// F2: each replacement found its term and its log on its volume.
	tr.FromDisk = tr.Settled
	for _, name := range tr.Deleted {
		st, err := r.T.StartedLine(ctx, name)
		if err != nil && tr.Error == "" {
			tr.Error = err.Error()
		}
		tr.Started[name] = st
		if !st.Found || st.Term == 0 || st.LastLogIndex == 0 {
			tr.FromDisk = false
		}
	}
	return tr
}

// TrialSummary totals a set of trials.
type TrialSummary struct {
	Trials int `json:"trials"`
	// Passed is the trials that saw a new leader and a first write, settled, and
	// had every replacement start from its volume.
	Passed          int `json:"passed"`
	NoNewLeader     int `json:"no_new_leader_within_60s"`
	NoFirstWrite    int `json:"no_first_write_seen"`
	NotSettled      int `json:"not_settled_within_180s"`
	NotFromDisk     int `json:"replacement_did_not_start_from_its_volume"`
	Errors          int `json:"errors"`
	ReplacementPods int `json:"replacement_pods"`

	NewLeaderMs           Dist `json:"new_leader_ms"`
	FirstWriteMs          Dist `json:"first_write_ms"`
	OldLeaderLastAnswerMs Dist `json:"old_leader_last_answer_ms"`
	KubectlMs             Dist `json:"kubectl_delete_returned_ms"`
	PodsReadyMs           Dist `json:"all_pods_ready_ms"`
	SettledMs             Dist `json:"settled_ms"`
	LongestWriteGapMs     Dist `json:"longest_write_gap_ms"`

	// ElectedInOneTerm counts the trials whose new leader was in the very next
	// term: the first election after the delete succeeded.
	ElectedInOneTerm int `json:"elected_in_one_term"`
	// FirstWriteAfterLeaderMs is how long after the new leader was seen the
	// first write was acknowledged. The cluster could take writes for all of
	// that time; the clients were between retries.
	FirstWriteAfterLeaderMs Dist `json:"first_write_after_new_leader_ms"`
	FirstWriteOver1s        int  `json:"first_write_took_over_1s"`

	TrialsWithExtraLeaderChanges int `json:"trials_with_extra_leader_changes"`
	ExtraLeaderChanges           int `json:"extra_leader_changes"`
	// NewLeaderByPod counts how often each pod took over.
	NewLeaderByPod map[string]int `json:"new_leader_by_pod"`
	DeletedByPod   map[string]int `json:"deleted_by_pod"`
}

// SummarizeTrials computes every number reported about a set of trials. A trial
// that didn't see something is counted as that, and is left out of that
// distribution only: nothing is dropped silently.
func SummarizeTrials(trials []Trial) TrialSummary {
	s := TrialSummary{Trials: len(trials), NewLeaderByPod: map[string]int{}, DeletedByPod: map[string]int{}}
	var leader, write, gone, kubectl, ready, settled, gap, lag []float64
	for _, tr := range trials {
		if tr.OK() {
			s.Passed++
		}
		if tr.Error != "" {
			s.Errors++
		}
		for _, pod := range tr.Deleted {
			s.DeletedByPod[pod]++
			s.ReplacementPods++
		}
		kubectl = append(kubectl, tr.KubectlMs)
		if tr.NewLeaderSeen {
			leader = append(leader, tr.NewLeaderMs)
			s.NewLeaderByPod[tr.NewLeader]++
			if tr.NewTerm == tr.OldTerm+1 {
				s.ElectedInOneTerm++
			}
		} else {
			s.NoNewLeader++
		}
		if tr.FirstWriteSeen {
			write = append(write, tr.FirstWriteMs)
			if tr.FirstWriteMs > 1000 {
				s.FirstWriteOver1s++
			}
			if tr.NewLeaderSeen {
				lag = append(lag, tr.FirstWriteMs-tr.NewLeaderMs)
			}
		} else {
			s.NoFirstWrite++
		}
		if tr.OldLeaderSeenGone {
			gone = append(gone, tr.OldLeaderLastAnswerMs)
		}
		if tr.Settled {
			ready, settled = append(ready, tr.PodsReadyMs), append(settled, tr.SettledMs)
			gap = append(gap, tr.LongestWriteGapMs)
		} else {
			s.NotSettled++
		}
		if !tr.FromDisk {
			s.NotFromDisk++
		}
		if tr.ExtraLeaderChanges > 0 {
			s.TrialsWithExtraLeaderChanges++
			s.ExtraLeaderChanges += tr.ExtraLeaderChanges
		}
	}
	s.NewLeaderMs, s.FirstWriteMs, s.OldLeaderLastAnswerMs = Summarize(leader), Summarize(write), Summarize(gone)
	s.KubectlMs, s.PodsReadyMs, s.SettledMs, s.LongestWriteGapMs = Summarize(kubectl), Summarize(ready), Summarize(settled), Summarize(gap)
	s.FirstWriteAfterLeaderMs = Summarize(lag)
	return s
}
