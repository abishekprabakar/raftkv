package kube

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"
)

// coldStartLimit bounds one `terraform apply` from nothing.
const coldStartLimit = 15 * time.Minute

// ColdRound is one destroy followed by one apply from nothing. Times are from
// the moment before `terraform apply` was started.
type ColdRound struct {
	N              int     `json:"round"`
	DestroySeconds float64 `json:"terraform_destroy_seconds"`
	// NothingLeft means that after the destroy there was no node container and
	// the Terraform state was empty.
	NothingLeft bool `json:"nothing_left_after_destroy"`
	// APIServerSeconds is the first kubectl answer from the new cluster.
	APIServerSeconds float64 `json:"api_server_answered_seconds"`
	// PodsCreatedSeconds is the first moment all three pods exist.
	PodsCreatedSeconds float64 `json:"pods_created_seconds"`
	// ReadySeconds is the cold start time: all 3 pods Ready and all 3 nodes
	// naming the same leader in the same term.
	ReadySeconds float64 `json:"ready_with_leader_seconds"`
	Ready        bool    `json:"ready_with_leader"`
	// ApplySeconds is when `terraform apply` itself returned.
	ApplySeconds float64 `json:"terraform_apply_seconds"`
	ApplyOK      bool    `json:"terraform_apply_succeeded"`
	Error        string  `json:"error,omitempty"`
}

// OK reports whether the round did what C1 asks.
func (c ColdRound) OK() bool { return c.Error == "" && c.NothingLeft && c.Ready && c.ApplyOK }

// Destroy runs `terraform destroy` and checks that nothing is left.
func (t *Target) Destroy(ctx context.Context) (seconds float64, nothingLeft bool, err error) {
	t0 := time.Now()
	_, err = t.Terraform(ctx, "destroy", "-auto-approve", "-input=false", "-no-color")
	seconds = time.Since(t0).Seconds()
	if err != nil {
		return seconds, false, err
	}
	state, err := t.Terraform(ctx, "state", "list")
	if err != nil {
		return seconds, false, err
	}
	containers, err := t.run(ctx, "docker", "ps", "--all", "--quiet", "--filter", "name=^"+t.NodeContainer+"$")
	if err != nil {
		return seconds, false, err
	}
	return seconds, strings.TrimSpace(string(state)) == "" && strings.TrimSpace(string(containers)) == "", nil
}

// ColdStartRound destroys everything, then times one `terraform apply`.
func ColdStartRound(t *Target, n int, out io.Writer) ColdRound {
	ctx, cancel := context.WithTimeout(context.Background(), coldStartLimit)
	defer cancel()
	c := ColdRound{N: n}
	var err error
	if c.DestroySeconds, c.NothingLeft, err = t.Destroy(ctx); err != nil {
		c.Error = errorf("%v", err)
		return c
	}

	start := time.Now()
	since := func() time.Duration { return time.Since(start) }
	// The prober starts with nothing to talk to. Its connections fail until
	// Docker maps the ports and the pods listen, and it retries every 100 ms at
	// most, so it sees a leader as soon as there is one.
	prober := StartProber(t, since)
	defer prober.Stop()

	applied := make(chan error, 1)
	go func() {
		_, err := t.Terraform(ctx, "apply", "-auto-approve", "-input=false", "-no-color")
		applied <- err
	}()

	var applyDone bool
	for !c.Ready || !applyDone {
		select {
		case err := <-applied:
			applyDone, c.ApplySeconds, c.ApplyOK = true, since().Seconds(), err == nil
			if err != nil {
				c.Error = errorf("%v", err)
				return c
			}
		case <-ctx.Done():
			c.Error = fmt.Sprintf("not ready within %v", coldStartLimit)
			return c
		case <-time.After(100 * time.Millisecond):
		}
		if c.Ready || !fileExists(t.Kubeconfig) {
			continue
		}
		pods, err := t.Pods(ctx)
		if err != nil {
			continue
		}
		if c.APIServerSeconds == 0 {
			c.APIServerSeconds = since().Seconds()
		}
		if c.PodsCreatedSeconds == 0 && len(pods) == len(t.Nodes) {
			c.PodsCreatedSeconds = since().Seconds()
		}
		if _, _, agreed := prober.Agreed(); agreed && t.AllReady(pods) {
			c.Ready, c.ReadySeconds = true, since().Seconds()
		}
	}
	return c
}

// ColdSummary totals a set of rounds.
type ColdSummary struct {
	Rounds             int  `json:"rounds"`
	Passed             int  `json:"passed"`
	ReadySeconds       Dist `json:"ready_with_leader_seconds"`
	ApplySeconds       Dist `json:"terraform_apply_seconds"`
	APIServerSeconds   Dist `json:"api_server_answered_seconds"`
	PodsCreatedSeconds Dist `json:"pods_created_seconds"`
	DestroySeconds     Dist `json:"terraform_destroy_seconds"`
}

// SummarizeColdStarts computes every number reported about the rounds.
func SummarizeColdStarts(rounds []ColdRound) ColdSummary {
	s := ColdSummary{Rounds: len(rounds)}
	var ready, apply, api, pods, destroy []float64
	for _, c := range rounds {
		if c.OK() {
			s.Passed++
		}
		if c.Ready {
			ready = append(ready, c.ReadySeconds)
		}
		if c.ApplyOK {
			apply = append(apply, c.ApplySeconds)
			api, pods = append(api, c.APIServerSeconds), append(pods, c.PodsCreatedSeconds)
		}
		destroy = append(destroy, c.DestroySeconds)
	}
	s.ReadySeconds, s.ApplySeconds, s.DestroySeconds = Summarize(ready), Summarize(apply), Summarize(destroy)
	s.APIServerSeconds, s.PodsCreatedSeconds = Summarize(api), Summarize(pods)
	return s
}
