package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/abishekprabakar/raftkv/internal/client"
)

// Check is one pass-or-fail criterion from docs/acceptance/k8s.md.
type Check struct {
	ID     string `json:"id"`
	What   string `json:"what"`
	Pass   bool   `json:"pass"`
	Detail string `json:"detail"`
}

// Checks runs A1, A2, A4, K1 and K2 against a cluster that `terraform apply` has
// just made. It changes the cluster with kubectl in K1 and K2, and puts it back.
func Checks(t *Target, progress func(Check)) []Check {
	var all []Check
	add := func(id, what string, f func() (string, error)) {
		detail, err := f()
		c := Check{ID: id, What: what, Pass: err == nil, Detail: detail}
		if err != nil {
			c.Detail = strings.TrimSpace(detail + " FAILED: " + errorf("%v", err))
		}
		all = append(all, c)
		if progress != nil {
			progress(c)
		}
	}
	r, err := NewRun(t, "", 0, false, nil)
	if err != nil {
		return []Check{{ID: "A1", Detail: err.Error()}}
	}
	defer r.Prober.Stop()

	add("A1", "after one apply, 3 pods are Ready and the nodes agree on a leader", r.checkReadyNow)
	add("A2", "the StatefulSet, Services, budget, claims and probes are as specified", t.checkObjects)
	add("A4", "a write and a read through the client Service, from a pod and from the host", t.checkClientService)
	add("K1", "a node without a majority is Running, not Ready, and not restarted", r.checkReadinessFollowsRaft)
	add("K1-plan", "terraform plan reports no changes after K1", t.checkNoDrift)
	add("K2", "the disruption budget lets a drain evict only one pod", r.checkBudget)
	add("K2-plan", "terraform plan reports no changes after K2", t.checkNoDrift)
	return all
}

// checkReadyNow is A1. The pods are asked once, with no waiting: apply has
// returned, so they must be ready already. The prober gets two seconds only
// because it has just started and needs one answer from each node.
func (r *Run) checkReadyNow() (string, error) {
	pods, err := r.T.Pods(context.Background())
	if err != nil {
		return "", err
	}
	if !r.T.AllReady(pods) {
		return fmt.Sprintf("pods: %+v", pods), errors.New("not every pod was Ready when apply returned")
	}
	var leader Node
	var term uint64
	if !waitUntil(2*time.Second, 10*time.Millisecond, func() bool {
		var ok bool
		leader, term, ok = r.Prober.Agreed()
		return ok
	}) {
		return "", errors.New("the nodes did not name one leader")
	}
	return fmt.Sprintf("3 of 3 pods Ready; every node names %s (node %d) leader in term %d", leader.Pod, leader.ID, term), nil
}

func (t *Target) getJSON(kind, name string, into any) error {
	args := []string{"get", kind, "-o", "json"}
	if name != "" {
		args = []string{"get", kind, name, "-o", "json"}
	}
	out, err := t.poll(context.Background(), "kubectl", t.kubectlArgs(args)...)
	if err != nil {
		return err
	}
	return json.Unmarshal(out, into)
}

// checkObjects is A2: read each object back from the API server and compare it
// with what the criteria say, rather than trusting that Terraform asked for it.
func (t *Target) checkObjects() (string, error) {
	var problems []string
	want := func(ok bool, format string, args ...any) {
		if !ok {
			problems = append(problems, fmt.Sprintf(format, args...))
		}
	}
	type probe struct {
		HTTPGet struct {
			Path string `json:"path"`
		} `json:"httpGet"`
	}
	var sts struct {
		Spec struct {
			Replicas            int    `json:"replicas"`
			ServiceName         string `json:"serviceName"`
			PodManagementPolicy string `json:"podManagementPolicy"`
			UpdateStrategy      struct {
				Type string `json:"type"`
			} `json:"updateStrategy"`
			VolumeClaimTemplates []struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			} `json:"volumeClaimTemplates"`
			Template struct {
				Spec struct {
					Containers []struct {
						Liveness  *probe `json:"livenessProbe"`
						Readiness *probe `json:"readinessProbe"`
						Startup   *probe `json:"startupProbe"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if err := t.getJSON("statefulset", t.StatefulSet, &sts); err != nil {
		return "", err
	}
	want(sts.Spec.Replicas == len(t.Nodes), "StatefulSet has %d replicas", sts.Spec.Replicas)
	want(sts.Spec.PodManagementPolicy == "Parallel", "podManagementPolicy is %q", sts.Spec.PodManagementPolicy)
	want(sts.Spec.UpdateStrategy.Type == "RollingUpdate", "updateStrategy is %q", sts.Spec.UpdateStrategy.Type)
	want(len(sts.Spec.VolumeClaimTemplates) == 1, "StatefulSet has %d volume claim templates", len(sts.Spec.VolumeClaimTemplates))
	if c := sts.Spec.Template.Spec.Containers; len(c) == 1 {
		want(c[0].Liveness != nil && c[0].Liveness.HTTPGet.Path == "/livez", "no liveness probe on /livez")
		want(c[0].Readiness != nil && c[0].Readiness.HTTPGet.Path == "/readyz", "no readiness probe on /readyz")
		want(c[0].Startup != nil && c[0].Startup.HTTPGet.Path == "/livez", "no startup probe on /livez")
	} else {
		want(false, "the pod has %d containers", len(c))
	}

	type service struct {
		Spec struct {
			ClusterIP                string `json:"clusterIP"`
			Type                     string `json:"type"`
			PublishNotReadyAddresses bool   `json:"publishNotReadyAddresses"`
		} `json:"spec"`
	}
	var peers, clientSvc service
	if err := t.getJSON("service", sts.Spec.ServiceName, &peers); err != nil {
		return "", err
	}
	want(peers.Spec.ClusterIP == "None", "the StatefulSet's Service %s is not headless: clusterIP %q", sts.Spec.ServiceName, peers.Spec.ClusterIP)
	want(peers.Spec.PublishNotReadyAddresses, "the headless Service does not publish addresses that are not ready")
	clientName, _, _ := strings.Cut(t.ClientInside, ".")
	if err := t.getJSON("service", clientName, &clientSvc); err != nil {
		return "", err
	}
	want(clientSvc.Spec.ClusterIP != "None" && clientSvc.Spec.ClusterIP != "", "the client Service has no cluster IP")
	want(!clientSvc.Spec.PublishNotReadyAddresses, "the client Service routes to pods that are not ready")

	var pdbs struct {
		Items []struct {
			Spec struct {
				MaxUnavailable json.RawMessage `json:"maxUnavailable"`
			} `json:"spec"`
			Status struct {
				DisruptionsAllowed int `json:"disruptionsAllowed"`
				CurrentHealthy     int `json:"currentHealthy"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := t.getJSON("poddisruptionbudget", "", &pdbs); err != nil {
		return "", err
	}
	if len(pdbs.Items) == 1 {
		p := pdbs.Items[0]
		want(strings.Trim(string(p.Spec.MaxUnavailable), `"`) == "1", "the budget's maxUnavailable is %s", p.Spec.MaxUnavailable)
		want(p.Status.DisruptionsAllowed == 1 && p.Status.CurrentHealthy == len(t.Nodes),
			"the budget sees %d healthy pods and allows %d disruptions", p.Status.CurrentHealthy, p.Status.DisruptionsAllowed)
	} else {
		want(false, "%d disruption budgets", len(pdbs.Items))
	}

	var pvcs struct {
		Items []struct {
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := t.getJSON("persistentvolumeclaim", "", &pvcs); err != nil {
		return "", err
	}
	bound := 0
	for _, p := range pvcs.Items {
		if p.Status.Phase == "Bound" {
			bound++
		}
	}
	want(bound == len(t.Nodes) && len(pvcs.Items) == len(t.Nodes), "%d volume claims, %d Bound", len(pvcs.Items), bound)

	if len(problems) > 0 {
		return "", errors.New(strings.Join(problems, "; "))
	}
	return fmt.Sprintf("StatefulSet: %d replicas, Parallel start, RollingUpdate, 3 probes; headless Service %s publishes not-ready addresses; client Service %s; budget maxUnavailable 1, 1 disruption allowed; %d claims Bound",
		sts.Spec.Replicas, sts.Spec.ServiceName, clientName, bound), nil
}

func (t *Target) hostClient() (*client.Client, error) {
	return client.New([]string{t.Client}, client.Options{Dial: t.Dial})
}

// checkClientService is A4.
func (t *Target) checkClientService() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stamp := fmt.Sprint(time.Now().UnixNano())
	// From inside: the CLI in one pod writes through the Service's DNS name, and
	// the CLI in another reads it back.
	cli := func(pod string, args ...string) (string, error) {
		out, err := t.Kubectl(ctx, append([]string{"exec", pod, "--", "/usr/local/bin/raftkv", "--addr", t.ClientInside}, args...)...)
		return strings.TrimSpace(string(out)), err
	}
	if _, err := cli(t.Nodes[0].Pod, "put", "check.inside", stamp); err != nil {
		return "", err
	}
	if got, err := cli(t.Nodes[len(t.Nodes)-1].Pod, "get", "check.inside"); err != nil || got != stamp {
		return "", fmt.Errorf("read %q from inside the cluster, want %q: %v", got, stamp, err)
	}
	// From the host: the client library, through the mapped port.
	c, err := t.hostClient()
	if err != nil {
		return "", err
	}
	defer c.Close()
	if err := c.Put(ctx, []byte("check.host"), []byte(stamp)); err != nil {
		return "", err
	}
	if v, found, err := c.Get(ctx, []byte("check.host")); err != nil || !found || string(v) != stamp {
		return "", fmt.Errorf("read %q from the host, want %q: %v", v, stamp, err)
	}
	return fmt.Sprintf("put and get through %s from two pods, and through %s from the host", t.ClientInside, t.Client), nil
}

// checkReadinessFollowsRaft is K1. Scaling the StatefulSet to one replica is the
// fault: it leaves one node running with no majority. Nothing is wrong with that
// node's process, so it must not be restarted, and it can't serve anything, so
// it must not be ready.
func (r *Run) checkReadinessFollowsRaft() (detail string, err error) {
	const hold = 20 * time.Second
	ctx := context.Background()
	t := r.T
	c, err := t.hostClient()
	if err != nil {
		return "", err
	}
	defer c.Close()
	stamp := fmt.Sprint(time.Now().UnixNano())
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = c.Put(wctx, []byte("check.k1"), []byte(stamp))
	cancel()
	if err != nil {
		return "", err
	}
	lone := t.Nodes[0].Pod // scaling down removes the highest ordinals
	find := func() (Pod, bool) {
		pods, err := t.Pods(ctx)
		for _, p := range pods {
			if err == nil && p.Name == lone {
				return p, true
			}
		}
		return Pod{}, false
	}
	before, ok := find()
	if !ok {
		return "", fmt.Errorf("no pod %s", lone)
	}

	// Whatever happens next, put the replicas back.
	defer func() {
		if _, e := t.Kubectl(ctx, "scale", "statefulset", t.StatefulSet, "--replicas", fmt.Sprint(len(t.Nodes))); e != nil && err == nil {
			err = e
		}
		if !r.WaitHealthy(120*time.Second) && err == nil {
			err = errors.New("not all pods were Ready, with an agreed leader, within 120 s of scaling back")
		}
		if err != nil {
			return
		}
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if v, found, e := c.Get(rctx, []byte("check.k1")); e != nil || !found || string(v) != stamp {
			err = fmt.Errorf("after scaling back, check.k1 read %q, want %q: %v", v, stamp, e)
		}
	}()

	if _, err := t.Kubectl(ctx, "scale", "statefulset", t.StatefulSet, "--replicas", "1"); err != nil {
		return "", err
	}
	t0 := time.Now()
	if !waitUntil(15*time.Second, pollPods, func() bool {
		p, ok := find()
		return ok && p.Phase == "Running" && !p.Ready
	}) {
		return "", fmt.Errorf("%s was still Ready 15 s after losing its majority", lone)
	}
	notReadyAfter := time.Since(t0)
	time.Sleep(hold - notReadyAfter)
	after, ok := find()
	switch {
	case !ok || after.UID != before.UID:
		return "", fmt.Errorf("%s was replaced during the hold", lone)
	case after.Phase != "Running" || after.Ready:
		return "", fmt.Errorf("%s is %s, Ready %v, at the end of the hold", lone, after.Phase, after.Ready)
	case after.Restarts != before.Restarts:
		return "", fmt.Errorf("%s was restarted %d times: liveness failed, and it should not have", lone, after.Restarts-before.Restarts)
	}
	// The kubelet records why the readiness probe failed.
	reason, _ := t.Kubectl(ctx, "get", "events", "--field-selector", "involvedObject.name="+lone+",reason=Unhealthy",
		"-o", "jsonpath={.items[-1:].message}")
	return fmt.Sprintf("%s alone: not Ready %.1f s after the scale-down, still Running with %d restarts after %v. The kubelet's last probe failure: %s. Scaled back: 3 Ready, and the key written before reads back",
		lone, notReadyAfter.Seconds(), after.Restarts-before.Restarts, hold, errorf("%s", reason)), nil
}

var evicted = regexp.MustCompile(`pod/(\S+) evicted`)

// checkBudget is K2. A drain is the standard voluntary disruption: it asks the
// API server to evict each pod, and the API server refuses any eviction that
// would break a budget. The node is cordoned, so the one pod that is evicted
// can't come back, and the budget has to hold the other two for the whole drain.
func (r *Run) checkBudget() (detail string, err error) {
	const drainFor = 30 * time.Second
	ctx := context.Background()
	t := r.T
	out, err := t.Kubectl(ctx, "get", "nodes", "-o", "jsonpath={.items[0].metadata.name}")
	if err != nil {
		return "", err
	}
	node := strings.TrimSpace(string(out))
	defer func() {
		if _, e := t.Kubectl(ctx, "uncordon", node); e != nil && err == nil {
			err = e
		}
		if !r.WaitHealthy(120*time.Second) && err == nil {
			err = errors.New("not all pods were Ready, with an agreed leader, within 120 s of the uncordon")
		}
	}()

	drain := exec.Command("kubectl", t.kubectlArgs([]string{"drain", node, "--pod-selector", Selector,
		"--ignore-daemonsets", "--delete-emptydir-data", "--timeout", drainFor.String()})...)
	var output bytes.Buffer
	drain.Stdout, drain.Stderr = &output, &output
	if err := drain.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- drain.Wait() }()

	// While the drain keeps trying: count ready pods, and write once.
	minReady, wrote, start := len(t.Nodes), false, time.Now()
	var writeErr, drainErr error
	c, err := t.hostClient()
	if err != nil {
		return "", err
	}
	defer c.Close()
	for running := true; running; {
		select {
		case drainErr = <-done:
			running = false
		case <-time.After(500 * time.Millisecond):
		}
		if pods, err := t.Pods(ctx); err == nil && time.Since(start) > 2*time.Second {
			ready := 0
			for _, p := range pods {
				if p.Ready && !p.Deleting {
					ready++
				}
			}
			minReady = min(minReady, ready)
		}
		if !wrote && time.Since(start) > drainFor/2 {
			wrote = true
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			writeErr = c.Put(wctx, []byte("check.k2"), []byte("written during the drain"))
			cancel()
		}
	}
	if t.Log != nil {
		fmt.Fprintf(t.Log, "$ kubectl drain %s ...\n%s", node, output.Bytes())
	}
	gone := map[string]bool{}
	for _, m := range evicted.FindAllStringSubmatch(output.String(), -1) {
		gone[m[1]] = true
	}
	switch {
	case drainErr == nil:
		return "", errors.New("the drain succeeded: nothing stopped it evicting every pod")
	case len(gone) != 1:
		return "", fmt.Errorf("the drain evicted %d raftkv pods, want exactly 1", len(gone))
	case !strings.Contains(output.String(), "disruption budget"):
		return "", errors.New("the drain failed, but not on the disruption budget")
	case minReady != len(t.Nodes)-1:
		return "", fmt.Errorf("the fewest Ready pods during the drain was %d, want %d", minReady, len(t.Nodes)-1)
	case !wrote || writeErr != nil:
		return "", fmt.Errorf("a write during the drain failed: %v", writeErr)
	}
	var name string
	for n := range gone {
		name = n
	}
	return fmt.Sprintf("the drain evicted %s and was refused the other two for %v (\"Cannot evict pod as it would violate the pod's disruption budget\"); 2 pods stayed Ready and a write succeeded meanwhile; after uncordon, 3 Ready",
		name, drainFor), nil
}

// checkNoDrift runs `terraform plan`. Exit status 0 means the cluster still
// matches the configuration; 2 means something would change.
func (t *Target) checkNoDrift() (string, error) {
	_, err := t.Terraform(context.Background(), "plan", "-detailed-exitcode", "-input=false", "-no-color", "-lock=false")
	if err != nil {
		return "", err
	}
	return "terraform plan -detailed-exitcode exited 0", nil
}
