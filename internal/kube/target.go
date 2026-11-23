// Package kube measures a raftkv cluster running on Kubernetes: leader failover,
// durability across pod deletions, a rolling update and a cold start. It drives
// the cluster the way an operator would, with kubectl and terraform, and watches
// it the way a client would, over gRPC.
//
// Everything is timed by one clock, in this process. The harness starts each
// kubectl command itself, polls every node's Status itself, and runs the clients
// itself, so no measurement compares a time on the host with a time inside the
// Docker VM.
package kube

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Selector picks out the raftkv pods, by the label Terraform gives them.
const Selector = "app.kubernetes.io/name=raftkv"

// Node is one raftkv node as Terraform deployed it.
type Node struct {
	Pod string `json:"pod"`
	ID  int    `json:"id"`
	// Address is what the nodes call it: the pod's DNS name inside the cluster.
	// It is the address a redirect carries.
	Address string `json:"address"`
	// HostAddress reaches the same node from the host, through a port kind maps.
	HostAddress string `json:"host_address"`
}

// Target is a deployed cluster, read from `terraform output`.
type Target struct {
	Dir           string // the Terraform directory
	Kubeconfig    string
	Namespace     string
	StatefulSet   string
	Image         string
	NodeContainer string
	Client        string // the client Service, from the host
	ClientInside  string // the client Service, from a pod
	Nodes         []Node

	// Log receives every command the harness runs and its output.
	Log io.Writer
}

// LoadTarget reads the outputs of the Terraform configuration in dir.
func LoadTarget(dir string, log io.Writer) (*Target, error) {
	t := &Target{Dir: dir, Log: log}
	out, err := t.Terraform(context.Background(), "output", "-json")
	if err != nil {
		return nil, err
	}
	var raw map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, fmt.Errorf("terraform output: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("terraform output in %s is empty: has `terraform apply` run?", dir)
	}
	for name, dst := range map[string]any{
		"kubeconfig": &t.Kubeconfig, "namespace": &t.Namespace, "statefulset": &t.StatefulSet,
		"image": &t.Image, "node_container": &t.NodeContainer, "client_address": &t.Client,
		"client_address_in_cluster": &t.ClientInside, "nodes": &t.Nodes,
	} {
		if err := json.Unmarshal(raw[name].Value, dst); err != nil {
			return nil, fmt.Errorf("terraform output %q: %w", name, err)
		}
	}
	return t, nil
}

// run executes one command, logs it, and returns its standard output. Standard
// error goes to the log, and into the error if the command fails.
func (t *Target) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return t.exec(ctx, true, name, args...)
}

// poll is run without the log, for a command repeated many times a second while
// waiting, whose output would bury everything else.
func (t *Target) poll(ctx context.Context, name string, args ...string) ([]byte, error) {
	return t.exec(ctx, false, name, args...)
}

func (t *Target) exec(ctx context.Context, logged bool, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if t.Log != nil && logged {
		fmt.Fprintf(t.Log, "$ %s %s\n%s%s", name, strings.Join(args, " "), stdout.Bytes(), stderr.Bytes())
	}
	if err != nil {
		return stdout.Bytes(), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// Terraform runs terraform in the target's directory. Variables set in the
// environment (TF_VAR_...) reach it, so a run on a planted image or without
// volumes keeps those settings in every apply the harness makes.
func (t *Target) Terraform(ctx context.Context, args ...string) ([]byte, error) {
	return t.run(ctx, "terraform", append([]string{"-chdir=" + t.Dir}, args...)...)
}

// Kubectl runs kubectl against the target's cluster and namespace.
func (t *Target) Kubectl(ctx context.Context, args ...string) ([]byte, error) {
	return t.run(ctx, "kubectl", t.kubectlArgs(args)...)
}

func (t *Target) kubectlArgs(args []string) []string {
	return append([]string{"--kubeconfig", t.Kubeconfig, "--namespace", t.Namespace}, args...)
}

// Dial is the client library's dial hook. A redirect names a pod by its DNS name
// inside the cluster, which the host can't resolve, so that name is swapped for
// the host port kind maps to the same pod. Any other address is dialed as it is.
func (t *Target) Dial(ctx context.Context, addr string) (net.Conn, error) {
	for _, n := range t.Nodes {
		if n.Address == addr {
			addr = n.HostAddress
			break
		}
	}
	return (&net.Dialer{}).DialContext(ctx, "tcp", addr)
}

// NodeByPod returns the node that runs in the named pod.
func (t *Target) NodeByPod(pod string) (Node, bool) {
	for _, n := range t.Nodes {
		if n.Pod == pod {
			return n, true
		}
	}
	return Node{}, false
}

// Pod is what the harness needs to know about one pod.
type Pod struct {
	Name     string
	UID      string
	Phase    string
	Ready    bool
	Deleting bool // it has a deletion timestamp
	Restarts int
	Image    string
}

// Pods lists the StatefulSet's pods.
func (t *Target) Pods(ctx context.Context) ([]Pod, error) {
	out, err := t.poll(ctx, "kubectl", t.kubectlArgs([]string{"get", "pods", "-l", Selector, "-o", "json"})...)
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name              string  `json:"name"`
				UID               string  `json:"uid"`
				DeletionTimestamp *string `json:"deletionTimestamp"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Image string `json:"image"`
				} `json:"containers"`
			} `json:"spec"`
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
				ContainerStatuses []struct {
					RestartCount int `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	var pods []Pod
	for _, it := range list.Items {
		p := Pod{Name: it.Metadata.Name, UID: it.Metadata.UID, Phase: it.Status.Phase, Deleting: it.Metadata.DeletionTimestamp != nil}
		for _, c := range it.Status.Conditions {
			if c.Type == "Ready" && c.Status == "True" {
				p.Ready = true
			}
		}
		for _, c := range it.Status.ContainerStatuses {
			p.Restarts += c.RestartCount
		}
		if len(it.Spec.Containers) > 0 {
			p.Image = it.Spec.Containers[0].Image
		}
		pods = append(pods, p)
	}
	return pods, nil
}

// AllReady reports whether every node's pod exists, is ready, and is not on its
// way out. A pod that has been deleted stays listed, and can stay ready, until
// its process has gone, so a pod with a deletion timestamp never counts.
func (t *Target) AllReady(pods []Pod) bool {
	ready := 0
	for _, p := range pods {
		if p.Ready && !p.Deleting {
			ready++
		}
	}
	return ready == len(t.Nodes) && len(pods) == len(t.Nodes)
}

// Started is the line a node logs when it starts, which says what it found on
// its disk.
type Started struct {
	Found        bool   `json:"found"`
	Term         uint64 `json:"term"`
	LastLogIndex uint64 `json:"last_log_index"`
	Applied      uint64 `json:"applied"`
}

// StartedLine reads the pod's log for the line its current container logged at
// startup.
func (t *Target) StartedLine(ctx context.Context, pod string) (Started, error) {
	out, err := t.Kubectl(ctx, "logs", pod, "--tail=-1")
	if err != nil {
		return Started{}, err
	}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.Contains(line, "msg=started ") {
			continue
		}
		s := Started{Found: true}
		for _, f := range strings.Fields(line) {
			k, v, _ := strings.Cut(f, "=")
			switch k {
			case "term":
				fmt.Sscan(v, &s.Term)
			case "last_log_index":
				fmt.Sscan(v, &s.LastLogIndex)
			case "applied":
				fmt.Sscan(v, &s.Applied)
			}
		}
		return s, nil
	}
	return Started{}, nil
}

// Versions records what the measurements ran on.
type Versions struct {
	Docker     string `json:"docker"`
	Kubernetes string `json:"kubernetes"` // the cluster's version
	Kubectl    string `json:"kubectl"`
	NodeImage  string `json:"kind_node_image"`
	Terraform  string `json:"terraform"`
	DockerCPUs string `json:"docker_cpus"`
	DockerMem  string `json:"docker_memory_bytes"`
	Commit     string `json:"source_commit"`
	Dirty      bool   `json:"source_tree_had_changes"`
	OnBattery  string `json:"power,omitempty"`
}

// CollectVersions asks each tool for its version. A tool that doesn't answer
// leaves its field empty.
func (t *Target) CollectVersions(ctx context.Context) Versions {
	quiet := &Target{Dir: t.Dir, Kubeconfig: t.Kubeconfig, Namespace: t.Namespace}
	text := func(out []byte, _ error) string { return strings.TrimSpace(string(out)) }
	v := Versions{
		Docker:     text(quiet.run(ctx, "docker", "version", "--format", "{{.Server.Version}}")),
		DockerCPUs: text(quiet.run(ctx, "docker", "info", "--format", "{{.NCPU}}")),
		DockerMem:  text(quiet.run(ctx, "docker", "info", "--format", "{{.MemTotal}}")),
		Commit:     text(quiet.run(ctx, "git", "rev-parse", "HEAD")),
	}
	v.Dirty = text(quiet.run(ctx, "git", "status", "--porcelain", "--untracked-files=no")) != ""
	if t.NodeContainer != "" {
		v.NodeImage = text(quiet.run(ctx, "docker", "inspect", "--format", "{{.Config.Image}}", t.NodeContainer))
	}
	var kv struct {
		ClientVersion struct{ GitVersion string } `json:"clientVersion"`
		ServerVersion struct{ GitVersion string } `json:"serverVersion"`
	}
	args := []string{"version", "-o", "json"}
	if t.Kubeconfig != "" {
		args = append([]string{"--kubeconfig", t.Kubeconfig}, args...)
	}
	if out, _ := quiet.run(ctx, "kubectl", args...); json.Unmarshal(out, &kv) == nil {
		v.Kubectl, v.Kubernetes = kv.ClientVersion.GitVersion, kv.ServerVersion.GitVersion
	}
	var tv struct {
		Version string `json:"terraform_version"`
	}
	if out, _ := quiet.run(ctx, "terraform", "version", "-json"); json.Unmarshal(out, &tv) == nil {
		v.Terraform = tv.Version
	}
	if out, err := quiet.run(ctx, "pmset", "-g", "batt"); err == nil {
		if line, _, _ := strings.Cut(string(out), "\n"); strings.Contains(line, "'") {
			v.OnBattery = strings.Trim(line[strings.Index(line, "'"):], "'")
		}
	}
	return v
}

// waitUntil polls cond every interval until it holds or limit passes.
func waitUntil(limit, interval time.Duration, cond func() bool) bool {
	for deadline := time.Now().Add(limit); ; time.Sleep(interval) {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// fileExists reports whether path names a file.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
