package chaos

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// network is the Compose project's network. Cutting a node off is removing its
// container from this network, which is all `docker network disconnect` does.
const network = "raftkv_default"

// cluster drives the Docker Compose project in dir. Every command it runs is
// written to log.
type cluster struct {
	dir   string
	image string
	log   io.Writer
	ids   [Nodes + 1]string // container IDs, by node number
}

func (c *cluster) run(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = c.dir
	cmd.Env = append(os.Environ(), "RAFTKV_IMAGE="+c.image)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = c.log
	err := cmd.Run()
	if err != nil {
		err = fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return out.Bytes(), err
}

func (c *cluster) compose(args ...string) ([]byte, error) {
	return c.run("docker", append([]string{"compose"}, args...)...)
}

// up starts a fresh cluster: no containers, no volumes, nothing left from before.
func (c *cluster) up() error {
	if _, err := c.compose("down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	if _, err := c.compose("up", "-d", "--wait"); err != nil {
		return err
	}
	for i := 1; i <= Nodes; i++ {
		out, err := c.compose("ps", "-q", nodeName(i))
		if err != nil {
			return err
		}
		c.ids[i] = strings.TrimSpace(string(out))
	}
	return nil
}

func (c *cluster) down() error {
	_, err := c.compose("down", "-v", "--remove-orphans")
	return err
}

func nodeName(i int) string { return fmt.Sprintf("node%d", i) }

func (c *cluster) disconnect(node int) error {
	_, err := c.run("docker", "network", "disconnect", network, c.ids[node])
	return err
}

// connect puts a node back. The alias matters: Compose gave the container its
// service name on this network, and a plain reconnect would lose it, leaving the
// other nodes unable to resolve "node1".
func (c *cluster) connect(node int) error {
	_, err := c.run("docker", "network", "connect", "--alias", nodeName(node), network, c.ids[node])
	return err
}

type nodeStatus struct {
	role   string
	term   uint64
	leader int
}

// status asks a node about itself, from inside its own container, so it works
// however the node is cut off.
func (c *cluster) status(node int) (nodeStatus, error) {
	out, err := c.run("docker", "exec", c.ids[node], "/usr/local/bin/raftkv", "--addr", "localhost:7000", "--timeout", "2s", "status")
	if err != nil {
		return nodeStatus{}, err
	}
	var st nodeStatus
	for _, f := range strings.Fields(string(out)) {
		k, v, _ := strings.Cut(f, "=")
		switch k {
		case "role":
			st.role = v
		case "term":
			st.term, _ = strconv.ParseUint(v, 10, 64)
		case "leader":
			st.leader, _ = strconv.Atoi(v)
		}
	}
	return st, nil
}

func (c *cluster) statuses() [Nodes + 1]*nodeStatus {
	var all [Nodes + 1]*nodeStatus
	var wg sync.WaitGroup
	for i := 1; i <= Nodes; i++ {
		wg.Go(func() {
			if st, err := c.status(i); err == nil {
				all[i] = &st
			}
		})
	}
	wg.Wait()
	return all
}

// leader returns the node that calls itself leader in the highest term, or 0.
func (c *cluster) leader() (int, uint64) {
	best, term := 0, uint64(0)
	for i, st := range c.statuses() {
		if st != nil && st.role == "leader" && st.term > term {
			best, term = i, st.term
		}
	}
	return best, term
}

// agreed reports whether all three nodes name the same leader in the same term.
func (c *cluster) agreed() bool {
	all := c.statuses()
	for i := 1; i <= Nodes; i++ {
		if all[i] == nil || all[i].leader == 0 || all[i].leader != all[1].leader || all[i].term != all[1].term {
			return false
		}
	}
	return true
}

func (c *cluster) logs() ([]byte, error) {
	return c.compose("logs", "--no-color")
}

// watchEvents starts recording Docker's network events as JSON lines, and stop
// ends the recording and returns them. They are streamed as they happen because
// asking afterwards with --since doesn't work: Docker keeps only a short backlog
// of past events, and a run's many docker exec calls push the disconnects out of it.
func (c *cluster) watchEvents() (stop func() []byte, err error) {
	cmd := exec.Command("docker", "events", "--filter", "type=network", "--format", "{{json .}}")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = c.log
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return func() []byte {
		time.Sleep(500 * time.Millisecond) // let the last events arrive
		cmd.Process.Kill()
		cmd.Wait()
		return buf.Bytes()
	}, nil
}

// worker is one client process, kvworker, running inside a node's container.
type worker struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Scanner
}

type workerResult struct {
	OK    bool   `json:"ok"`
	Found bool   `json:"found"`
	Value string `json:"value"`
	Err   string `json:"err"`
}

func (c *cluster) startWorker(node int) (*worker, error) {
	// Its own node first, by an address that needs no network, then everyone.
	addrs := "localhost:7000,node1:7000,node2:7000,node3:7000"
	cmd := exec.Command("docker", "exec", "-i", c.ids[node], "/usr/local/bin/kvworker",
		"-addr", addrs, "-timeout", OpTimeout.String(), "-attempt", AttemptTimeout.String())
	cmd.Stderr = c.log
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return &worker{cmd: cmd, in: in, out: sc}, nil
}

func (w *worker) do(in Input) (workerResult, error) {
	req, _ := json.Marshal(map[string]string{"op": in.Kind.String(), "key": in.Key, "value": in.Value})
	if _, err := w.in.Write(append(req, '\n')); err != nil {
		return workerResult{}, err
	}
	if !w.out.Scan() {
		return workerResult{}, fmt.Errorf("worker ended: %v", w.out.Err())
	}
	var res workerResult
	err := json.Unmarshal(w.out.Bytes(), &res)
	return res, err
}

func (w *worker) close() {
	w.in.Close()
	w.cmd.Wait()
}
