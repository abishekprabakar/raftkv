package chaos

import (
	"bufio"
	"bytes"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"time"
)

// Failover time is read from two logs written by the same clock, the Docker VM's:
// Docker's own network events say when a node was cut off, and the nodes' logs say
// when another node became leader. The harness's clock, on the host, is never
// mixed in.

// stateLine is one "raft state" line from a node's log. The server writes one
// whenever the node's role, term or known leader changes.
type stateLine struct {
	At   time.Time
	Node int
	Role string
	Term uint64
}

var stateRE = regexp.MustCompile(`time=(\S+) level=\S+ msg="raft state" node=(\d+) role=(\w+) term=(\d+)`)

func parseStates(logs []byte) []stateLine {
	var lines []stateLine
	sc := bufio.NewScanner(bytes.NewReader(logs))
	for sc.Scan() {
		m := stateRE.FindSubmatch(sc.Bytes())
		if m == nil {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, string(m[1]))
		if err != nil {
			continue
		}
		node, _ := strconv.Atoi(string(m[2]))
		term, _ := strconv.ParseUint(string(m[4]), 10, 64)
		lines = append(lines, stateLine{At: at, Node: node, Role: string(m[3]), Term: term})
	}
	slices.SortStableFunc(lines, func(a, b stateLine) int { return a.At.Compare(b.At) })
	return lines
}

// netEvent is a container leaving or joining the Compose network.
type netEvent struct {
	At     time.Time
	Node   int
	Action string // "disconnect" or "connect"
}

func parseEvents(raw []byte, ids [Nodes + 1]string) []netEvent {
	var events []netEvent
	sc := bufio.NewScanner(bytes.NewReader(raw))
	for sc.Scan() {
		var e struct {
			Action   string
			Actor    struct{ Attributes map[string]string }
			TimeNano int64 `json:"timeNano"`
		}
		if json.Unmarshal(sc.Bytes(), &e) != nil || e.Actor.Attributes["name"] != network {
			continue
		}
		for node := 1; node <= Nodes; node++ {
			if ids[node] != "" && e.Actor.Attributes["container"] == ids[node] {
				events = append(events, netEvent{At: time.Unix(0, e.TimeNano), Node: node, Action: e.Action})
			}
		}
	}
	slices.SortStableFunc(events, func(a, b netEvent) int { return a.At.Compare(b.At) })
	return events
}

// Failover is one leader isolation, measured from the logs.
type Failover struct {
	Isolated int    `json:"isolated"`
	OldTerm  uint64 `json:"old_term"`
	// NewLeader is 0 if no other node became leader before the partition healed.
	NewLeader int     `json:"new_leader"`
	NewTerm   uint64  `json:"new_term"`
	Ms        float64 `json:"ms"`
	Note      string  `json:"note,omitempty"`
}

// failovers measures every leader isolation in faults. The harness cuts nodes off
// one command at a time, in order, so the k-th disconnect event for a node is the
// k-th time the harness cut that node off.
func failovers(faults []FaultRecord, events []netEvent, states []stateLine) []Failover {
	seen := map[int]int{} // how many times each node has been cut off so far
	var out []Failover
	for _, f := range faults {
		for _, node := range f.Targets {
			seen[node]++
		}
		if f.Kind != IsolateLeader || len(f.Targets) != 1 {
			continue
		}
		node := f.Targets[0]
		fo := Failover{Isolated: node}
		cut, healed, ok := window(events, node, seen[node])
		if !ok {
			fo.Note = "no matching disconnect event"
			out = append(out, fo)
			continue
		}
		for _, s := range states {
			if s.At.After(cut) {
				break
			}
			if s.Node == node && s.Role == "leader" {
				fo.OldTerm = s.Term
			}
		}
		for _, s := range states {
			if s.At.Before(cut) || s.Node == node || s.Role != "leader" || s.Term <= fo.OldTerm {
				continue
			}
			if !healed.IsZero() && s.At.After(healed) {
				break
			}
			fo.NewLeader, fo.NewTerm = s.Node, s.Term
			fo.Ms = float64(s.At.Sub(cut).Microseconds()) / 1000
			break
		}
		if fo.NewLeader == 0 {
			fo.Note = "no new leader before the heal"
		}
		out = append(out, fo)
	}
	return out
}

// window finds the k-th disconnect of node, and the first reconnect after it.
func window(events []netEvent, node, k int) (cut, healed time.Time, ok bool) {
	n := 0
	for i, e := range events {
		if e.Node != node || e.Action != "disconnect" {
			continue
		}
		if n++; n < k {
			continue
		}
		for _, later := range events[i+1:] {
			if later.Node == node && later.Action == "connect" {
				return e.At, later.At, true
			}
		}
		return e.At, time.Time{}, true
	}
	return time.Time{}, time.Time{}, false
}
