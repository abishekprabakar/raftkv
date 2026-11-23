package kube

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/abishekprabakar/raftkv/internal/pb"
)

const (
	// probeEvery is the pause between one Status answer and the next request to
	// the same node. With the request itself, a node is asked about every 6 ms.
	probeEvery = 5 * time.Millisecond
	// probeTimeout bounds one request. Each node has its own goroutine, so a
	// node that hangs never delays the others.
	probeTimeout = 200 * time.Millisecond
	// fresh is how old a node's last answer may be and still count when asking
	// whether the nodes agree.
	fresh = 250 * time.Millisecond
)

// Sample is one node's answer to Status, or the lack of one.
type Sample struct {
	At      time.Duration // when the answer arrived, since the run started
	OK      bool
	Role    string
	Term    uint64
	Leader  int
	Applied uint64
}

// LeaderEvent is the first time a node was seen leading in a term.
type LeaderEvent struct {
	At   time.Duration `json:"-"`
	AtMs float64       `json:"at_ms"`
	Pod  string        `json:"pod"`
	ID   int           `json:"id"`
	Term uint64        `json:"term"`
}

// outage is one stretch in which a node gave no answer.
type outage struct {
	lastAnswer time.Duration // the last answer before it
	firstMiss  time.Duration // the first request that failed
}

// Prober asks every node for its Status, over and over, for as long as it runs.
// It is how the harness sees a leader appear: nothing is read from logs, so the
// time of an election is on the same clock as everything else.
type Prober struct {
	t     *Target
	since func() time.Duration

	mu        sync.Mutex
	latest    []Sample
	leaders   []LeaderEvent
	seen      map[[2]uint64]bool // (node, term) pairs already in leaders
	outages   [][]outage
	intervals []float32 // ms between successive answers from one node

	stop chan struct{}
	wg   sync.WaitGroup
}

// StartProber starts polling. since is the run's clock.
func StartProber(t *Target, since func() time.Duration) *Prober {
	p := &Prober{
		t: t, since: since, latest: make([]Sample, len(t.Nodes)), seen: map[[2]uint64]bool{},
		outages: make([][]outage, len(t.Nodes)), stop: make(chan struct{}),
	}
	for i := range t.Nodes {
		p.wg.Go(func() { p.poll(i) })
	}
	return p
}

// Stop ends the polling.
func (p *Prober) Stop() {
	close(p.stop)
	p.wg.Wait()
}

func (p *Prober) poll(i int) {
	node := p.t.Nodes[i]
	// Reconnect quickly: a pod that comes back should be seen at once, not
	// after gRPC's usual backoff.
	conn, err := grpc.NewClient("passthrough:///"+node.HostAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 20 * time.Millisecond, Multiplier: 1.5, Jitter: 0.1, MaxDelay: 100 * time.Millisecond},
			MinConnectTimeout: probeTimeout,
		}))
	if err != nil {
		return
	}
	defer conn.Close()
	kv := pb.NewKVClient(conn)
	var last Sample
	for {
		ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		st, err := kv.Status(ctx, &pb.StatusRequest{})
		cancel()
		s := Sample{At: p.since()}
		if err == nil {
			s.OK, s.Role, s.Term, s.Leader, s.Applied = true, st.Role, st.Term, int(st.LeaderId), st.LastApplied
		}
		p.record(i, node, last, s)
		last = s
		select {
		case <-p.stop:
			return
		case <-time.After(probeEvery):
		}
	}
}

func (p *Prober) record(i int, node Node, last, s Sample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.latest[i] = s
	switch {
	case s.OK && last.OK:
		p.intervals = append(p.intervals, float32(s.At-last.At)/float32(time.Millisecond))
	case !s.OK && last.OK:
		p.outages[i] = append(p.outages[i], outage{lastAnswer: last.At, firstMiss: s.At})
	}
	if s.OK && s.Role == "leader" && !p.seen[[2]uint64{uint64(node.ID), s.Term}] {
		p.seen[[2]uint64{uint64(node.ID), s.Term}] = true
		p.leaders = append(p.leaders, LeaderEvent{At: s.At, AtMs: ms(s.At), Pod: node.Pod, ID: node.ID, Term: s.Term})
	}
}

// Agreed reports whether every node has answered within the last 250 ms and all
// name the same leader in the same term, and that leader says so itself.
func (p *Prober) Agreed() (leader Node, term uint64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.since()
	first := p.latest[0]
	for i, s := range p.latest {
		if !s.OK || now-s.At > fresh || s.Leader == 0 || s.Leader != first.Leader || s.Term != first.Term {
			return Node{}, 0, false
		}
		if p.t.Nodes[i].ID == s.Leader {
			if s.Role != "leader" {
				return Node{}, 0, false
			}
			leader = p.t.Nodes[i]
		}
	}
	return leader, first.Term, leader.ID != 0
}

// LeaderAbove returns the first time, at or after from, that any node was seen
// leading in a term above term.
func (p *Prober) LeaderAbove(term uint64, from time.Duration) (LeaderEvent, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.leaders {
		if e.Term > term && e.At >= from {
			return e, true
		}
	}
	return LeaderEvent{}, false
}

// LeadersBetween returns the leaders first seen after from and up to to.
func (p *Prober) LeadersBetween(from, to time.Duration) []LeaderEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []LeaderEvent
	for _, e := range p.leaders {
		if e.At > from && e.At <= to {
			out = append(out, e)
		}
	}
	return out
}

// LastAnswerBeforeOutage returns when the node in pod last answered before the
// first stretch of silence that began at or after from.
func (p *Prober) LastAnswerBeforeOutage(pod string, from time.Duration) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i, n := range p.t.Nodes {
		if n.Pod != pod {
			continue
		}
		for _, o := range p.outages[i] {
			if o.firstMiss >= from {
				return o.lastAnswer, true
			}
		}
	}
	return 0, false
}

// Resolution describes how often a node was actually asked: the gaps between
// one answer and the next. A time read off the prober is that coarse.
func (p *Prober) Resolution() Dist {
	p.mu.Lock()
	defer p.mu.Unlock()
	values := make([]float64, len(p.intervals))
	for i, v := range p.intervals {
		values[i] = float64(v)
	}
	return Summarize(values)
}
