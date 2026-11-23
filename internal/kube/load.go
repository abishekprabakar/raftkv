package kube

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/abishekprabakar/raftkv/internal/chaos"
	"github.com/abishekprabakar/raftkv/internal/client"
)

// The shape of the client load, fixed in docs/acceptance/k8s.md.
const (
	Clients      = 6
	KeysPerEpoch = 4
	EpochLength  = 2 * time.Second
	maxThink     = 40 * time.Millisecond
	getShare     = 0.4
)

// Op is one client operation. It is M5's record, which the Porcupine model
// reads, plus what the measurements here need.
type Op struct {
	chaos.Op
	// Term is the term of the node that acknowledged a write.
	Term uint64 `json:"term,omitempty"`
	// Retries counts the attempts inside this operation that failed before the
	// one that settled it.
	Retries int `json:"retries,omitempty"`
	// Final marks the reads made after the load stopped, one per key.
	Final bool `json:"final,omitempty"`
}

// ack is one acknowledged write, kept so that a trial can ask for the first
// write a new leader acknowledged.
type ack struct {
	at   time.Duration
	term uint64
}

// Load is the clients, running.
type Load struct {
	t      *Target
	prefix string
	since  func() time.Duration
	start  time.Duration

	mu        sync.Mutex
	histories [][]Op
	acks      []ack

	stop chan struct{}
	wg   sync.WaitGroup
}

// StartLoad starts the clients. Each gets only the client Service's address and
// finds the leader from there. prefix goes in front of every key, so that two
// runs on one cluster never write the same key: the model assumes every key
// starts empty.
func StartLoad(t *Target, prefix string, seed uint64, since func() time.Duration) (*Load, error) {
	l := &Load{t: t, prefix: prefix, since: since, start: since(), histories: make([][]Op, Clients), stop: make(chan struct{})}
	for i := range Clients {
		c, err := l.newClient()
		if err != nil {
			close(l.stop)
			l.wg.Wait()
			return nil, err
		}
		l.wg.Go(func() {
			defer c.Close()
			l.run(i, c, rand.New(rand.NewPCG(seed, uint64(i+1))))
		})
	}
	return l, nil
}

func (l *Load) newClient() (*client.Client, error) {
	return client.New([]string{l.t.Client}, client.Options{AttemptTimeout: chaos.AttemptTimeout, Dial: l.t.Dial})
}

// run is one client: one operation at a time until the load stops.
func (l *Load) run(i int, c *client.Client, rng *rand.Rand) {
	appends := 0
	for {
		select {
		case <-l.stop:
			return
		case <-time.After(time.Duration(rng.Int64N(int64(maxThink) + 1))):
		}
		// New keys every epoch keep values short, however long the run lasts.
		epoch := int((l.since() - l.start) / EpochLength)
		in := chaos.Input{Kind: chaos.Append, Key: fmt.Sprintf("%s.e%d.k%d", l.prefix, epoch, rng.IntN(KeysPerEpoch))}
		if rng.Float64() < getShare {
			in.Kind = chaos.Get
		} else {
			appends++
			in.Value = fmt.Sprintf("c%d.%d;", i, appends) // unique, so a lost or repeated append shows
		}
		op := l.do(i, c, in)
		l.mu.Lock()
		l.histories[i] = append(l.histories[i], op)
		if op.Kind == chaos.Append && op.OK {
			l.acks = append(l.acks, ack{time.Duration(op.Return), op.Term})
		}
		l.mu.Unlock()
	}
}

// do runs one operation and records it. Call and Return come from the run's one
// clock.
func (l *Load) do(i int, c *client.Client, in chaos.Input) Op {
	ctx, cancel := context.WithTimeout(context.Background(), chaos.OpTimeout)
	defer cancel()
	op := Op{Op: chaos.Op{Client: i, Kind: in.Kind, Key: in.Key, Value: in.Value}}
	retries := c.Retries
	op.Call = l.since().Nanoseconds()
	var err error
	if in.Kind == chaos.Get {
		var v []byte
		v, _, err = c.Get(ctx, []byte(in.Key))
		op.Value = string(v)
	} else {
		err = c.Append(ctx, []byte(in.Key), []byte(in.Value))
		op.Term = c.Term
	}
	op.Return = l.since().Nanoseconds()
	op.Retries = c.Retries - retries
	if err != nil {
		op.OK, op.Err, op.Term = false, err.Error(), 0
		if in.Kind == chaos.Get {
			op.Value = ""
		}
		return op
	}
	op.OK = true
	return op
}

// FirstAckAbove returns when the first write was acknowledged, at or after from,
// by a node in a term above term.
func (l *Load) FirstAckAbove(term uint64, from time.Duration) (time.Duration, uint64, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	best, bestTerm, found := time.Duration(0), uint64(0), false
	for _, a := range l.acks {
		if a.term > term && a.at >= from && (!found || a.at < best) {
			best, bestTerm, found = a.at, a.term, true
		}
	}
	return best, bestTerm, found
}

// LongestWriteGap returns the longest stretch with no write acknowledged that
// ended between from and to. The stretch may have begun before from.
func (l *Load) LongestWriteGap(from, to time.Duration) time.Duration {
	l.mu.Lock()
	times := make([]time.Duration, 0, len(l.acks))
	for _, a := range l.acks {
		times = append(times, a.at)
	}
	l.mu.Unlock()
	slices.Sort(times)
	var longest time.Duration
	for i := 1; i < len(times); i++ {
		if times[i] >= from && times[i] <= to {
			longest = max(longest, times[i]-times[i-1])
		}
	}
	return longest
}

// Window counts what the clients did among operations called between from and to.
func (l *Load) Window(from, to time.Duration) LoadStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	var ops []Op
	for _, h := range l.histories {
		for _, op := range h {
			if at := time.Duration(op.Call); at >= from && at <= to {
				ops = append(ops, op)
			}
		}
	}
	return Stats(ops)
}

// Finish stops the clients, waits for the operations in flight, and then reads
// every key the run used, once, with a client of its own. It returns the whole
// history in call order.
func (l *Load) Finish() ([]Op, error) {
	close(l.stop)
	l.wg.Wait()
	var history []Op
	keys := map[string]bool{}
	for _, h := range l.histories {
		for _, op := range h {
			keys[op.Key] = true
		}
		history = append(history, h...)
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	slices.Sort(sorted)

	c, err := l.newClient()
	if err != nil {
		return nil, err
	}
	defer c.Close()
	for _, key := range sorted {
		op := l.do(Clients, c, chaos.Input{Kind: chaos.Get, Key: key})
		op.Final = true
		history = append(history, op)
	}
	slices.SortStableFunc(history, func(a, b Op) int {
		switch {
		case a.Call < b.Call:
			return -1
		case a.Call > b.Call:
			return 1
		}
		return 0
	})
	return history, nil
}

// LoadStats counts what the clients did.
type LoadStats struct {
	Requests      int `json:"requests"`
	WritesAcked   int `json:"writes_acknowledged"`
	WritesUnknown int `json:"writes_unknown"`
	ReadsServed   int `json:"reads_served"`
	ReadsFailed   int `json:"reads_failed"`
	// Failed is the requests the client library gave up on: WritesUnknown plus
	// ReadsFailed.
	Failed int `json:"failed_requests"`
	// Retries is the failed attempts inside requests, whether or not the request
	// succeeded in the end.
	Retries             int  `json:"retries"`
	RequestsWithRetries int  `json:"requests_that_retried"`
	LatencyMs           Dist `json:"latency_ms"`
}

// Stats counts a set of operations. Final reads are left out: they are the
// check, not the load.
func Stats(ops []Op) LoadStats {
	var s LoadStats
	var latencies []float64
	for _, op := range ops {
		if op.Final {
			continue
		}
		s.Requests++
		s.Retries += op.Retries
		if op.Retries > 0 {
			s.RequestsWithRetries++
		}
		switch {
		case op.Kind == chaos.Get && op.OK:
			s.ReadsServed++
		case op.Kind == chaos.Get:
			s.ReadsFailed++
		case op.OK:
			s.WritesAcked++
		default:
			s.WritesUnknown++
		}
		latencies = append(latencies, float64(op.Return-op.Call)/1e6)
	}
	s.Failed = s.WritesUnknown + s.ReadsFailed
	s.LatencyMs = Summarize(latencies)
	return s
}
