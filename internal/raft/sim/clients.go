package sim

import (
	"errors"
	"fmt"

	"github.com/abishekprabakar/raftkv/internal/kv"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// client is one simulated client. It runs one operation at a time, and it behaves
// the way a real client must: it can't tell a lost request from a lost reply, so
// after a timeout it retries the same write under the same Seq, on another node.
type client struct {
	id     uint64
	seq    uint64
	target int // index of the host it will try next
	op     *clientOp
}

type clientOp struct {
	read    bool
	key     string
	token   string
	seq     uint64
	attempt int

	// What a read must see, from the moment it started.
	mustSeeAcked  int
	mustSeeTokens int
}

// sentinelClient is the client ID behind the quiet phase's final write.
const sentinelClient = ^uint64(0)

func (s *Sim) startClients() {
	for i := 0; i < s.sched.Clients; i++ {
		c := &client{id: uint64(i + 1), target: s.rnd.IntN(len(s.hosts))}
		s.clients = append(s.clients, c)
		s.schedule(between(s.rnd, 0, s.sched.ThinkTime), event{kind: evClientNext, client: c})
	}
}

// clientNext starts a client's next operation.
func (s *Sim) clientNext(c *client) {
	if s.quiet {
		return
	}
	key := fmt.Sprintf("k%d", s.rnd.IntN(s.sched.Keys))
	op := &clientOp{key: key}
	if s.rnd.Float64() < s.sched.ReadRate {
		op.read = true
		op.mustSeeAcked, op.mustSeeTokens = s.model.readStart(key)
	} else {
		c.seq++
		op.seq = c.seq
		op.token = fmt.Sprintf("c%d-%d", c.id, c.seq)
		s.model.issued[op.token] = true
	}
	c.op = op
	s.clientSend(c)
}

// clientSend sends the current operation to the client's current target. Clients
// reach every live node: a partition cuts servers off from each other, so a
// deposed leader stays reachable, which is the dangerous case for reads.
func (s *Sim) clientSend(c *client) {
	op := c.op
	op.attempt++
	s.schedule(s.sched.ClientTimeout, event{kind: evClientTimeout, client: c, op: op, attempt: op.attempt})
	if s.chance(s.sched.DropRate) {
		return
	}
	delay := between(s.rnd, s.sched.MinDelay, s.sched.MaxDelay)
	s.schedule(delay, event{kind: evClientRequest, client: c, op: op, host: s.hosts[c.target]})
}

// clientRequest is a request arriving at a server.
func (s *Sim) clientRequest(e *event) {
	h, c, op := e.host, e.client, e.op
	if h.server == nil {
		return
	}
	s.tracef("client %d asks node %d: %+v", c.id, h.id, *op)
	if op.read {
		s.call(h, func() error {
			return h.server.Read([]byte(op.key), func(value []byte, _ bool, err error) {
				s.clientReply(h, c, op, value, err)
			})
		})
		return
	}
	cmd := kv.Command{ClientID: c.id, Seq: op.seq, Kind: kv.Append, Key: []byte(op.key), Value: []byte(op.token + tokenSeparator)}
	s.stats.Proposed++
	leading := h.status.Role == raft.Leader
	s.call(h, func() error {
		return h.server.Submit(cmd, func(err error) { s.clientReply(h, c, op, nil, err) })
	})
	if leading && s.sched.LeaderHunt > 0 {
		s.schedule(between(s.rnd, 1, s.sched.LeaderHunt), event{kind: evCrashNode, host: h})
	}
}

// clientReply sends a server's answer back across the network.
func (s *Sim) clientReply(h *host, c *client, op *clientOp, value []byte, err error) {
	if s.chance(s.sched.DropRate) {
		return
	}
	s.schedule(between(s.rnd, s.sched.MinDelay, s.sched.MaxDelay), event{
		kind: evClientResponse, client: c, op: op, from: h.id, hint: h.status.Leader, value: value, err: err,
	})
}

// clientResponse is an answer arriving at a client. An answer to an earlier try of
// the current operation still counts, as it would on a real connection.
func (s *Sim) clientResponse(e *event) {
	c, op := e.client, e.op
	if s.quiet || c.op != op {
		return
	}
	if e.err != nil {
		if op.read {
			s.stats.ReadsRefused++
		}
		if errors.Is(e.err, kv.ErrNotLeader) && e.hint != raft.None {
			c.target = int(e.hint) - 1
		} else {
			c.target = (c.target + 1) % len(s.hosts)
		}
		s.schedule(between(s.rnd, 1, s.sched.ThinkTime+10*Millisecond), event{kind: evClientRetry, client: c, op: op})
		return
	}
	if op.read {
		s.stats.ReadsServed++
		s.model.readDone(e.from, op.key, e.value, op.mustSeeAcked, op.mustSeeTokens)
	} else {
		s.stats.WritesAcked++
		s.model.ack(op.key, op.token)
	}
	c.op = nil
	s.schedule(between(s.rnd, 0, s.sched.ThinkTime), event{kind: evClientNext, client: c})
}

func (s *Sim) clientTimeout(e *event) {
	c, op := e.client, e.op
	if s.quiet || c.op != op || op.attempt != e.attempt {
		return
	}
	c.target = (c.target + 1) % len(s.hosts)
	s.clientSend(c)
}
