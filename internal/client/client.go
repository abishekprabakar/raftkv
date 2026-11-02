// Package client is the raftkv client library. Give it the address of any node,
// or several: it finds the leader by following the redirects nodes send back, and
// keeps trying until a request succeeds or its context runs out.
//
// A write is retried under the same client ID and sequence number every time, so
// however many times it reaches the cluster, it applies at most once. That is what
// makes retrying a write safe when the client can't tell whether it happened.
package client

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/abishekprabakar/raftkv/internal/pb"
)

// ErrGaveUp means the context ran out before any node completed the request. For
// a write, the write may or may not have happened.
var ErrGaveUp = errors.New("client: gave up")

// Options tune a Client. Zero values select the defaults listed.
type Options struct {
	// AttemptTimeout bounds one try at one node. A leader cut off from the others
	// can hold a request without ever answering it, so the client has to move on
	// to another node. Default 2 s.
	AttemptTimeout time.Duration
	// Log, if set, hears about every redirect and every failed attempt.
	Log func(format string, args ...any)
}

// Client talks to one cluster. It is not safe for concurrent use: writes are
// numbered in order, and the cluster's session for this client only remembers the
// highest number, so one write must finish before the next starts.
type Client struct {
	opts  Options
	addrs []string // every address known: the ones given, and any learned from redirects
	next  int      // index into addrs of the node to try first
	conns map[string]*grpc.ClientConn
	id    uint64
	seq   uint64

	// Redirects counts the redirects this client has followed.
	Redirects int
}

// New returns a client that starts with these addresses. It doesn't connect until
// the first request.
func New(addrs []string, opts Options) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 2 * time.Second
	}
	c := &Client{opts: opts, addrs: slices.Clone(addrs), conns: map[string]*grpc.ClientConn{}}
	// A random 64-bit ID. Two clients sharing an ID would have each other's writes
	// dropped as repeats, and at this size that is vanishingly unlikely.
	for c.id == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		c.id = binary.LittleEndian.Uint64(b[:])
	}
	return c, nil
}

// Close closes every connection.
func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (c *Client) logf(format string, args ...any) {
	if c.opts.Log != nil {
		c.opts.Log(format, args...)
	}
}

func (c *Client) conn(addr string) (*grpc.ClientConn, error) {
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}
	// "passthrough" looks the name up on every connection attempt rather than
	// caching it for 30 s, so a node that comes back on a new address is found at
	// once. See target in internal/server/peer.go for the longer story.
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}),
		// As between nodes: ping an idle connection every 10 s, and on Linux close
		// one whose data goes unacknowledged for 2 s.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 2 * time.Second, PermitWithoutStream: true}),
	)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

// forget closes the connection to addr, so the next request dials a new one.
func (c *Client) forget(addr string) {
	if conn, ok := c.conns[addr]; ok {
		conn.Close()
		delete(c.conns, addr)
	}
}

// learn returns addr's index in the address list, adding it if it's new.
func (c *Client) learn(addr string) int {
	if i := slices.Index(c.addrs, addr); i >= 0 {
		return i
	}
	c.addrs = append(c.addrs, addr)
	return len(c.addrs) - 1
}

// attempt is one try at one node. It returns the node's redirect, if it sent one.
type attempt func(ctx context.Context, kv pb.KVClient) (*pb.NotLeader, error)

// call runs try against the cluster until a node completes it.
//
// A redirect to a new address is followed at once. Anything else (a node that
// doesn't know the leader, can't be reached, or answers "unavailable") moves on to
// the next known address after a pause that doubles each time, so a cluster in the
// middle of an election isn't flooded.
func (c *Client) call(ctx context.Context, try attempt) error {
	pause := 20 * time.Millisecond
	hops := 0 // redirects followed since the last pause
	for {
		addr := c.addrs[c.next]
		var notLeader *pb.NotLeader
		conn, err := c.conn(addr)
		if err == nil {
			actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
			notLeader, err = try(actx, pb.NewKVClient(conn))
			cancel()
		}
		if err == nil && notLeader == nil {
			return nil
		}
		if status.Code(err) == codes.InvalidArgument {
			return err // the request itself is wrong; no node will take it
		}

		var why error
		switch {
		case err != nil:
			why = fmt.Errorf("%s: %w", addr, err)
			c.next = (c.next + 1) % len(c.addrs)
			if status.Code(err) == codes.DeadlineExceeded && ctx.Err() == nil {
				// The node didn't answer within the attempt. Its connection may be
				// dead without anyone having noticed, since a node that is cut off
				// never closes its connections, and gRPC would keep sending on it.
				// M5 caught a client failing every request this way long after the
				// network healed. Start a fresh connection next time.
				c.forget(addr)
			}
		case notLeader.LeaderAddress == "" || notLeader.LeaderAddress == addr:
			why = fmt.Errorf("%s: not the leader, and doesn't know who is", addr)
			c.next = (c.next + 1) % len(c.addrs)
		default:
			c.Redirects++
			c.next = c.learn(notLeader.LeaderAddress)
			c.logf("%s is not the leader: redirected to node %d at %s", addr, notLeader.LeaderId, notLeader.LeaderAddress)
			// Follow a fresh hint at once. If hints go round in a circle, nodes
			// disagree about the leader, which settles within an election timeout.
			if hops++; hops <= 2 {
				continue
			}
			why = errors.New("redirects went round in a circle")
		}
		c.logf("%v; retrying in %v", why, pause)
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return fmt.Errorf("%w: %v (last error: %v)", ErrGaveUp, ctx.Err(), why)
		}
		pause = min(2*pause, 500*time.Millisecond)
		hops = 0
	}
}

func (c *Client) write(ctx context.Context, op pb.Op, key, value []byte) error {
	c.seq++
	req := &pb.WriteRequest{ClientId: c.id, Seq: c.seq, Op: op, Key: key, Value: value}
	return c.call(ctx, func(ctx context.Context, kv pb.KVClient) (*pb.NotLeader, error) {
		resp, err := kv.Write(ctx, req)
		if err != nil {
			return nil, err
		}
		return resp.NotLeader, nil
	})
}

// Put sets key to value.
func (c *Client) Put(ctx context.Context, key, value []byte) error {
	return c.write(ctx, pb.Op_OP_PUT, key, value)
}

// Append adds value to the end of key's current value.
func (c *Client) Append(ctx context.Context, key, value []byte) error {
	return c.write(ctx, pb.Op_OP_APPEND, key, value)
}

// Delete removes key.
func (c *Client) Delete(ctx context.Context, key []byte) error {
	return c.write(ctx, pb.Op_OP_DELETE, key, nil)
}

// Get reads key. The read is linearizable: it sees every write that was
// acknowledged before Get was called.
func (c *Client) Get(ctx context.Context, key []byte) (value []byte, found bool, err error) {
	err = c.call(ctx, func(ctx context.Context, kv pb.KVClient) (*pb.NotLeader, error) {
		resp, err := kv.Get(ctx, &pb.GetRequest{Key: key})
		if err != nil {
			return nil, err
		}
		value, found = resp.Value, resp.Found
		return resp.NotLeader, nil
	})
	return value, found, err
}

// Status asks one node for its view of the cluster. It is never redirected.
func (c *Client) Status(ctx context.Context, addr string) (*pb.StatusResponse, error) {
	conn, err := c.conn(addr)
	if err != nil {
		return nil, err
	}
	return pb.NewKVClient(conn).Status(ctx, &pb.StatusRequest{})
}
