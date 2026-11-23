package server

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/abishekprabakar/raftkv/internal/pb"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// peerQueue is how many messages may wait for one peer. Raft only needs the
// newest few: a heartbeat or a retry replaces anything lost, so when a peer is slow
// or down, dropping beats letting memory grow without bound.
const peerQueue = 1024

// maxMessageBytes bounds one gRPC message in either direction. The default is 4
// MiB, and one AppendEntries can reach 16 MiB (64 entries of up to 256 KiB). A
// snapshot is sent whole, so a store larger than this can't be sent to a follower
// that needs one: a known limit, since snapshots aren't split into chunks.
const maxMessageBytes = 256 << 20

// dialOptions are shared by node-to-node and client connections.
func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageBytes), grpc.MaxCallRecvMsgSize(maxMessageBytes)),
		// gRPC's default reconnect backoff grows to two minutes. A restarted node
		// would sit unreachable for that long, so cap it at a second.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: time.Second,
		}),
		// A peer killed or cut off never closes its TCP connection, so without help
		// a sender could write into a dead connection for minutes. Two things find
		// it. An idle connection is pinged every 10 s, which is the least gRPC
		// allows: it silently raises anything lower to 10 s. And on Linux, gRPC
		// turns the 2 s Timeout into TCP_USER_TIMEOUT, so a connection whose data
		// goes unacknowledged for 2 s is closed, however recently it was pinged.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 2 * time.Second, PermitWithoutStream: true}),
	}
}

// peer sends one node's messages to one other node, on a single stream it keeps
// open. It runs on its own goroutine so that a slow or dead peer never holds up
// the Raft loop, which only drops a message into the queue.
type peer struct {
	id      raft.NodeID
	addr    string
	queue   chan *pb.Envelope
	log     *slog.Logger
	dropped atomic.Uint64

	// conn is replaced when run finds it connected to the wrong node, and a
	// health check may be reading it at that moment, so it has a lock.
	mu   sync.Mutex
	conn *grpc.ClientConn
}

// connection returns the connection to the peer as it is now.
func (p *peer) connection() *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conn
}

func newPeer(id raft.NodeID, addr string, log *slog.Logger) (*peer, error) {
	p := &peer{id: id, addr: addr, queue: make(chan *pb.Envelope, peerQueue), log: log}
	return p, p.dial()
}

// target turns a host:port into a gRPC target that looks the host up again on
// every connection attempt. gRPC's default resolver caches the answer and asks
// again at most every 30 s. Containers get new addresses when they restart or
// reconnect, and two can even swap: M5 saw node2 come back on node3's old address.
// With a cached answer, a node would keep dialing the wrong place for up to 30 s.
func target(addr string) string { return "passthrough:///" + addr }

func (p *peer) dial() error {
	conn, err := grpc.NewClient(target(p.addr), dialOptions()...)
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()
	return err
}

// send queues a message, or drops it if the queue is full. It never blocks.
func (p *peer) send(env *pb.Envelope) {
	select {
	case p.queue <- env:
	default:
		p.dropped.Add(1)
	}
}

// run keeps a stream open to the peer and feeds it from the queue until ctx ends.
// When the stream breaks, the message being sent is lost and the next stream
// carries on from the queue. Raft never assumes a message arrives.
func (p *peer) run(ctx context.Context) {
	client := pb.NewRaftClient(p.connection())
	pause := 50 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := client.Stream(ctx)
		if err == nil {
			// Opening and losing a stream are logged, so a connection problem can
			// be lined up with elections in the log. Failed attempts are not:
			// while a peer is down there is one a second.
			p.log.Info("stream to peer open", "peer", p.id)
			pause = 50 * time.Millisecond
			err = p.pump(ctx, stream)
			if ctx.Err() == nil {
				p.log.Info("stream to peer lost", "peer", p.id, "err", err)
			}
			if status.Code(err) == codes.FailedPrecondition {
				// The connection reached a node that isn't this peer, because the
				// address now belongs to someone else. The connection itself is
				// healthy, so gRPC would keep reusing it. Drop it, and dial again,
				// which looks the name up afresh.
				p.connection().Close()
				if err := p.dial(); err != nil {
					p.log.Error("redialing peer", "peer", p.id, "err", err)
					return
				}
				client = pb.NewRaftClient(p.connection())
			}
		}
		if ctx.Err() != nil {
			return
		}
		p.log.Debug("could not open a stream to peer", "peer", p.id, "addr", p.addr, "err", err)
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return
		}
		pause = min(2*pause, time.Second)
	}
}

// pump sends queued messages on the stream until the stream ends, and says why
// it ended.
//
// It has to notice a stream that dies while nothing is being sent. A follower
// sends nothing to another follower between elections, so that stream is idle
// for as long as the leader is healthy. If the other node restarts meanwhile,
// the stream is dead, and Send is the only call that would find out: the first
// message written to it, which is the RequestVote of the next election or the
// reply to one, would be lost with it. The Kubernetes failover runs caught
// exactly that: most elections failed once and had to be run again.
func (p *peer) pump(ctx context.Context, stream pb.Raft_StreamClient) error {
	// The peer answers a stream only once, when it ends, so this receive blocks
	// for as long as the stream is alive and returns the reason when it isn't.
	ended := make(chan error, 1)
	go func() {
		err := stream.RecvMsg(new(pb.StreamClosed))
		if err == nil {
			err = io.EOF // the peer closed the stream without an error
		}
		ended <- err
	}()
	for {
		select {
		case env := <-p.queue:
			if err := stream.Send(env); err != nil {
				// Send only says the stream ended. The receive has the reason,
				// and gets it at once, since the stream is over.
				select {
				case why := <-ended:
					return why
				case <-time.After(time.Second):
					return err
				}
			}
		case err := <-ended:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
