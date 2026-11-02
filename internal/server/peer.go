package server

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

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
		// A peer killed outright never closes its TCP connection, and without pings
		// a sender could wait on it for minutes. The server allows pings this often.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 2 * time.Second, Timeout: 2 * time.Second, PermitWithoutStream: true}),
	}
}

// peer sends one node's messages to one other node, on a single stream it keeps
// open. It runs on its own goroutine so that a slow or dead peer never holds up
// the Raft loop, which only drops a message into the queue.
type peer struct {
	id      raft.NodeID
	addr    string
	queue   chan *pb.Envelope
	conn    *grpc.ClientConn
	log     *slog.Logger
	dropped atomic.Uint64
}

func newPeer(id raft.NodeID, addr string, log *slog.Logger) (*peer, error) {
	conn, err := grpc.NewClient(addr, dialOptions()...)
	if err != nil {
		return nil, err
	}
	return &peer{id: id, addr: addr, queue: make(chan *pb.Envelope, peerQueue), conn: conn, log: log}, nil
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
	client := pb.NewRaftClient(p.conn)
	pause := 50 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := client.Stream(ctx)
		if err == nil {
			pause = 50 * time.Millisecond
			err = p.pump(ctx, stream)
		}
		if ctx.Err() != nil {
			return
		}
		p.log.Debug("stream to peer ended", "peer", p.id, "addr", p.addr, "err", err)
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return
		}
		pause = min(2*pause, time.Second)
	}
}

func (p *peer) pump(ctx context.Context, stream pb.Raft_StreamClient) error {
	for {
		select {
		case env := <-p.queue:
			if err := stream.Send(env); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
