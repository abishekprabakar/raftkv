package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abishekprabakar/raftkv/internal/pb"
)

// countingListener counts the TCP connections it accepts.
type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

// wrongNode answers every stream the way a node does when a message was meant for
// someone else.
type wrongNode struct{ pb.UnimplementedRaftServer }

func (wrongNode) Stream(stream pb.Raft_StreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return status.Error(codes.FailedPrecondition, "meant for another node")
}

// Regression test for the M5 bug. When two containers swapped addresses, a node's
// connection to "node2" reached node3, which refused every stream. The connection
// itself was healthy, so gRPC kept reusing it, and the node reopened streams on it
// every 50 ms. Now a refused stream makes the peer drop the connection and dial
// again, so it can find wherever the name points now.
func TestPeerRedialsWhenItReachesTheWrongNode(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis := &countingListener{Listener: inner}
	srv := grpc.NewServer()
	pb.RegisterRaftServer(srv, wrongNode{})
	go srv.Serve(lis)
	defer srv.Stop()

	p, err := newPeer(2, lis.Addr().String(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	defer func() { cancel(); <-done; p.connection().Close() }()

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && lis.accepted.Load() < 3; time.Sleep(20 * time.Millisecond) {
		p.send(&pb.Envelope{From: 1, To: 2, Message: &pb.Envelope_RequestVoteReply{RequestVoteReply: &pb.RequestVoteReply{Term: 1}}})
	}
	if n := lis.accepted.Load(); n < 3 {
		t.Fatalf("%d connections: the peer kept reusing a connection to the wrong node", n)
	}
}

// recordingNode accepts streams and keeps what arrives on them.
type recordingNode struct {
	pb.UnimplementedRaftServer
	streams atomic.Int64
	got     chan *pb.Envelope
}

func (n *recordingNode) Stream(stream pb.Raft_StreamServer) error {
	n.streams.Add(1)
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.StreamClosed{})
		}
		if err != nil {
			return err
		}
		n.got <- env
	}
}

func serveRecording(t *testing.T, addr string) (*recordingNode, *grpc.Server, string) {
	t.Helper()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	node := &recordingNode{got: make(chan *pb.Envelope, 16)}
	srv := grpc.NewServer()
	pb.RegisterRaftServer(srv, node)
	go srv.Serve(lis)
	return node, srv, lis.Addr().String()
}

// Regression test for a bug the Kubernetes failover runs found. A follower sends
// nothing to another follower between elections, so its stream to that node sits
// idle. If the other node restarted in the meantime, the stream was dead, and
// nothing noticed until the next message was written to it: the RequestVote of the
// next election, or the reply to one. That message was lost, the election failed,
// and failover took a second election timeout. Now a stream that dies while idle
// is noticed at once and reopened, so it is ready before anything needs it.
func TestPeerReopensAStreamThatDiedWhileIdle(t *testing.T) {
	first, srv, addr := serveRecording(t, "127.0.0.1:0")
	p, err := newPeer(2, addr, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.run(ctx); close(done) }()
	defer func() { cancel(); <-done; p.connection().Close() }()

	waitFor := func(what string, cond func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if cond() {
				return
			}
		}
		t.Fatalf("timed out waiting until %s", what)
	}
	waitFor("the peer opens its stream", func() bool { return first.streams.Load() == 1 })

	// The node restarts: same address, new process. Nothing is sent meanwhile.
	srv.Stop()
	second, srv2, _ := serveRecording(t, addr)
	defer srv2.Stop()
	waitFor("the peer reopens its stream to the restarted node, with nothing to send", func() bool {
		return second.streams.Load() >= 1
	})

	// The first message after the restart arrives. Before the fix it was the
	// message that discovered the dead stream, and was lost with it.
	p.send(&pb.Envelope{From: 1, To: 2, Message: &pb.Envelope_RequestVote{RequestVote: &pb.RequestVoteArgs{Term: 9, CandidateId: 1}}})
	select {
	case env := <-second.got:
		if env.GetRequestVote().GetTerm() != 9 {
			t.Errorf("got %v", env)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the first message after the restart never arrived")
	}
}
