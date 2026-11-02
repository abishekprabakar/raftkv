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
	defer func() { cancel(); <-done; p.conn.Close() }()

	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && lis.accepted.Load() < 3; time.Sleep(20 * time.Millisecond) {
		p.send(&pb.Envelope{From: 1, To: 2, Message: &pb.Envelope_RequestVoteReply{RequestVoteReply: &pb.RequestVoteReply{Term: 1}}})
	}
	if n := lis.accepted.Load(); n < 3 {
		t.Fatalf("%d connections: the peer kept reusing a connection to the wrong node", n)
	}
}
