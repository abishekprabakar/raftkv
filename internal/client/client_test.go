package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/abishekprabakar/raftkv/internal/pb"
)

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

// silentNode never answers a write, like a node behind a connection that died
// without closing.
type silentNode struct{ pb.UnimplementedKVServer }

func (silentNode) Write(ctx context.Context, _ *pb.WriteRequest) (*pb.WriteResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// Regression test for the M5 bug. A client whose connection had gone dead during a
// partition kept sending on it after the network healed, and every request timed
// out. Now an attempt that times out drops its connection, so the next one dials
// fresh.
func TestClientRedialsAfterATimeout(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis := &countingListener{Listener: inner}
	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, silentNode{})
	go srv.Serve(lis)
	defer srv.Stop()

	c, err := New([]string{lis.Addr().String()}, Options{AttemptTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Put(ctx, []byte("k"), []byte("v")); err == nil {
		t.Fatal("a node that never answers acknowledged a write")
	}
	if n := lis.accepted.Load(); n < 3 {
		t.Fatalf("%d connections over about 1 s of timed-out attempts: the client kept reusing one", n)
	}
}
