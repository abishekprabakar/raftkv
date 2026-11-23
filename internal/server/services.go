package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/abishekprabakar/raftkv/internal/kv"
	"github.com/abishekprabakar/raftkv/internal/pb"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// raftService receives other nodes' streams of Raft messages.
type raftService struct {
	pb.UnimplementedRaftServer
	s *Server
}

func (h raftService) Stream(stream pb.Raft_StreamServer) error {
	s, ctx := h.s, stream.Context()
	for {
		env, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.StreamClosed{})
		}
		if err != nil {
			return err
		}
		from := raft.NodeID(env.From)
		if raft.NodeID(env.To) != s.cfg.ID || s.peers[from] == nil {
			// Nodes were given different address lists. Ending the stream makes
			// the sender log it, rather than every message vanishing silently.
			return status.Errorf(codes.FailedPrecondition, "node %d got a message from %d meant for %d", s.cfg.ID, env.From, env.To)
		}
		msg, err := fromEnvelope(env)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		select {
		case s.inbox <- inbound{from, msg}:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.loopDone:
			return status.Error(codes.Unavailable, errStopped.Error())
		}
	}
}

// kvService serves clients. Every handler follows the same pattern: build the
// request, hand it to the loop with do, and wait for the callback's answer.
type kvService struct {
	pb.UnimplementedKVServer
	s *Server
}

// failed turns a failure to reach or hear from the loop into a gRPC status. For a
// write, the outcome is then unknown, and the client retries under the same
// sequence number, which the session makes safe.
func failed(err error) error {
	if errors.Is(err, errStopped) {
		return status.Error(codes.Unavailable, err.Error())
	}
	return status.FromContextError(err).Err()
}

func commandFor(req *pb.WriteRequest) (kv.Command, error) {
	kinds := map[pb.Op]kv.Kind{pb.Op_OP_PUT: kv.Put, pb.Op_OP_APPEND: kv.Append, pb.Op_OP_DELETE: kv.Delete}
	kind, ok := kinds[req.Op]
	switch {
	case !ok:
		return kv.Command{}, fmt.Errorf("unknown op %v", req.Op)
	case req.ClientId == 0 || req.Seq == 0:
		// The store drops any seq at or below the last one it applied, which
		// starts at 0. A zero seq would be acknowledged and silently never applied.
		return kv.Command{}, errors.New("client_id and seq must both be nonzero")
	case len(req.Key) == 0:
		return kv.Command{}, errors.New("empty key")
	}
	cmd := kv.Command{ClientID: req.ClientId, Seq: req.Seq, Kind: kind, Key: req.Key}
	if kind != kv.Delete {
		cmd.Value = req.Value
	}
	return cmd, nil
}

func (h kvService) Write(ctx context.Context, req *pb.WriteRequest) (*pb.WriteResponse, error) {
	s := h.s
	cmd, err := commandFor(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	type outcome struct {
		err    error
		leader raft.NodeID
		term   uint64
	}
	done := make(chan outcome, 1) // buffered: the loop must never wait on a handler
	err = s.do(ctx, func() error {
		// The callback runs on the loop too, later, so it may ask who leads.
		return s.kv.Submit(cmd, func(err error) {
			st := s.kv.Status()
			done <- outcome{err, st.Leader, st.Term}
		})
	})
	if err != nil {
		return nil, failed(err)
	}
	o, err := await(ctx, s, done)
	switch {
	case err != nil:
		return nil, failed(err)
	case o.err == nil:
		return &pb.WriteResponse{Term: o.term}, nil
	case errors.Is(o.err, kv.ErrNotLeader):
		return &pb.WriteResponse{NotLeader: s.notLeader(o.leader)}, nil
	case errors.Is(o.err, raft.ErrEntryTooLarge):
		return nil, status.Error(codes.InvalidArgument, o.err.Error())
	}
	return nil, status.Error(codes.Unavailable, o.err.Error()) // kv.ErrUnknown
}

func (h kvService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s := h.s
	type outcome struct {
		value  []byte
		found  bool
		err    error
		leader raft.NodeID
	}
	done := make(chan outcome, 1)
	err := s.do(ctx, func() error {
		return s.kv.Read(req.Key, func(v []byte, found bool, err error) { done <- outcome{v, found, err, s.kv.Leader()} })
	})
	if err != nil {
		return nil, failed(err)
	}
	o, err := await(ctx, s, done)
	switch {
	case err != nil:
		return nil, failed(err)
	case o.err == nil:
		return &pb.GetResponse{Found: o.found, Value: o.value}, nil
	case errors.Is(o.err, kv.ErrNotLeader):
		return &pb.GetResponse{NotLeader: s.notLeader(o.leader)}, nil
	}
	return nil, status.Error(codes.Unavailable, o.err.Error()) // kv.ErrRetry
}

func (h kvService) Status(ctx context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	s := h.s
	done := make(chan raft.Status, 1)
	if err := s.do(ctx, func() error { done <- s.kv.Status(); return nil }); err != nil {
		return nil, failed(err)
	}
	st, err := await(ctx, s, done)
	if err != nil {
		return nil, failed(err)
	}
	return &pb.StatusResponse{
		Id: uint64(st.ID), Role: st.Role.String(), Term: st.Term,
		LeaderId: uint64(st.Leader), LeaderAddress: s.cfg.Peers[st.Leader],
		CommitIndex: st.CommitIndex, LastApplied: st.LastApplied,
		LastLogIndex: st.LastLogIndex, SnapshotIndex: st.SnapshotIndex,
	}, nil
}
