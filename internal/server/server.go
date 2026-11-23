// Package server runs one raftkv node as a real process: a kv.Server driven by a
// real clock, talking to its peers and its clients over gRPC.
//
// The raft and kv packages are single-threaded on purpose (see the raft package
// comment), and this is the one place they meet concurrency. Everything that
// touches them runs on one goroutine, the loop. gRPC handlers, the clock and
// incoming messages never call them directly: they hand work to the loop through
// channels, and the loop does one thing at a time. So the deterministic core the
// simulator tested is exactly the code that runs here, with no locks added to it.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/abishekprabakar/raftkv/internal/kv"
	"github.com/abishekprabakar/raftkv/internal/pb"
	"github.com/abishekprabakar/raftkv/internal/raft"
)

// Config configures a node. Zero values select the defaults listed.
type Config struct {
	ID raft.NodeID
	// Peers maps every member, this node included, to the address the others
	// reach it at. Membership is fixed: every node must be given the same map.
	Peers map[raft.NodeID]string
	// DataDir holds the Raft log (raft/) and the store (kv/).
	DataDir string
	// Listen is the address to listen on. Default: every interface, on the port
	// of this node's own address in Peers.
	Listen string
	// Listener, if set, is used instead of Listen. Tests bind free ports first,
	// so that every node's address is known before any node starts.
	Listener net.Listener

	// Tick is how often the Raft clock advances. Default 10 ms.
	Tick time.Duration
	// ElectionTicks and HeartbeatTicks are passed to raft.Config. Defaults 50 and
	// 5: an election timeout of 500 ms to 1 s, and a heartbeat every 50 ms.
	ElectionTicks  int
	HeartbeatTicks int
	// SnapshotEvery and TrailingEntries are passed to kv.Config. Defaults 10,000
	// and 1,000.
	SnapshotEvery   uint64
	TrailingEntries uint64
	// ReadyLag is how many entries a follower's applied index may trail the
	// leader's commit index and still count as ready. Default 64, one
	// AppendEntries worth: under steady writes a healthy follower is always a
	// message behind the leader, and it should not flap in and out of readiness
	// for that.
	ReadyLag uint64

	Logger *slog.Logger
}

func (c Config) withDefaults() (Config, error) {
	if _, ok := c.Peers[c.ID]; !ok || c.ID == raft.None {
		return c, fmt.Errorf("server: node %d is not in the peer list", c.ID)
	}
	if c.DataDir == "" {
		return c, errors.New("server: no data directory")
	}
	if c.Listen == "" {
		_, port, err := net.SplitHostPort(c.Peers[c.ID])
		if err != nil {
			return c, fmt.Errorf("server: own address: %w", err)
		}
		c.Listen = ":" + port
	}
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&c.ElectionTicks, 50)
	def(&c.HeartbeatTicks, 5)
	if c.Tick <= 0 {
		c.Tick = 10 * time.Millisecond
	}
	if c.SnapshotEvery == 0 {
		c.SnapshotEvery = 10_000
	}
	if c.TrailingEntries == 0 {
		c.TrailingEntries = 1_000
	}
	if c.ReadyLag == 0 {
		c.ReadyLag = 64
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c, nil
}

// errStopped is what callers see once the loop has ended.
var errStopped = errors.New("server: node stopped")

type inbound struct {
	from raft.NodeID
	msg  raft.Message
}

// Server is one running node.
type Server struct {
	cfg Config
	log *slog.Logger

	// Only the loop goroutine touches these three after Start returns.
	kv        *kv.Server
	raftFiles *raft.FileStorage
	store     *kv.LSM

	peers map[raft.NodeID]*peer // read-only after Start
	grpc  *grpc.Server

	inbox chan inbound
	calls chan func() error // unbuffered: a send returns once the loop has taken it

	stopPeers context.CancelFunc
	stopLoop  chan struct{}
	loopDone  chan struct{}
	loopErr   error // why the loop ended, if it failed; read after loopDone closes
	wg        sync.WaitGroup
	stopOnce  sync.Once
	stopErr   error
}

// Start opens the node's files, starts serving, and returns. The node then runs
// until Stop, or until it fails (see Done).
func Start(cfg Config) (*Server, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg: cfg, log: cfg.Logger.With("node", cfg.ID),
		peers: map[raft.NodeID]*peer{},
		inbox: make(chan inbound, peerQueue), calls: make(chan func() error),
		stopLoop: make(chan struct{}), loopDone: make(chan struct{}),
	}
	if err := s.open(); err != nil {
		s.closeFiles()
		for _, p := range s.peers {
			if conn := p.connection(); conn != nil {
				conn.Close()
			}
		}
		return nil, err
	}

	recovered := s.kv.Status() // before the loop starts, so nothing else is using it

	ctx, cancel := context.WithCancel(context.Background())
	s.stopPeers = cancel
	for _, p := range s.peers {
		s.wg.Go(func() { p.run(ctx) })
	}
	go s.loop()
	lis := s.cfg.Listener
	s.wg.Go(func() {
		if err := s.grpc.Serve(lis); err != nil {
			s.log.Error("gRPC server ended", "err", err)
		}
	})
	// What the node found on disk. A node that has run before starts with its
	// term and its log; one that starts with neither has an empty data directory.
	s.log.Info("started", "listen", lis.Addr().String(), "peers", len(cfg.Peers),
		"term", recovered.Term, "last_log_index", recovered.LastLogIndex, "applied", recovered.LastApplied)
	return s, nil
}

func (s *Server) open() error {
	cfg := s.cfg
	var err error
	if s.raftFiles, err = raft.OpenFileStorage(filepath.Join(cfg.DataDir, "raft")); err != nil {
		return err
	}
	if s.store, err = kv.OpenLSM(filepath.Join(cfg.DataDir, "kv")); err != nil {
		return err
	}
	ids := make([]raft.NodeID, 0, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		ids = append(ids, id)
		if id == cfg.ID {
			continue
		}
		if s.peers[id], err = newPeer(id, addr, s.log); err != nil {
			return err
		}
	}
	slices.Sort(ids)
	s.kv, err = kv.NewServer(kv.Config{
		Raft: raft.Config{
			ID: cfg.ID, Peers: ids, ElectionTicks: cfg.ElectionTicks, HeartbeatTicks: cfg.HeartbeatTicks,
			// Election timeouts must differ between nodes and between restarts of
			// one node, or two nodes could keep timing out together.
			Seed:    uint64(time.Now().UnixNano()),
			Storage: s.raftFiles, Transport: s,
		},
		Backend: s.store, SnapshotEvery: cfg.SnapshotEvery, TrailingEntries: cfg.TrailingEntries,
	})
	if err != nil {
		return err
	}
	if s.cfg.Listener == nil {
		if s.cfg.Listener, err = net.Listen("tcp", cfg.Listen); err != nil {
			return err
		}
	}
	s.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessageBytes), grpc.MaxSendMsgSize(maxMessageBytes),
		// Peers ping idle connections every 10 s (see dialOptions). The default
		// policy rejects pings more often than every 5 minutes and closes the
		// connection, so it has to allow them.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: time.Second, PermitWithoutStream: true}),
		grpc.WaitForHandlers(true),
	)
	pb.RegisterRaftServer(s.grpc, raftService{s: s})
	pb.RegisterKVServer(s.grpc, kvService{s: s})
	return nil
}

// Send implements raft.Transport. The loop calls it from inside kv.Server, so it
// must not block, and it doesn't: the peer's queue takes the message or drops it.
func (s *Server) Send(to raft.NodeID, msg raft.Message) {
	if p, ok := s.peers[to]; ok {
		p.send(toEnvelope(s.cfg.ID, to, msg))
	}
}

// loop is the only goroutine that calls into kv.Server, and through it the Raft
// node and the store. One select, one input at a time.
func (s *Server) loop() {
	defer close(s.loopDone)
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	last := s.kv.Status()
	for {
		var err error
		select {
		case <-s.stopLoop:
			return
		case <-ticker.C:
			err = s.kv.Tick()
		case in := <-s.inbox:
			err = s.kv.Receive(in.from, in.msg)
		case call := <-s.calls:
			err = call()
		}
		if err != nil {
			// A Raft node stops for good after a failed save, because it can no
			// longer tell what it has promised. The process should exit and start
			// again from its files.
			s.loopErr = err
			s.log.Error("node failed", "err", err)
			return
		}
		last = s.logChanges(last)
	}
}

func (s *Server) logChanges(last raft.Status) raft.Status {
	st := s.kv.Status()
	if st.Role != last.Role || st.Term != last.Term || st.Leader != last.Leader {
		s.log.Info("raft state", "role", st.Role.String(), "term", st.Term, "leader", st.Leader)
	}
	return st
}

// do hands f to the loop. It returns once the loop has taken f, which will then
// run, so a caller waiting on f's result can't miss it.
func (s *Server) do(ctx context.Context, f func() error) error {
	select {
	case s.calls <- f:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.loopDone:
		return errStopped
	}
}

// await waits for a result the loop will send on ch.
func await[T any](ctx context.Context, s *Server, ch <-chan T) (T, error) {
	var zero T
	select {
	case v := <-ch:
		return v, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-s.loopDone:
		return zero, errStopped
	}
}

// Done is closed when the node stops, whether by Stop or by failing.
func (s *Server) Done() <-chan struct{} { return s.loopDone }

// Err says why the node failed, once Done is closed. It is nil after a plain Stop.
func (s *Server) Err() error {
	<-s.loopDone
	return s.loopErr
}

// Addr returns the address the node listens on.
func (s *Server) Addr() string { return s.cfg.Listener.Addr().String() }

// Stop shuts the node down and closes its files. It is safe to call twice.
func (s *Server) Stop() error {
	s.stopOnce.Do(func() {
		// Close connections first. Every handler waits on its context as well as
		// on the loop, so they all return, and nothing new reaches the loop.
		s.grpc.Stop()
		close(s.stopLoop)
		<-s.loopDone
		s.stopPeers()
		s.wg.Wait()
		for _, p := range s.peers {
			p.connection().Close()
		}
		s.stopErr = s.closeFiles()
		s.log.Info("stopped")
	})
	return s.stopErr
}

func (s *Server) closeFiles() error {
	var errs []error
	if s.store != nil {
		errs = append(errs, s.store.Close())
	}
	if s.raftFiles != nil {
		errs = append(errs, s.raftFiles.Close())
	}
	return errors.Join(errs...)
}

// notLeader builds the redirect a client follows.
func (s *Server) notLeader(leader raft.NodeID) *pb.NotLeader {
	return &pb.NotLeader{LeaderId: uint64(leader), LeaderAddress: s.cfg.Peers[leader]}
}
