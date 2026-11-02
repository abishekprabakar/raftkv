// Command raftkvd runs one raftkv node.
//
//	raftkvd --id 1 --peers 1=node1:7000,2=node2:7000,3=node3:7000 --data /data
//
// Every node gets the same --peers list, which fixes the cluster's membership. The
// node listens on the port of its own address in that list unless --listen says
// otherwise. It stops cleanly on SIGINT or SIGTERM, and exits with status 1 if the
// Raft node fails, so that it can be restarted from its files.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/abishekprabakar/raftkv/internal/raft"
	"github.com/abishekprabakar/raftkv/internal/server"
)

func main() {
	id := flag.Uint64("id", 0, "this node's ID, one of the IDs in --peers")
	peers := flag.String("peers", "", "every member as id=host:port, comma-separated")
	data := flag.String("data", "", "directory for this node's files")
	listen := flag.String("listen", "", "listen address (default: the port of this node's own address)")
	tick := flag.Duration("tick", 10*time.Millisecond, "Raft clock tick")
	election := flag.Int("election-ticks", 50, "shortest election timeout, in ticks")
	heartbeat := flag.Int("heartbeat-ticks", 5, "heartbeat interval, in ticks")
	snapshotEvery := flag.Uint64("snapshot-every", 10_000, "compact the log after this many applied entries")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	members, err := parsePeers(*peers)
	if err != nil {
		fatal(log, err)
	}
	s, err := server.Start(server.Config{
		ID: raft.NodeID(*id), Peers: members, DataDir: *data, Listen: *listen,
		Tick: *tick, ElectionTicks: *election, HeartbeatTicks: *heartbeat,
		SnapshotEvery: *snapshotEvery, Logger: log,
	})
	if err != nil {
		fatal(log, err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-signals:
		log.Info("shutting down", "signal", sig.String())
	case <-s.Done():
	}
	if err := s.Stop(); err != nil {
		log.Error("closing files", "err", err)
	}
	if s.Err() != nil { // the loop has ended by now, so this doesn't wait
		os.Exit(1)
	}
}

// parsePeers reads "1=node1:7000,2=node2:7000" into a map.
func parsePeers(s string) (map[raft.NodeID]string, error) {
	members := map[raft.NodeID]string{}
	for _, item := range strings.Split(s, ",") {
		idText, addr, ok := strings.Cut(strings.TrimSpace(item), "=")
		id, err := strconv.ParseUint(idText, 10, 64)
		if !ok || err != nil || id == 0 || addr == "" {
			return nil, fmt.Errorf("bad --peers entry %q: want id=host:port with id above 0", item)
		}
		if _, dup := members[raft.NodeID(id)]; dup {
			return nil, fmt.Errorf("--peers lists node %d twice", id)
		}
		members[raft.NodeID(id)] = addr
	}
	return members, nil
}

func fatal(log *slog.Logger, err error) {
	log.Error("cannot start", "err", err)
	os.Exit(1)
}
