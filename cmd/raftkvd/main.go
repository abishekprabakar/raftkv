// Command raftkvd runs one raftkv node.
//
//	raftkvd --id 1 --peers 1=node1:7000,2=node2:7000,3=node3:7000 --data /data
//
// Every node gets the same --peers list, which fixes the cluster's membership. The
// node listens on the port of its own address in that list unless --listen says
// otherwise. It stops cleanly on SIGINT or SIGTERM, and exits with status 1 if the
// Raft node fails, so that it can be restarted from its files.
//
// On Kubernetes every pod of a StatefulSet runs the same command line, so the ID
// can't be written into it. --id-from-hostname takes it from the pod's name, and
// --health serves the two endpoints the kubelet probes:
//
//	raftkvd --id-from-hostname --peers ... --data /data --health :8080
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/abishekprabakar/raftkv/internal/raft"
	"github.com/abishekprabakar/raftkv/internal/server"
)

// version is set at build time with -ldflags "-X main.version=...". Two images
// built from the same source with different versions are how the rolling-update
// test tells which one a pod is running.
var version = "dev"

func main() {
	id := flag.Uint64("id", 0, "this node's ID, one of the IDs in --peers")
	idFromHost := flag.Bool("id-from-hostname", false, "take the ID from the number at the end of the hostname, plus one (pod raftkv-0 is node 1)")
	health := flag.String("health", "", "serve /livez and /readyz over HTTP on this address (default: off)")
	readyLag := flag.Uint64("ready-lag", 64, "entries a follower may trail the leader's commit index and still be ready")
	showVersion := flag.Bool("version", false, "print the version and exit")
	peers := flag.String("peers", "", "every member as id=host:port, comma-separated")
	data := flag.String("data", "", "directory for this node's files")
	listen := flag.String("listen", "", "listen address (default: the port of this node's own address)")
	tick := flag.Duration("tick", 10*time.Millisecond, "Raft clock tick")
	election := flag.Int("election-ticks", 50, "shortest election timeout, in ticks")
	heartbeat := flag.Int("heartbeat-ticks", 5, "heartbeat interval, in ticks")
	snapshotEvery := flag.Uint64("snapshot-every", 10_000, "compact the log after this many applied entries")
	verbose := flag.Bool("v", false, "debug logging")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	log.Info("raftkvd", "version", version)
	members, err := parsePeers(*peers)
	if err != nil {
		fatal(log, err)
	}
	if *idFromHost {
		host, err := os.Hostname()
		if err != nil {
			fatal(log, err)
		}
		if *id, err = idFromHostname(host); err != nil {
			fatal(log, err)
		}
	}
	// Bind the health port before opening any files, so a port that is taken
	// fails at once. It only starts answering after the node has started: until
	// then a probe gets no reply, which is the truth.
	var healthListener net.Listener
	if *health != "" {
		if healthListener, err = net.Listen("tcp", *health); err != nil {
			fatal(log, err)
		}
	}
	s, err := server.Start(server.Config{
		ID: raft.NodeID(*id), Peers: members, DataDir: *data, Listen: *listen,
		Tick: *tick, ElectionTicks: *election, HeartbeatTicks: *heartbeat,
		SnapshotEvery: *snapshotEvery, ReadyLag: *readyLag, Logger: log,
	})
	if err != nil {
		fatal(log, err)
	}
	if healthListener != nil {
		hs := &http.Server{Handler: s.HealthHandler(), ReadHeaderTimeout: 2 * time.Second}
		go hs.Serve(healthListener)
		defer hs.Close()
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

// idFromHostname turns a StatefulSet pod name into a node ID. A StatefulSet names
// its pods NAME-0, NAME-1 and so on, and gives a pod the same name every time it
// is replaced, which is exactly what a Raft node's identity needs. Node IDs start
// at 1 because 0 means "nobody", so the ID is the ordinal plus one.
func idFromHostname(host string) (uint64, error) {
	i := strings.LastIndexByte(host, '-')
	ordinal, err := strconv.ParseUint(host[i+1:], 10, 32)
	if i < 0 || err != nil {
		return 0, fmt.Errorf("hostname %q does not end in -N, so it gives no node ID", host)
	}
	return ordinal + 1, nil
}

func fatal(log *slog.Logger, err error) {
	log.Error("cannot start", "err", err)
	os.Exit(1)
}
