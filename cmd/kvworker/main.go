// Command kvworker is one client for the fault-injection harness, cmd/chaos. It
// reads one operation per line on stdin, as JSON, runs it against the cluster, and
// writes one result per line on stdout, in the same order.
//
// The harness starts it inside a node's container with docker exec. That puts the
// client next to its node: when the node is cut off from the network, the client
// can still reach it, which is how a cut-off leader gets asked for reads.
//
//	{"op":"append","key":"k3","value":"c2.17;"}  ->  {"ok":true}
//	{"op":"get","key":"k3"}                      ->  {"ok":true,"found":true,"value":"c2.17;"}
//
// "ok":false means the operation didn't complete in time. For a write that means
// its outcome is unknown, since it may still have been applied.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/abishekprabakar/raftkv/internal/client"
)

type request struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type result struct {
	OK    bool   `json:"ok"`
	Found bool   `json:"found,omitempty"`
	Value string `json:"value,omitempty"`
	Err   string `json:"err,omitempty"`
}

func main() {
	addr := flag.String("addr", "localhost:7000", "comma-separated node addresses, tried in this order")
	timeout := flag.Duration("timeout", 4*time.Second, "time allowed for one operation")
	attempt := flag.Duration("attempt", time.Second, "time allowed for one try at one node")
	flag.Parse()

	c, err := client.New(strings.Split(*addr, ","), client.Options{AttemptTimeout: *attempt})
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvworker:", err)
		os.Exit(1)
	}
	defer c.Close()

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 1<<20), 1<<20)
	out := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(out)
	for in.Scan() {
		var req request
		var res result
		if err := json.Unmarshal(in.Bytes(), &req); err != nil {
			res.Err = "bad request: " + err.Error()
		} else {
			res = run(c, req, *timeout)
		}
		enc.Encode(res)
		out.Flush() // the harness waits for each answer before sending the next
	}
}

func run(c *client.Client, req request, timeout time.Duration) result {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	key, value := []byte(req.Key), []byte(req.Value)
	var err error
	switch req.Op {
	case "put":
		err = c.Put(ctx, key, value)
	case "append":
		err = c.Append(ctx, key, value)
	case "get":
		v, found, err := c.Get(ctx, key)
		if err != nil {
			return result{Err: err.Error()}
		}
		return result{OK: true, Found: found, Value: string(v)}
	default:
		return result{Err: "unknown op " + req.Op}
	}
	if err != nil {
		return result{Err: err.Error()}
	}
	return result{OK: true}
}
