// Command raftkv is the command-line client for a raftkv cluster.
//
//	raftkv --addr node1:7000 put greeting hello
//	raftkv --addr node2:7000 get greeting      # node2 redirects to the leader
//	raftkv status                              # every node in $RAFTKV_ADDR
//
// Any node's address will do: a node that isn't the leader answers with the
// leader's address, and the client goes there. -v prints each redirect.
//
// Exit status: 0 on success, 1 if the request failed, 2 for a usage error, and 3
// when get finds no value for the key.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/abishekprabakar/raftkv/internal/client"
)

const usage = `usage: raftkv [flags] <command> [args]

commands:
  put KEY VALUE      set KEY to VALUE
  append KEY VALUE   add VALUE to the end of KEY's value
  delete KEY         remove KEY
  get KEY            print KEY's value (exit status 3 if it has none)
  status             print each node's own view of the cluster

flags:
`

func main() {
	defaultAddr := os.Getenv("RAFTKV_ADDR")
	if defaultAddr == "" {
		defaultAddr = "localhost:7000"
	}
	addr := flag.String("addr", defaultAddr, "comma-separated node addresses (default from $RAFTKV_ADDR)")
	timeout := flag.Duration("timeout", 10*time.Second, "give up after this long")
	verbose := flag.Bool("v", false, "print every redirect and retry to stderr")
	flag.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	opts := client.Options{}
	if *verbose {
		opts.Log = func(format string, a ...any) { fmt.Fprintf(os.Stderr, "raftkv: "+format+"\n", a...) }
	}
	addrs := strings.Split(*addr, ",")
	c, err := client.New(addrs, opts)
	if err != nil {
		fail(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cmd, args := args[0], args[1:]
	want := map[string]int{"put": 2, "append": 2, "delete": 1, "get": 1, "status": 0}
	if n, ok := want[cmd]; !ok || len(args) != n {
		flag.Usage()
		os.Exit(2)
	}
	start := time.Now()
	switch cmd {
	case "put":
		err = c.Put(ctx, []byte(args[0]), []byte(args[1]))
	case "append":
		err = c.Append(ctx, []byte(args[0]), []byte(args[1]))
	case "delete":
		err = c.Delete(ctx, []byte(args[0]))
	case "get":
		var value []byte
		var found bool
		value, found, err = c.Get(ctx, []byte(args[0]))
		if err == nil && !found {
			fmt.Fprintf(os.Stderr, "raftkv: %s has no value\n", args[0])
			os.Exit(3)
		}
		if err == nil {
			fmt.Println(string(value))
		}
	case "status":
		err = status(ctx, c, addrs)
	}
	if err != nil {
		if errors.Is(err, client.ErrGaveUp) && cmd != "get" && cmd != "status" {
			err = fmt.Errorf("%w\nthe write may or may not have happened", err)
		}
		fail(err)
	}
	if *verbose {
		fmt.Fprintf(os.Stderr, "raftkv: %s done in %v after %d redirects\n", cmd, time.Since(start).Round(time.Millisecond), c.Redirects)
	}
}

// status prints one line per node. It fails if any node didn't answer, which makes
// it usable as a health check.
func status(ctx context.Context, c *client.Client, addrs []string) error {
	var failed []string
	for _, addr := range addrs {
		st, err := c.Status(ctx, addr)
		if err != nil {
			fmt.Printf("%s unreachable: %v\n", addr, err)
			failed = append(failed, addr)
			continue
		}
		fmt.Printf("%s id=%d role=%s term=%d leader=%d commit=%d applied=%d last=%d snapshot=%d\n",
			addr, st.Id, st.Role, st.Term, st.LeaderId, st.CommitIndex, st.LastApplied, st.LastLogIndex, st.SnapshotIndex)
	}
	if len(failed) > 0 {
		return fmt.Errorf("no answer from %s", strings.Join(failed, ", "))
	}
	return nil
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "raftkv:", err)
	os.Exit(1)
}
