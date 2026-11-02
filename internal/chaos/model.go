// Package chaos runs fault schedules against a real raftkv cluster on Docker
// Compose, records what every client saw, and checks the history with Porcupine.
//
// The question it answers: with nodes cut off and reconnected while clients keep
// working, does the cluster still behave like a single copy of the data? That is
// linearizability. Every operation must appear to take effect at one instant
// between when it was called and when it returned, and the instants must be in an
// order that a single, correct key-value store could have produced.
//
// Porcupine searches for such an order. If none exists, the history is illegal, and
// it can draw the part that fails.
package chaos

import (
	"fmt"
	"slices"

	"github.com/anishathalye/porcupine"
)

// Kind is what an operation does.
type Kind uint8

const (
	Get Kind = iota
	Put
	Append
)

func (k Kind) String() string {
	return [...]string{"get", "put", "append"}[k]
}

// Input is an operation as a client issued it.
type Input struct {
	Kind  Kind
	Key   string
	Value string // for put and append
}

// Output is what came back. Only a get returns anything.
type Output struct {
	Value string
}

// Model is a key-value store as Porcupine sees it: one string per key, where a key
// that was never written holds "". Histories are split by key, because operations
// on different keys never constrain each other, and checking eight small histories
// is far cheaper than one large one.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			key := op.Input.(Input).Key
			byKey[key] = append(byKey[key], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		slices.Sort(keys) // a fixed order, so a report reads the same each time
		parts := make([][]porcupine.Operation, len(keys))
		for i, k := range keys {
			parts[i] = byKey[k]
		}
		return parts
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		s, in := state.(string), input.(Input)
		switch in.Kind {
		case Get:
			return output.(Output).Value == s, s
		case Put:
			return true, in.Value
		default:
			return true, s + in.Value
		}
	},
	Equal: func(a, b any) bool { return a.(string) == b.(string) },
	DescribeOperation: func(input, output any) string {
		in := input.(Input)
		if in.Kind == Get {
			return fmt.Sprintf("get(%s) -> %q", in.Key, output.(Output).Value)
		}
		return fmt.Sprintf("%s(%s, %q)", in.Kind, in.Key, in.Value)
	},
	DescribeState: func(state any) string { return fmt.Sprintf("%q", state) },
}

// Op is one completed or abandoned operation, with times in nanoseconds since the
// run started, all from one clock.
type Op struct {
	Client int    `json:"client"`
	Kind   Kind   `json:"kind"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"` // written, or read
	Call   int64  `json:"call"`
	Return int64  `json:"return"`
	// OK is false when the client gave up. For a write that means the outcome is
	// unknown; for a read, nothing was observed. Err says why.
	OK  bool   `json:"ok"`
	Err string `json:"err,omitempty"`
}

// Operations turns a history into Porcupine's form.
//
// A read that failed observed nothing, so it is left out. A write that failed
// may still have been applied, at any moment after it was called, or never. It
// is given a return time after everything else: Porcupine can then place it
// anywhere from its call to the very end, and placing it at the end has the same
// effect as it never happening.
func Operations(history []Op) []porcupine.Operation {
	var end int64
	for _, op := range history {
		end = max(end, op.Return)
	}
	var ops []porcupine.Operation
	for _, op := range history {
		in := Input{Kind: op.Kind, Key: op.Key}
		out := Output{}
		ret := op.Return
		switch {
		case op.Kind == Get && !op.OK:
			continue
		case op.Kind == Get:
			out.Value = op.Value
		case !op.OK:
			in.Value = op.Value
			ret = end + 1
		default:
			in.Value = op.Value
		}
		ops = append(ops, porcupine.Operation{ClientId: op.Client, Input: in, Call: op.Call, Output: out, Return: ret})
	}
	return ops
}
