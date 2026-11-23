package kube

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/abishekprabakar/raftkv/internal/chaos"
)

// porcupineTimeout bounds the linearizability check of one run's history.
const porcupineTimeout = 10 * time.Minute

// Durability is the direct count: what was acknowledged, against what the final
// reads found. It doesn't need Porcupine, and it gives numbers a reader can
// check by hand from the history.
type Durability struct {
	Keys int `json:"keys"`
	// KeysNotRead is the keys whose final read failed. Their writes could not be
	// checked, so anything above 0 means the counts below are incomplete.
	KeysNotRead   int `json:"keys_whose_final_read_failed"`
	WritesAcked   int `json:"writes_acknowledged"`
	WritesUnknown int `json:"writes_unknown"`
	// AckedLost is the acknowledged appends whose value is missing from the
	// final read of its key. This is the number that must be 0.
	AckedLost int `json:"acknowledged_writes_lost"`
	// Duplicated is the appended values found more than once.
	Duplicated int `json:"writes_duplicated"`
	// UnknownApplied is the writes a client gave up on that did take effect.
	// That is allowed: unknown means it may have happened.
	UnknownApplied int `json:"unknown_writes_found_applied"`
	// NeverWritten is the values in a final read that no client ever sent.
	NeverWritten int `json:"values_never_written"`
	// Examples names up to ten of the writes counted as lost, duplicated or
	// never written.
	Examples []string `json:"examples,omitempty"`
}

// OK reports whether nothing was lost, repeated or made up, and every key was
// read.
func (d Durability) OK() bool {
	return d.AckedLost == 0 && d.Duplicated == 0 && d.NeverWritten == 0 && d.KeysNotRead == 0
}

// CheckDurability compares every append in a history with the final read of its
// key. Every appended value is unique and ends in ";", so the final value splits
// cleanly into the appends that took effect.
func CheckDurability(history []Op) Durability {
	var d Durability
	note := func(what string, op Op) {
		if len(d.Examples) < 10 {
			d.Examples = append(d.Examples, what+": "+op.Key+" "+op.Value)
		}
	}
	keys := map[string]bool{}
	final := map[string]map[string]int{} // key -> appended value -> times it appears
	for _, op := range history {
		keys[op.Key] = true
		if !op.Final || !op.OK {
			continue
		}
		counts := map[string]int{}
		for _, v := range strings.SplitAfter(op.Value, ";") {
			if v != "" {
				counts[v]++
			}
		}
		final[op.Key] = counts
	}
	d.Keys = len(keys)
	d.KeysNotRead = len(keys) - len(final)

	written := map[string]map[string]bool{}
	for _, op := range history {
		if op.Kind != chaos.Append {
			continue
		}
		if op.OK {
			d.WritesAcked++
		} else {
			d.WritesUnknown++
		}
		if written[op.Key] == nil {
			written[op.Key] = map[string]bool{}
		}
		written[op.Key][op.Value] = true
		counts, read := final[op.Key]
		if !read {
			continue
		}
		switch n := counts[op.Value]; {
		case n > 1:
			d.Duplicated++
			note("duplicated", op)
		case n == 0 && op.OK:
			d.AckedLost++
			note("acknowledged and lost", op)
		case n == 1 && !op.OK:
			d.UnknownApplied++
		}
	}
	for key, counts := range final {
		for v := range counts {
			if !written[key][v] {
				d.NeverWritten++
				note("never written", Op{Op: chaos.Op{Key: key, Value: v}})
			}
		}
	}
	return d
}

// Verdict is everything checked about one run's history.
type Verdict struct {
	Durability Durability `json:"durability"`
	// Linearizable is Porcupine's answer: Ok, Illegal, or Unknown if it timed out.
	Linearizable porcupine.CheckResult `json:"linearizable"`
	CheckSeconds float64               `json:"check_seconds"`
}

// OK reports whether the run passed D1 to D4.
func (v Verdict) OK() bool { return v.Durability.OK() && v.Linearizable == porcupine.Ok }

// Verify counts durability and checks the history with Porcupine, using M5's
// model unchanged. The history is saved under dir, compressed, so anyone can
// run the check again; if Porcupine finds a violation, a drawing of it is saved
// beside it.
func Verify(history []Op, dir string) (Verdict, error) {
	v := Verdict{Durability: CheckDurability(history)}
	plain := make([]chaos.Op, len(history))
	for i, op := range history {
		plain[i] = op.Op
	}
	t0 := time.Now()
	result, info := porcupine.CheckOperationsVerbose(chaos.Model, chaos.Operations(plain), porcupineTimeout)
	v.Linearizable, v.CheckSeconds = result, time.Since(t0).Seconds()
	if dir == "" {
		return v, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return v, err
	}
	err := writeGzip(filepath.Join(dir, "history.json.gz"), func(w io.Writer) error { return json.NewEncoder(w).Encode(history) })
	if err == nil && result == porcupine.Illegal {
		err = writeGzip(filepath.Join(dir, "violation.html.gz"), func(w io.Writer) error { return porcupine.Visualize(chaos.Model, info, w) })
	}
	return v, err
}

func writeGzip(path string, write func(io.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	if err := write(zw); err != nil {
		return err
	}
	return zw.Close()
}
