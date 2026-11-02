// Command crashtest is the crash-recovery harness for the storage engine.
//
// The parent process repeatedly launches itself as a child that opens the
// database and hammers it with writes from several goroutines, each owning a
// disjoint key range and a deterministic operation stream. Every acknowledged
// operation is reported to the parent over a pipe. After a random delay the
// parent sends SIGKILL. It then sometimes adds a torn tail to the newest WAL
// segment, sometimes starts recovery in another child and kills that too, and
// finally reopens the database in-process and checks that:
//
//  1. every acknowledged operation is reflected in the recovered state,
//  2. the recovered state of each stream is exactly a prefix of that stream
//     (durable writes beyond the last acknowledgement are allowed, torn or
//     reordered writes are not), and
//  3. every value read back is byte-for-byte what the generator produced,
//     including its embedded checksum.
//
// The workload child always starts from a directory the parent closed
// cleanly, so killing it can never interrupt a recovery. That is why recovery
// gets its own child, started while the WAL still holds what the workload left
// behind, and why those kills are timed from the moment Open begins.
//
// What this cannot test: SIGKILL leaves the OS page cache intact, so everything
// the process handed to write(2) survives. The harness therefore proves that a
// write is not acknowledged before it reaches the OS, and that recovery is
// correct. It does not prove the fsyncs are in the right places, and it passes
// with fsync turned off. See docs/NOTES.md.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abishekprabakar/raftkv/internal/hwinfo"
	"github.com/abishekprabakar/raftkv/internal/storage/engine"
	"github.com/abishekprabakar/raftkv/internal/storage/wal"
)

var (
	child        = flag.Bool("child", false, "run as the workload child (internal)")
	recoverChild = flag.Bool("recover-child", false, "open the database and wait to be killed (internal)")
	trials       = flag.Int("trials", 500, "number of kill/recover trials")
	dir          = flag.String("dir", "", "database directory (default: temp dir)")
	seed         = flag.Uint64("seed", 1, "random seed")
	writers      = flag.Int("writers", 4, "concurrent writer goroutines in the child")
	keys         = flag.Int("keys", 500, "keys per writer")
	out          = flag.String("out", "", "write a JSON summary here")
	startOps     = flag.String("startops", "", "comma-separated first op per writer (internal)")
	verbose      = flag.Bool("v", false, "log every trial")
)

const (
	// maxRecoveryKills is the most times one trial kills recovery in a row.
	maxRecoveryKills = 3
	// recoveryKillWindowMs bounds how long after Open starts a kill lands.
	// Recovery does a table fsync and manifest and directory fsyncs, so it
	// takes several fsync times; the window is chosen to land inside it.
	recoveryKillWindowMs = 30
	// tornTailOneIn gives the chance of adding a torn tail to a trial.
	tornTailOneIn = 4
)

// Small sizes so flushes and compactions happen within a trial.
func dbOptions() engine.Options {
	return engine.Options{
		SyncMode:            wal.GroupCommit,
		MemtableBytes:       64 << 10,
		BlockBytes:          1024,
		L0CompactionTrigger: 2,
		BaseLevelBytes:      256 << 10,
		LevelMultiplier:     4,
		TargetFileBytes:     64 << 10,
	}
}

// ---- deterministic workload --------------------------------------------------

type op struct {
	key    string
	value  []byte // nil for delete
	delete bool
}

// genOp derives operation n of writer g purely from (seed, g, n).
func genOp(seed uint64, g, n int, keysPerWriter int) op {
	h := fnv.New64a()
	var b [24]byte
	binary.LittleEndian.PutUint64(b[0:], seed)
	binary.LittleEndian.PutUint64(b[8:], uint64(g))
	binary.LittleEndian.PutUint64(b[16:], uint64(n))
	h.Write(b[:])
	r := rand.New(rand.NewPCG(h.Sum64(), uint64(n)*0x9E3779B97F4A7C15+1))
	key := fmt.Sprintf("w%02d-k%06d", g, r.IntN(keysPerWriter))
	if r.IntN(8) == 0 {
		return op{key: key, delete: true}
	}
	pad := make([]byte, r.IntN(240))
	for i := range pad {
		pad[i] = 'a' + byte(r.IntN(26))
	}
	body := fmt.Sprintf("g=%d;n=%d;key=%s;pad=%s;", g, n, key, pad)
	sum := crc32.ChecksumIEEE([]byte(body))
	return op{key: key, value: []byte(fmt.Sprintf("%scrc=%08x", body, sum))}
}

// parseValue extracts (g, n) from a stored value and verifies its checksum.
func parseValue(v []byte) (g, n int, ok bool) {
	s := string(v)
	i := strings.LastIndex(s, "crc=")
	if i < 0 || len(s)-i != 12 {
		return 0, 0, false
	}
	want, err := strconv.ParseUint(s[i+4:], 16, 32)
	if err != nil || crc32.ChecksumIEEE([]byte(s[:i])) != uint32(want) {
		return 0, 0, false
	}
	if _, err := fmt.Sscanf(s, "g=%d;n=%d;", &g, &n); err != nil {
		return 0, 0, false
	}
	return g, n, true
}

// ---- child --------------------------------------------------------------------

func runChild() {
	db, err := engine.Open(*dir, dbOptions())
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(2)
	}
	starts := parseStarts(*startOps, *writers)
	var outMu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < *writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for n := starts[g]; ; n++ {
				o := genOp(*seed, g, n, *keys)
				var err error
				if o.delete {
					err = db.Delete([]byte(o.key))
				} else {
					err = db.Put([]byte(o.key), o.value)
				}
				if err != nil {
					fmt.Fprintln(os.Stderr, "child write:", err)
					os.Exit(3)
				}
				line := fmt.Sprintf("A %d %d\n", g, n)
				outMu.Lock()
				os.Stdout.WriteString(line) // unbuffered: one syscall per ack
				outMu.Unlock()
			}
		}(g)
	}
	wg.Wait()
}

// runRecoveryChild opens the database and waits to be killed. Open replays the
// WAL, flushes it to a table and deletes the old segments, so a kill here lands
// in real recovery work. The parent times its kill from the first line.
func runRecoveryChild() {
	fmt.Println("started")
	if _, err := engine.Open(*dir, dbOptions()); err != nil {
		fmt.Fprintln(os.Stderr, "recovery open:", err)
		os.Exit(2)
	}
	fmt.Println("opened")
	// Sleep rather than select{}: once the background worker goes idle the
	// runtime would see every goroutine blocked and report a deadlock.
	time.Sleep(time.Hour)
}

func parseStarts(s string, n int) []int {
	starts := make([]int, n)
	if s == "" {
		return starts
	}
	for i, part := range strings.Split(s, ",") {
		if i < n {
			starts[i], _ = strconv.Atoi(part)
		}
	}
	return starts
}

// ---- parent -------------------------------------------------------------------

type streamState struct {
	applied int               // ops [0, applied) are reflected in model
	model   map[string][]byte // key -> value for this writer
}

func (s *streamState) apply(seed uint64, g int, n int, keysPerWriter int) {
	o := genOp(seed, g, n, keysPerWriter)
	if o.delete {
		delete(s.model, o.key)
	} else {
		s.model[o.key] = o.value
	}
}

type trialResult struct {
	Trial           int    `json:"trial"`
	KillAfterMs     int64  `json:"kill_after_ms"`
	AckedOps        int    `json:"acked_ops"`
	UnackedDurable  int    `json:"unacked_durable_ops"`
	RecoveryKills   int    `json:"recovery_kills"`
	KillsMidOpen    int    `json:"recovery_kills_mid_open"`
	TornTail        bool   `json:"torn_tail_injected"`
	RecoveredValues int    `json:"recovered_values"`
	SSTables        int    `json:"sstables_after_recovery"`
	DeepestLevel    int    `json:"deepest_level"`
	Replayed        int64  `json:"wal_records_replayed"`
	Truncated       int64  `json:"wal_bytes_truncated"`
	Error           string `json:"error,omitempty"`
}

type summary struct {
	GeneratedAt           string         `json:"generated_at"`
	Hardware              hwinfo.Info    `json:"hardware"`
	Config                map[string]any `json:"config"`
	Trials                int            `json:"trials"`
	Passed                int            `json:"passed"`
	Failed                int            `json:"failed"`
	TotalAckedOps         int            `json:"total_acked_ops"`
	TotalValuesValidated  int            `json:"total_values_validated"`
	TrialsKilledBeforeAck int            `json:"trials_killed_before_first_ack"`
	RecoveryKills         int            `json:"recovery_kills"`
	RecoveryKillsMidOpen  int            `json:"recovery_kills_mid_open"`
	TornTailsInjected     int            `json:"torn_tails_injected"`
	TrialsWithSSTables    int            `json:"trials_with_sstables"`
	TrialsWithCompaction  int            `json:"trials_with_data_below_l0"`
	TrialsWithTornTail    int            `json:"trials_with_truncated_wal_tail"`
	MaxUnackedDurable     int            `json:"max_unacked_durable_ops"`
	TotalUnackedDurable   int            `json:"total_unacked_durable_ops"`
	WallSeconds           float64        `json:"wall_seconds"`
	Failures              []trialResult  `json:"failures,omitempty"`
}

func main() {
	flag.Parse()
	if *child {
		runChild()
		return
	}
	if *recoverChild {
		runRecoveryChild()
		return
	}
	if *dir == "" {
		d, err := os.MkdirTemp("", "raftkv-crash-")
		if err != nil {
			fatal(err)
		}
		*dir = d
		defer os.RemoveAll(d)
	}
	self, err := os.Executable()
	if err != nil {
		fatal(err)
	}
	start := time.Now()
	rnd := rand.New(rand.NewPCG(*seed, 0xC0FFEE))
	streams := make([]*streamState, *writers)
	for g := range streams {
		streams[g] = &streamState{model: map[string][]byte{}}
	}
	sum := summary{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Hardware:    hwinfo.Collect(*dir),
		Config: map[string]any{
			"trials": *trials, "writers": *writers, "keys_per_writer": *keys, "seed": *seed,
			"sync_mode": dbOptions().SyncMode.String(), "memtable_bytes": dbOptions().MemtableBytes,
			"l0_compaction_trigger": dbOptions().L0CompactionTrigger, "kill_signal": "SIGKILL",
			"kill_delay_ms":            "30% in [0,60), 45% in [60,400), 25% in [400,1500)",
			"recovery_kills_per_trial": fmt.Sprintf("0 to %d", maxRecoveryKills),
			"recovery_kill_delay_ms":   fmt.Sprintf("0 to %d after Open starts", recoveryKillWindowMs-1),
			"torn_tail_chance":         fmt.Sprintf("1 in %d trials", tornTailOneIn),
		},
	}
	for t := 1; t <= *trials; t++ {
		res := runTrial(self, rnd, t, streams)
		sum.Trials++
		sum.TotalAckedOps += res.AckedOps
		sum.TotalValuesValidated += res.RecoveredValues
		sum.TotalUnackedDurable += res.UnackedDurable
		sum.RecoveryKills += res.RecoveryKills
		sum.RecoveryKillsMidOpen += res.KillsMidOpen
		if res.TornTail {
			sum.TornTailsInjected++
		}
		if res.UnackedDurable > sum.MaxUnackedDurable {
			sum.MaxUnackedDurable = res.UnackedDurable
		}
		if res.AckedOps == 0 {
			sum.TrialsKilledBeforeAck++
		}
		if res.SSTables > 0 {
			sum.TrialsWithSSTables++
		}
		if res.DeepestLevel > 0 {
			sum.TrialsWithCompaction++
		}
		if res.Truncated > 0 {
			sum.TrialsWithTornTail++
		}
		if res.Error != "" {
			sum.Failed++
			sum.Failures = append(sum.Failures, res)
			fmt.Printf("trial %d FAILED: %s\n", t, res.Error)
			// The model can no longer be trusted after a failure; stop here.
			break
		} else {
			sum.Passed++
		}
		if *verbose || t%25 == 0 {
			fmt.Printf("trial %4d/%d  kill@%4dms acked=%5d unacked_durable=%d ssts=%2d deepest=L%d replayed=%d  pass=%d fail=%d\n",
				t, *trials, res.KillAfterMs, res.AckedOps, res.UnackedDurable, res.SSTables, res.DeepestLevel, res.Replayed, sum.Passed, sum.Failed)
		}
	}
	sum.WallSeconds = time.Since(start).Seconds()
	fmt.Printf("\n%d trials: %d passed, %d failed; %d acked ops verified, %d values validated, %d trials had data below L0, %d trials killed before first ack, %d recovery kills (%d during Open with WAL data), %d torn tails injected, %d seen truncated by the final Open (%.0fs)\n",
		sum.Trials, sum.Passed, sum.Failed, sum.TotalAckedOps, sum.TotalValuesValidated, sum.TrialsWithCompaction, sum.TrialsKilledBeforeAck,
		sum.RecoveryKills, sum.RecoveryKillsMidOpen, sum.TornTailsInjected, sum.TrialsWithTornTail, sum.WallSeconds)
	if *out != "" {
		os.MkdirAll(filepath.Dir(*out), 0o755)
		data, _ := json.MarshalIndent(sum, "", "  ")
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			fatal(err)
		}
		fmt.Println("wrote", *out)
	}
	if sum.Failed > 0 {
		os.Exit(1)
	}
}

func killDelay(rnd *rand.Rand) time.Duration {
	switch p := rnd.Float64(); {
	case p < 0.30:
		return time.Duration(rnd.IntN(60)) * time.Millisecond
	case p < 0.75:
		return time.Duration(60+rnd.IntN(340)) * time.Millisecond
	default:
		return time.Duration(400+rnd.IntN(1100)) * time.Millisecond
	}
}

// walSegments returns the WAL files in dir, oldest first. Their names are
// zero-padded numbers, so sorting by name sorts by age.
func walSegments(dir string) ([]string, error) {
	segs, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	sort.Strings(segs)
	return segs, err
}

func walBytes(dir string) (int64, error) {
	segs, err := walSegments(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, seg := range segs {
		info, err := os.Stat(seg)
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// injectTornTail appends the start of a record that claims more bytes than
// follow it, which is what a power cut in the middle of a write leaves behind.
// SIGKILL never produces one on its own, because the kernel finishes a write(2)
// that has started, so without this the truncation path would go unexercised.
func injectTornTail(dir string, rnd *rand.Rand) (bool, error) {
	segs, err := walSegments(dir)
	if err != nil || len(segs) == 0 {
		return false, err
	}
	frame := make([]byte, 8+rnd.IntN(64))
	binary.LittleEndian.PutUint32(frame[0:], uint32(len(frame)+100)) // the length field runs past the end
	for i := 4; i < len(frame); i++ {
		frame[i] = byte(rnd.Uint32())
	}
	f, err := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return false, err
	}
	defer f.Close()
	_, err = f.Write(frame)
	return err == nil, err
}

// crashRecovery starts recovery in a child and kills it while Open runs, kills
// times in a row, leaving whatever state each kill leaves for the next one.
func crashRecovery(self string, rnd *rand.Rand, kills int, res *trialResult) error {
	for i := 0; i < kills; i++ {
		pending, err := walBytes(*dir)
		if err != nil {
			return err
		}
		cmd := exec.Command(self, "-recover-child", "-dir", *dir)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return err
		}
		if err := cmd.Start(); err != nil {
			return err
		}
		sc := bufio.NewScanner(stdout)
		if !sc.Scan() {
			cmd.Wait()
			return errors.New("recovery child exited before starting Open")
		}
		time.Sleep(time.Duration(rnd.IntN(recoveryKillWindowMs)) * time.Millisecond)
		cmd.Process.Kill()
		opened := false
		for sc.Scan() {
			opened = opened || sc.Text() == "opened"
		}
		cmd.Wait()
		if cmd.ProcessState.Exited() {
			return fmt.Errorf("recovery child exited on its own with code %d", cmd.ProcessState.ExitCode())
		}
		res.RecoveryKills++
		if pending > 0 && !opened {
			res.KillsMidOpen++
		}
	}
	return nil
}

func runTrial(self string, rnd *rand.Rand, t int, streams []*streamState) trialResult {
	res := trialResult{Trial: t}
	starts := make([]string, len(streams))
	for g, s := range streams {
		starts[g] = strconv.Itoa(s.applied)
	}
	cmd := exec.Command(self, "-child", "-dir", *dir, "-seed", strconv.FormatUint(*seed, 10),
		"-writers", strconv.Itoa(*writers), "-keys", strconv.Itoa(*keys), "-startops", strings.Join(starts, ","))
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if err := cmd.Start(); err != nil {
		res.Error = err.Error()
		return res
	}
	lastAck := make([]int, len(streams))
	for g := range lastAck {
		lastAck[g] = streams[g].applied - 1
	}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			var g, n int
			if _, err := fmt.Sscanf(sc.Text(), "A %d %d", &g, &n); err == nil && g < len(lastAck) && n > lastAck[g] {
				lastAck[g] = n
			}
		}
	}()
	delay := killDelay(rnd)
	res.KillAfterMs = delay.Milliseconds()
	time.Sleep(delay)
	cmd.Process.Kill()
	// Read to EOF before calling Wait: Wait closes the pipe, which would throw
	// away any acks still in it.
	<-readerDone
	cmd.Wait()
	if cmd.ProcessState.Exited() {
		res.Error = fmt.Sprintf("workload child exited on its own with code %d", cmd.ProcessState.ExitCode())
		return res
	}

	for g, s := range streams {
		res.AckedOps += lastAck[g] + 1 - s.applied
	}

	if rnd.IntN(tornTailOneIn) == 0 {
		if res.TornTail, err = injectTornTail(*dir, rnd); err != nil {
			res.Error = "inject torn tail: " + err.Error()
			return res
		}
	}
	kills := rnd.IntN(maxRecoveryKills + 1)
	if err := crashRecovery(self, rnd, kills, &res); err != nil {
		res.Error = err.Error()
		return res
	}

	// Recover and verify.
	db, err := engine.Open(*dir, dbOptions())
	if err != nil {
		res.Error = "open after crash: " + err.Error()
		return res
	}
	defer db.Close()
	st := db.Stats()
	res.Replayed = st.ReplayedRecords
	res.Truncated = st.WALTruncatedBytes
	// With no recovery child in between, this Open is the first to see the
	// tail. After a recovery child it may already be gone.
	if res.TornTail && kills == 0 && res.Truncated == 0 {
		res.Error = "an injected torn tail was not truncated"
		return res
	}
	for lvl, n := range st.LevelFiles {
		res.SSTables += n
		if n > 0 {
			res.DeepestLevel = lvl
		}
	}

	// Read back everything, validating each value's checksum and ownership.
	recovered := make([]map[string][]byte, len(streams))
	for g := range recovered {
		recovered[g] = map[string][]byte{}
	}
	it := db.Scan(nil, nil)
	for it.Next() {
		g, n, ok := parseValue(it.Value())
		if !ok {
			res.Error = fmt.Sprintf("corrupt value for key %q: %q", it.Key(), it.Value())
			it.Close()
			return res
		}
		want := genOp(*seed, g, n, *keys)
		if want.key != string(it.Key()) || string(want.value) != string(it.Value()) {
			res.Error = fmt.Sprintf("value for key %q claims to be op (%d,%d) but does not match the generator", it.Key(), g, n)
			it.Close()
			return res
		}
		recovered[g][string(it.Key())] = append([]byte(nil), it.Value()...)
		res.RecoveredValues++
	}
	if err := it.Close(); err != nil {
		res.Error = "scan: " + err.Error()
		return res
	}

	// Each stream must be a prefix that covers every acknowledged op.
	const maxUnacked = 100000
	for g, s := range streams {
		for s.applied <= lastAck[g] {
			s.apply(*seed, g, s.applied, *keys)
			s.applied++
		}
		mismatch := countMismatch(s.model, recovered[g])
		extra := 0
		for mismatch > 0 && extra < maxUnacked {
			o := genOp(*seed, g, s.applied, *keys)
			before := s.model[o.key]
			s.apply(*seed, g, s.applied, *keys)
			s.applied++
			extra++
			mismatch += mismatchDelta(before, s.model[o.key], recovered[g][o.key])
		}
		if mismatch > 0 {
			res.Error = fmt.Sprintf("writer %d: recovered state is not a prefix of the op stream (last ack op %d, %d keys differ)", g, lastAck[g], mismatch)
			return res
		}
		res.UnackedDurable += extra
		// Point reads must agree with the scan.
		for k, want := range s.model {
			got, err := db.Get([]byte(k))
			if err != nil || string(got) != string(want) {
				res.Error = fmt.Sprintf("get %q = %q (%v), want %q", k, got, err, want)
				return res
			}
		}
	}
	return res
}

func countMismatch(model, got map[string][]byte) int {
	n := 0
	for k, v := range model {
		if g, ok := got[k]; !ok || string(g) != string(v) {
			n++
		}
	}
	for k := range got {
		if _, ok := model[k]; !ok {
			n++
		}
	}
	return n
}

// mismatchDelta returns the change in mismatch count for one key after the
// model value moved from before to after; got is the recovered value.
func mismatchDelta(before, after, got []byte) int {
	eq := func(a, b []byte) bool { return (a == nil) == (b == nil) && string(a) == string(b) }
	d := 0
	if !eq(before, got) {
		d--
	}
	if !eq(after, got) {
		d++
	}
	return d
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
