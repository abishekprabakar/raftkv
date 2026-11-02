package sim

import "math/rand/v2"

// Virtual time is counted in microseconds.
const (
	Millisecond int64 = 1000
	Second            = 1000 * Millisecond
	// TickLength is the virtual time between a node's calls to Tick.
	TickLength = 10 * Millisecond
)

// Profiles lists the kinds of schedule. A seed picks one by remainder, so any
// range of seeds covers them evenly.
var Profiles = []string{"calm", "lossy", "partition", "crash", "chaos", "figure8", "solo", "snapshot"}

// Schedule is the plan for one run. NewSchedule derives all of it from the
// seed, so the seed alone identifies a run.
type Schedule struct {
	Seed     uint64
	Profile  string
	Nodes    int
	Lockstep bool // every node ticks at the same instant

	ElectionTicks        int
	HeartbeatTicks       int
	MaxEntriesPerMessage int

	DropRate      float64
	DupRate       float64
	MinDelay      int64
	MaxDelay      int64
	LongDelayRate float64 // chance of a delay up to LongDelayMax, far past any timeout
	LongDelayMax  int64

	PartitionGap int64 // longest wait between partitions; 0 means none
	PartitionLen int64 // longest a partition lasts
	OneWayRate   float64

	CrashGap        int64 // longest wait between random crashes; 0 means none
	MaxDowntime     int64
	CrashDuringSave float64 // chance per Save that the update is lost in a crash
	CrashAfterSave  float64 // chance per Save of dying before its messages leave
	LeaderHunt      int64   // if set, crash a leader within this long of proposing

	Clients       int
	Keys          int
	ReadRate      float64 // share of client operations that are reads
	ThinkTime     int64   // longest wait between a client's operations
	ClientTimeout int64   // how long a client waits before retrying elsewhere
	ChaosLen      int64   // how long faults run before the quiet phase

	SnapshotEvery   uint64 // compact the log this often; 0 means never
	TrailingEntries uint64
}

func between(r *rand.Rand, lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	return lo + r.Int64N(hi-lo+1)
}

func rate(r *rand.Rand, lo, hi float64) float64 { return lo + r.Float64()*(hi-lo) }

func pick[T any](r *rand.Rand, options ...T) T { return options[r.IntN(len(options))] }

// NewSchedule builds the plan for a seed.
//
// Most faults are random, but two profiles are aimed. Figure 8 needs a very
// particular history: a leader that replicates an old entry to a majority and
// then dies before its own term's entry spreads, followed by a leader who never
// had that entry. Random crashes rarely produce it, so that profile crashes each
// leader right after it accepts commands, sends one entry per message so old
// entries spread ahead of the new term's no-op, and mirrors 6.5840's Figure 8
// test. The lockstep flag exists because identical clocks are the worst case for
// election timeouts: only randomization can break the tie.
func NewSchedule(seed uint64) Schedule {
	r := rand.New(rand.NewPCG(seed, 0x5C4ED))
	s := Schedule{
		Seed:                 seed,
		Profile:              Profiles[seed%uint64(len(Profiles))],
		Nodes:                pick(r, 3, 5),
		ElectionTicks:        10,
		HeartbeatTicks:       2,
		MaxEntriesPerMessage: pick(r, 1, 2, 8, 64),
		MinDelay:             1 * Millisecond,
		MaxDelay:             5 * Millisecond,
		MaxDowntime:          500 * Millisecond,
		Clients:              pick(r, 1, 2, 3, 5),
		Keys:                 pick(r, 1, 2, 4),
		ReadRate:             rate(r, 0.1, 0.5),
		ThinkTime:            between(r, 1, 30) * Millisecond,
		ClientTimeout:        between(r, 50, 300) * Millisecond,
		ChaosLen:             between(r, 2, 6) * Second,
		SnapshotEvery:        pick[uint64](r, 0, 10, 50, 200),
		TrailingEntries:      pick[uint64](r, 0, 0, 5, 20),
	}
	switch s.Profile {
	case "calm":
		s.Lockstep = r.IntN(2) == 0
	case "lossy":
		s.Lockstep = r.IntN(2) == 0
		s.DropRate = rate(r, 0.05, 0.3)
		s.DupRate = rate(r, 0, 0.2)
		s.MaxDelay = between(r, 5, 30) * Millisecond
		s.LongDelayRate = rate(r, 0, 0.05)
		s.LongDelayMax = 2 * Second
	case "partition":
		s.Lockstep = r.IntN(4) == 0
		s.DropRate = rate(r, 0, 0.05)
		s.MaxDelay = between(r, 5, 20) * Millisecond
		s.PartitionGap = between(r, 100, 800) * Millisecond
		s.PartitionLen = between(r, 100, 1500) * Millisecond
		s.OneWayRate = rate(r, 0, 0.5)
	case "crash":
		s.Lockstep = r.IntN(4) == 0
		s.MaxDelay = between(r, 5, 20) * Millisecond
		s.CrashGap = between(r, 100, 600) * Millisecond
		s.MaxDowntime = between(r, 50, 1000) * Millisecond
		s.CrashDuringSave = rate(r, 0.001, 0.02)
		s.CrashAfterSave = rate(r, 0.001, 0.02)
	case "chaos":
		s.Lockstep = r.IntN(4) == 0
		s.DropRate = rate(r, 0.01, 0.2)
		s.DupRate = rate(r, 0, 0.15)
		s.MaxDelay = between(r, 5, 30) * Millisecond
		s.LongDelayRate = rate(r, 0, 0.05)
		s.LongDelayMax = 3 * Second
		s.PartitionGap = between(r, 100, 800) * Millisecond
		s.PartitionLen = between(r, 100, 1000) * Millisecond
		s.OneWayRate = rate(r, 0, 0.5)
		s.CrashGap = between(r, 100, 800) * Millisecond
		s.MaxDowntime = between(r, 50, 1000) * Millisecond
		s.CrashDuringSave = rate(r, 0, 0.02)
		s.CrashAfterSave = rate(r, 0, 0.02)
	case "figure8":
		s.Nodes = 5
		s.MaxEntriesPerMessage = pick(r, 1, 1, 2)
		s.DropRate = rate(r, 0, 0.1)
		s.MaxDelay = between(r, 5, 25) * Millisecond
		s.MaxDowntime = between(r, 50, 400) * Millisecond
		s.LeaderHunt = between(r, 10, 100) * Millisecond
		s.CrashDuringSave = rate(r, 0, 0.01)
		s.CrashAfterSave = rate(r, 0, 0.01)
		s.ChaosLen = between(r, 6, 15) * Second
		// Busy clients that find the new leader fast, so each leader takes commands
		// before it is crashed, and a log kept whole, so old entries stay in play.
		s.Clients = 5
		s.ReadRate = 0.05
		s.ThinkTime = between(r, 1, 5) * Millisecond
		s.ClientTimeout = between(r, 20, 60) * Millisecond
		s.SnapshotEvery = 0
	case "solo":
		s.Nodes = 1
		s.CrashDuringSave = rate(r, 0.01, 0.1)
		s.CrashAfterSave = rate(r, 0.01, 0.1)
		s.CrashGap = between(r, 200, 800) * Millisecond
	case "snapshot":
		// Frequent compaction, and long enough cuts that a node falls behind what
		// the leader still holds, so catching up takes a snapshot.
		s.DropRate = rate(r, 0, 0.1)
		s.DupRate = rate(r, 0, 0.1)
		s.MaxDelay = between(r, 5, 20) * Millisecond
		s.LongDelayRate = rate(r, 0, 0.02)
		s.LongDelayMax = 2 * Second
		s.PartitionGap = between(r, 100, 500) * Millisecond
		s.PartitionLen = between(r, 500, 2000) * Millisecond
		s.CrashGap = between(r, 300, 1000) * Millisecond
		s.MaxDowntime = between(r, 200, 1500) * Millisecond
		s.CrashDuringSave = rate(r, 0, 0.01)
		s.CrashAfterSave = rate(r, 0, 0.01)
		s.SnapshotEvery = pick[uint64](r, 3, 5, 10, 20)
		s.TrailingEntries = pick[uint64](r, 0, 0, 2)
	}
	return s
}
