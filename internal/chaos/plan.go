package chaos

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// The shape of every schedule, fixed in docs/acceptance/M5.md.
const (
	Nodes            = 3
	ClientsPerNode   = 2
	Keys             = 8
	WorkloadTime     = 20 * time.Second
	faultsFrom       = 2 * time.Second
	faultsUntil      = 18 * time.Second
	OpTimeout        = 4 * time.Second
	AttemptTimeout   = time.Second
	maxThink         = 40 * time.Millisecond
	settleTimeout    = 20 * time.Second
	porcupineTimeout = 120 * time.Second
	partitionMin     = 2 * time.Second
	partitionMax     = 6 * time.Second
	healMin          = 1 * time.Second
	healMax          = 3 * time.Second
)

// FaultKind is which nodes a partition cuts off.
type FaultKind string

const (
	IsolateLeader   FaultKind = "leader"
	IsolateFollower FaultKind = "follower"
	IsolateTwo      FaultKind = "two"
)

// Fault is one partition: the nodes it picks are cut off from the network at At,
// and reconnected at At+Hold.
type Fault struct {
	Kind FaultKind     `json:"kind"`
	At   time.Duration `json:"at"`
	Hold time.Duration `json:"hold"`
	// Pick chooses among the candidates once the leader is known: which follower,
	// or which two nodes. Drawing it now keeps every choice in the seed.
	Pick int `json:"pick"`
}

// Faults draws a schedule's partitions from its seed. From 2 s to 18 s, partitions
// alternate with heals: each partition lasts 2 to 6 s and each heal 1 to 3 s.
func Faults(seed uint64) []Fault {
	rng := rand.New(rand.NewPCG(seed, 0))
	var faults []Fault
	for at := faultsFrom; ; {
		hold := between(rng, partitionMin, partitionMax)
		if at+hold > faultsUntil {
			return faults
		}
		var kind FaultKind
		switch r := rng.Float64(); {
		case r < 0.5:
			kind = IsolateLeader
		case r < 0.8:
			kind = IsolateFollower
		default:
			kind = IsolateTwo
		}
		faults = append(faults, Fault{Kind: kind, At: at, Hold: hold, Pick: rng.IntN(1 << 30)})
		at += hold + between(rng, healMin, healMax)
	}
}

func between(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	return lo + time.Duration(rng.Int64N(int64(hi-lo)+1))
}

// Workload draws one client's operations from the seed: get 40%, append 50%, put
// 10%, on 8 keys, with 0 to 40 ms between operations. Every value written is
// unique, so a lost or duplicated append can't hide.
type Workload struct {
	client int
	rng    *rand.Rand
	n      int
}

func NewWorkload(seed uint64, client int) *Workload {
	return &Workload{client: client, rng: rand.New(rand.NewPCG(seed, uint64(client+1)))}
}

// Next returns the next operation and how long to pause before it.
func (w *Workload) Next() (Input, time.Duration) {
	w.n++
	in := Input{Key: fmt.Sprintf("k%d", w.rng.IntN(Keys))}
	switch r := w.rng.Float64(); {
	case r < 0.4:
		in.Kind = Get
	case r < 0.9:
		in.Kind = Append
	default:
		in.Kind = Put
	}
	if in.Kind != Get {
		in.Value = fmt.Sprintf("c%d.%d;", w.client, w.n)
	}
	return in, time.Duration(w.rng.Int64N(int64(maxThink) + 1))
}
