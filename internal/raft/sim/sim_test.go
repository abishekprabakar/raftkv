package sim

import (
	"strings"
	"testing"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// devSeeds are for tests that should not touch the acceptance ranges.
const devSeeds uint64 = 900_000_000

// TestSimSchedules is the cluster-level safety test: seeds 1 to 2,000 must all
// pass every check. To replay a failure:
//
//	go run ./cmd/raftsim -first SEED -count 1 -trace
func TestSimSchedules(t *testing.T) {
	const schedules = 2000
	if testing.Short() {
		t.Skip("takes several seconds")
	}
	results := RunRange(1, schedules, 0, Options{})
	sum := Summarize(results)
	t.Logf("%d schedules, %d events, %d crashes, %d partitions", sum.Schedules, sum.Totals.Events, sum.Totals.Crashes, sum.Totals.Partitions)
	shown := 0
	for _, r := range results {
		if r.Violation != nil && shown < 5 {
			shown++
			t.Errorf("seed %d (%s): %s at %dus on node %d: %s", r.Seed, r.Profile, r.Violation.Check, r.Violation.AtUs, r.Violation.Node, r.Violation.Detail)
		}
	}
	if sum.Violations > 0 {
		t.Fatalf("%d of %d schedules failed: %+v", sum.Violations, schedules, sum.ByCheck)
	}
}

func TestSameSeedSameRun(t *testing.T) {
	for seed := devSeeds; seed < devSeeds+200; seed++ {
		a, b := Run(seed, Options{}), Run(seed, Options{})
		if a.TraceHash != b.TraceHash || a.Stats != b.Stats {
			t.Fatalf("seed %d ran differently: %x %+v, then %x %+v", seed, a.TraceHash, a.Stats, b.TraceHash, b.Stats)
		}
		if a.TraceHash == 0 || a.Stats.Events == 0 {
			t.Fatalf("seed %d did nothing", seed)
		}
	}
}

func TestDifferentSeedsRunDifferently(t *testing.T) {
	seen := map[uint64]uint64{}
	for seed := devSeeds; seed < devSeeds+200; seed++ {
		r := Run(seed, Options{})
		if other, ok := seen[r.TraceHash]; ok {
			t.Fatalf("seeds %d and %d gave the same trace", other, seed)
		}
		seen[r.TraceHash] = seed
	}
}

func TestWorkerCountDoesNotChangeResults(t *testing.T) {
	one := RunRange(1, 200, 1, Options{})
	eight := RunRange(1, 200, 8, Options{})
	for i := range one {
		if one[i].TraceHash != eight[i].TraceHash {
			t.Fatalf("seed %d differs with 8 workers", one[i].Seed)
		}
	}
}

// TestOptionsDoNotChangeTheRun makes sure judging a schedule differently cannot
// change what happens in it, which is what lets a monitors-off run be compared
// with a normal one seed for seed.
func TestOptionsDoNotChangeTheRun(t *testing.T) {
	for seed := devSeeds; seed < devSeeds+50; seed++ {
		if a, b := Run(seed, Options{}), Run(seed, Options{PropertiesOnly: true}); a.TraceHash != b.TraceHash {
			t.Fatalf("seed %d: monitors changed the run", seed)
		}
	}
}

func TestEveryProfileRuns(t *testing.T) {
	for i := range Profiles {
		s := NewSchedule(devSeeds - devSeeds%uint64(len(Profiles)) + uint64(i))
		if s.Profile != Profiles[i] {
			t.Fatalf("seed for profile %d gave %s", i, s.Profile)
		}
	}
}

// TestProfilesProduceTheirFaults stops a profile from quietly doing nothing. A
// schedule that never drops a message would still pass every safety check.
func TestProfilesProduceTheirFaults(t *testing.T) {
	sum := map[string]*Stats{}
	for _, p := range Profiles {
		sum[p] = &Stats{}
	}
	for _, r := range RunRange(devSeeds, 7*30, 0, Options{}) {
		sum[r.Profile].add(r.Stats)
	}
	if s := sum["calm"]; s.Dropped+s.Duplicated+s.Crashes+s.Partitions != 0 {
		t.Fatalf("calm is meant to have no faults: %+v", s)
	}
	checks := []struct {
		profile string
		name    string
		got     func(*Stats) int64
	}{
		{"lossy", "drops", func(s *Stats) int64 { return s.Dropped }},
		{"lossy", "duplicates", func(s *Stats) int64 { return s.Duplicated }},
		{"lossy", "long delays", func(s *Stats) int64 { return s.LongDelayed }},
		{"lossy", "reorders", func(s *Stats) int64 { return s.Reordered }},
		{"partition", "partitions", func(s *Stats) int64 { return s.Partitions }},
		{"partition", "one-way cuts", func(s *Stats) int64 { return s.OneWayCuts }},
		{"partition", "blocked messages", func(s *Stats) int64 { return s.DroppedByCut }},
		{"crash", "crashes between events", func(s *Stats) int64 { return s.CrashesBetween }},
		{"crash", "crashes during save", func(s *Stats) int64 { return s.CrashesDuringSave }},
		{"crash", "crashes after save", func(s *Stats) int64 { return s.CrashesAfterSave }},
		{"crash", "restarts", func(s *Stats) int64 { return s.Restarts }},
		{"figure8", "crashes", func(s *Stats) int64 { return s.Crashes }},
		{"solo", "crashes during save", func(s *Stats) int64 { return s.CrashesDuringSave }},
	}
	for _, c := range checks {
		if c.got(sum[c.profile]) == 0 {
			t.Errorf("profile %s produced no %s: %+v", c.profile, c.name, sum[c.profile])
		}
	}
}

// ---- the checks themselves ----------------------------------------------------

// Each property below is fed the exact evidence that should trip it. Without
// these, a check with a bug in it would look the same as a protocol with no bugs.

func newTestChecker(t *testing.T) *checker {
	t.Helper()
	var now int64
	return newChecker(&now, 3, false)
}

func wantViolation(t *testing.T, c *checker, check string) {
	t.Helper()
	if c.violation == nil || c.violation.Check != check {
		t.Fatalf("got %+v, want a %s violation", c.violation, check)
	}
}

func TestElectionSafetyCheck(t *testing.T) {
	c := newTestChecker(t)
	c.sawLeader(1, 5)
	c.sawLeader(1, 5)
	c.sawLeader(2, 6)
	if c.violation != nil {
		t.Fatalf("no violation yet, got %+v", c.violation)
	}
	c.sawLeader(3, 5)
	wantViolation(t, c, CheckElectionSafety)
}

func TestLogMatchingCheck(t *testing.T) {
	c := newTestChecker(t)
	c.sawEntry(1, raft.Entry{Index: 2, Term: 3}, 111)
	c.sawEntry(2, raft.Entry{Index: 2, Term: 3}, 111)
	c.sawEntry(2, raft.Entry{Index: 2, Term: 4}, 222)
	if c.violation != nil {
		t.Fatalf("no violation yet, got %+v", c.violation)
	}
	c.sawEntry(3, raft.Entry{Index: 2, Term: 3}, 999)
	wantViolation(t, c, CheckLogMatching)
}

func TestStateMachineSafetyCheck(t *testing.T) {
	c := newTestChecker(t)
	c.sawApply(1, raft.Entry{Index: 1, Term: 1, Data: []byte("a")})
	c.sawApply(2, raft.Entry{Index: 1, Term: 1, Data: []byte("a")})
	if c.violation != nil {
		t.Fatalf("no violation yet, got %+v", c.violation)
	}
	c.sawApply(3, raft.Entry{Index: 1, Term: 1, Data: []byte("b")})
	wantViolation(t, c, CheckStateMachineSafety)
}

func TestLeaderCompletenessCheck(t *testing.T) {
	c := newTestChecker(t)
	c.sawCommit(1, 1, 4, 100, 4, 2)
	c.sawCommit(1, 2, 4, 200, 4, 2)
	full := func(uint64) uint64 { return 4 }
	missing := func(i uint64) uint64 {
		if i == 2 {
			return 0
		}
		return 4
	}
	c.checkLeaderHolds(2, 5, full)
	c.checkLeaderHolds(2, 3, missing) // an older leader owes nothing
	if c.violation != nil {
		t.Fatalf("no violation yet, got %+v", c.violation)
	}
	c.checkLeaderHolds(2, 5, missing)
	wantViolation(t, c, CheckLeaderCompleteness)
}

func TestConflictingCommitIsCaught(t *testing.T) {
	c := newTestChecker(t)
	c.sawCommit(1, 1, 4, 100, 4, 2)
	c.sawCommit(2, 1, 5, 100, 5, 2)
	wantViolation(t, c, CheckStateMachineSafety)
}

func TestCommitMajorityCheck(t *testing.T) {
	c := newTestChecker(t)
	c.sawCommit(1, 1, 4, 100, 4, 1)
	wantViolation(t, c, CheckCommitMajority)
}

func TestVoteOnceCheck(t *testing.T) {
	c := newTestChecker(t)
	c.sawGrant(1, 2, 7)
	c.sawGrant(1, 2, 7)
	c.sawGrant(1, 3, 8)
	if c.violation != nil {
		t.Fatalf("no violation yet, got %+v", c.violation)
	}
	c.sawGrant(1, 3, 7)
	wantViolation(t, c, CheckVoteOnce)
}

func TestMonitorsCanBeSwitchedOff(t *testing.T) {
	var now int64
	c := newChecker(&now, 3, true)
	c.sawGrant(1, 2, 7)
	c.sawGrant(1, 3, 7)
	c.sawCommit(1, 1, 4, 100, 4, 1)
	if c.violation != nil {
		t.Fatalf("a monitor fired with monitors off: %+v", c.violation)
	}
	c.sawLeader(1, 5)
	c.sawLeader(2, 5)
	wantViolation(t, c, CheckElectionSafety)
}

// ---- crash points -------------------------------------------------------------

// A crash during Save must lose the update, and nothing may leave the node. With
// every Save crashing, no message can be sent until the quiet phase stops them.
func TestCrashDuringSaveSendsNothing(t *testing.T) {
	sched := NewSchedule(devSeeds)
	sched.CrashDuringSave = 1
	sched.CrashAfterSave = 0
	var trace strings.Builder
	r := RunSchedule(sched, Options{Trace: &trace})
	if r.Violation != nil {
		t.Fatalf("%+v", r.Violation)
	}
	if r.Stats.CrashesDuringSave == 0 {
		t.Fatalf("no crash during save: %+v", r.Stats)
	}
	chaos, _, found := strings.Cut(trace.String(), "quiet phase begins")
	if !found {
		t.Fatal("no quiet phase")
	}
	if strings.Contains(chaos, "deliver") {
		t.Fatal("a message left a node whose save was lost")
	}
}

// Regression: replying to every reply with a send once produced 1,967,759
// messages in one 5-node schedule that needed a few thousand. The bound allows
// heartbeats, one send per proposal and one catch-up send per entry, with room
// to spare, in each direction of each link.
func TestNoMessageStorm(t *testing.T) {
	for _, r := range RunRange(devSeeds, 7*30, 0, Options{}) {
		nodes := NewSchedule(r.Seed).Nodes
		limit := int64(2000*(nodes-1)) * max(r.Stats.VirtualMs, 1000) / 1000
		if r.Stats.Sent > limit {
			t.Fatalf("seed %d (%s): %d messages in %dms, limit %d", r.Seed, r.Profile, r.Stats.Sent, r.Stats.VirtualMs, limit)
		}
	}
}
