package chaos

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// Every schedule keeps to the shape fixed in docs/acceptance/M5.md, and the same
// seed always gives the same schedule.
func TestFaultsFollowTheSpec(t *testing.T) {
	kinds := map[FaultKind]int{}
	for seed := uint64(1); seed <= 1000; seed++ {
		faults := Faults(seed)
		if !reflect.DeepEqual(faults, Faults(seed)) {
			t.Fatalf("seed %d: two calls gave different schedules", seed)
		}
		if len(faults) == 0 {
			t.Fatalf("seed %d: no faults", seed)
		}
		end := time.Duration(0)
		for i, f := range faults {
			kinds[f.Kind]++
			switch {
			case i == 0 && f.At != faultsFrom:
				t.Fatalf("seed %d: first fault at %v", seed, f.At)
			case i > 0 && (f.At-end < healMin || f.At-end > healMax):
				t.Fatalf("seed %d: heal of %v before fault %d", seed, f.At-end, i)
			case f.Hold < partitionMin || f.Hold > partitionMax:
				t.Fatalf("seed %d: fault %d held %v", seed, i, f.Hold)
			case f.At+f.Hold > faultsUntil:
				t.Fatalf("seed %d: fault %d ends at %v", seed, i, f.At+f.Hold)
			}
			end = f.At + f.Hold
		}
	}
	t.Logf("fault kinds over seeds 1 to 1000: %v", kinds)
}

func TestWorkloadIsSeededAndValuesAreUnique(t *testing.T) {
	a, b := NewWorkload(7, 2), NewWorkload(7, 2)
	other := NewWorkload(7, 3)
	seen := map[string]bool{}
	differs := false
	for range 1000 {
		x, px := a.Next()
		y, py := b.Next()
		z, _ := other.Next()
		if x != y || px != py {
			t.Fatalf("same seed and client, different operations: %+v %v, %+v %v", x, px, y, py)
		}
		if px < 0 || px > maxThink {
			t.Fatalf("pause %v", px)
		}
		if x.Kind != Get {
			if seen[x.Value] {
				t.Fatalf("value %q written twice", x.Value)
			}
			seen[x.Value] = true
		}
		differs = differs || x.Key != z.Key || x.Kind != z.Kind
	}
	if !differs {
		t.Fatal("two clients drew identical operations")
	}
}

// Failover is measured from Docker's disconnect event to the first other node to
// log itself leader in a higher term, before the heal.
func TestFailoverFromLogs(t *testing.T) {
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	at := func(ms int) time.Time { return base.Add(time.Duration(ms) * time.Millisecond) }
	ids := [Nodes + 1]string{"", "c1", "c2", "c3"}
	event := func(ms int, action, container string) string {
		return fmt.Sprintf(`{"Type":"network","Action":%q,"Actor":{"Attributes":{"container":%q,"name":%q}},"timeNano":%d}`,
			action, container, network, at(ms).UnixNano())
	}
	raw := []byte(event(1000, "disconnect", "c1") + "\n" + event(5000, "connect", "c1") + "\n" +
		event(7000, "disconnect", "c2") + "\n" + event(9000, "connect", "c2") + "\n" +
		`{"Type":"network","Action":"disconnect","Actor":{"Attributes":{"container":"c3","name":"other"}},"timeNano":1}` + "\n")
	logLine := func(ms, node int, role string, term int) string {
		return fmt.Sprintf(`node%d-1  | time=%s level=INFO msg="raft state" node=%d role=%s term=%d leader=0`,
			node, at(ms).Format(time.RFC3339Nano), node, role, term)
	}
	logs := []byte(logLine(0, 1, "leader", 2) + "\n" +
		logLine(1500, 2, "candidate", 3) + "\n" +
		logLine(1700, 1, "leader", 2) + "\n" + // the cut-off leader repeating itself doesn't count
		logLine(1750, 3, "leader", 3) + "\n" +
		logLine(6500, 2, "leader", 4) + "\n" + "unrelated line\n")

	faults := []FaultRecord{
		{Kind: IsolateLeader, Targets: []int{1}},
		{Kind: IsolateLeader, Targets: []int{2}}, // no one takes over before the heal at 9000
	}
	got := failovers(faults, parseEvents(raw, ids), parseStates(logs))
	want := []Failover{
		{Isolated: 1, OldTerm: 2, NewLeader: 3, NewTerm: 3, Ms: 750},
		{Isolated: 2, OldTerm: 4, Note: "no new leader before the heal"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestPercentileIsNearestRank(t *testing.T) {
	v := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	for p, want := range map[float64]float64{50: 50, 95: 100, 90: 90, 10: 10, 1: 10} {
		if got := percentile(v, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
}
