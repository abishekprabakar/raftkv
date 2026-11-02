// Package sim runs real raft.Nodes on a simulated network with a virtual clock,
// in the style of the MIT 6.5840 Raft tests but without any real time.
//
// Everything happens in one goroutine, driven by a queue of timed events, and
// every random choice comes from one generator seeded by the schedule. So a seed
// fixes the whole run, and a failure at seed N replays exactly:
//
//	go run ./cmd/raftsim -first N -count 1 -trace
//
// A schedule has two parts. In the chaos phase the network drops, delays,
// duplicates and reorders messages, cuts links and crashes nodes while clients
// send commands. In the quiet phase all of that stops, and the cluster has to
// elect a leader and bring every node up to date. Safety is checked throughout,
// after every event, and liveness is checked in the quiet phase. The checks are
// in checker.go and host.go, and docs/acceptance/M2.md lists them.
package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

const (
	crashDuringSave = "during-save"
	crashAfterSave  = "after-save"
	crashBetween    = "between-events"

	quietCheckGap = 20 * Millisecond
	quietMaxDelay = 5 * Millisecond
	// quietStall is how long a healed cluster may go without any node applying
	// anything. Catching up a long log can take a while, so liveness is judged
	// by whether progress stops, not by how long the whole thing takes. This is
	// 25 to 50 election timeouts, far more than a working election needs.
	quietStall = 5 * Second
	// quietLimit bounds the quiet phase, so every schedule ends. A cluster can
	// keep applying forever while one node never catches up, and that is a
	// liveness failure as well, one that quietStall alone would not notice.
	quietLimit = 60 * Second
)

// Options change how a schedule is judged or reported, never what happens in it.
type Options struct {
	// PropertiesOnly switches the Figure 2 monitors off, leaving the five
	// properties, to ask whether a broken rule causes visible harm.
	PropertiesOnly bool
	// Trace, if set, receives a line per event.
	Trace io.Writer
}

// Stats counts what actually happened in a run, as proof the faults were real.
type Stats struct {
	Events            int64 `json:"events"`
	VirtualMs         int64 `json:"virtual_ms"`
	Sent              int64 `json:"messages_sent"`
	Delivered         int64 `json:"messages_delivered"`
	Dropped           int64 `json:"messages_dropped"`
	DroppedByCut      int64 `json:"messages_blocked_by_partition"`
	DroppedAtDown     int64 `json:"messages_to_crashed_nodes"`
	Duplicated        int64 `json:"messages_duplicated"`
	LongDelayed       int64 `json:"messages_delayed_past_election_timeout"`
	Reordered         int64 `json:"messages_reordered"`
	Partitions        int64 `json:"partitions"`
	OneWayCuts        int64 `json:"one_way_cuts"`
	Crashes           int64 `json:"crashes"`
	CrashesBetween    int64 `json:"crashes_between_events"`
	CrashesDuringSave int64 `json:"crashes_during_save"`
	CrashesAfterSave  int64 `json:"crashes_after_save"`
	Restarts          int64 `json:"restarts"`
	Proposed          int64 `json:"commands_proposed"`
	LeadersElected    int64 `json:"leaders_elected"`
	Committed         int64 `json:"entries_committed"`
}

func (s *Stats) add(o Stats) {
	s.Events += o.Events
	s.VirtualMs += o.VirtualMs
	s.Sent += o.Sent
	s.Delivered += o.Delivered
	s.Dropped += o.Dropped
	s.DroppedByCut += o.DroppedByCut
	s.DroppedAtDown += o.DroppedAtDown
	s.Duplicated += o.Duplicated
	s.LongDelayed += o.LongDelayed
	s.Reordered += o.Reordered
	s.Partitions += o.Partitions
	s.OneWayCuts += o.OneWayCuts
	s.Crashes += o.Crashes
	s.CrashesBetween += o.CrashesBetween
	s.CrashesDuringSave += o.CrashesDuringSave
	s.CrashesAfterSave += o.CrashesAfterSave
	s.Restarts += o.Restarts
	s.Proposed += o.Proposed
	s.LeadersElected += o.LeadersElected
	s.Committed += o.Committed
}

// Result is the outcome of one schedule.
type Result struct {
	Seed      uint64
	Profile   string
	TraceHash uint64
	Stats     Stats
	Violation *Violation
}

type eventKind int

const (
	evTick eventKind = iota
	evDeliver
	evPartition
	evHeal
	evRandomCrash
	evCrashNode
	evRestart
	evPropose
	evQuiet
	evQuietCheck
)

// event is one thing due to happen. seq breaks ties, so events at the same
// instant run in the order they were scheduled, on every machine.
type event struct {
	at          int64
	seq         uint64
	kind        eventKind
	host        *host
	incarnation int
	from        raft.NodeID
	msg         raft.Message
	linkSeq     uint64
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// Sim is one running schedule.
type Sim struct {
	sched Schedule
	opts  Options
	rnd   *rand.Rand
	check *checker
	stats Stats

	now    int64
	seq    uint64
	events eventHeap
	hosts  []*host
	ids    []raft.NodeID
	links  network

	quiet         bool
	quietStart    int64
	lastProgress  int64
	lastApplied   uint64
	sentinel      string
	sentinelCount int
	done          bool
	commands      int
	trace         uint64
}

// Run executes the schedule for a seed.
func Run(seed uint64, opts Options) Result {
	return RunSchedule(NewSchedule(seed), opts)
}

// RunSchedule executes one schedule. A Node panic is a failed check, not a
// crashed test run, so a batch of seeds always finishes and reports.
func RunSchedule(sched Schedule, opts Options) (res Result) {
	s := &Sim{
		sched: sched, opts: opts,
		rnd:   rand.New(rand.NewPCG(sched.Seed, 0xA11CE)),
		links: newNetwork(),
	}
	s.check = newChecker(&s.now, sched.Nodes, opts.PropertiesOnly)
	defer func() {
		if r := recover(); r != nil {
			s.check.violation = &Violation{Check: CheckPanic, AtUs: s.now, Detail: fmt.Sprint(r)}
		}
		s.stats.VirtualMs = s.now / Millisecond
		s.stats.Committed = int64(len(s.check.committed))
		res = Result{Seed: sched.Seed, Profile: sched.Profile, TraceHash: s.trace, Stats: s.stats, Violation: s.check.violation}
	}()
	s.start()
	s.loop()
	return res
}

func (s *Sim) schedule(after int64, e event) {
	s.seq++
	e.at, e.seq = s.now+after, s.seq
	heap.Push(&s.events, &e)
}

func (s *Sim) chance(p float64) bool {
	return !s.quiet && p > 0 && s.rnd.Float64() < p
}

func (s *Sim) tracef(format string, args ...any) {
	if s.opts.Trace != nil {
		fmt.Fprintf(s.opts.Trace, "%9.3fms ", float64(s.now)/float64(Millisecond))
		fmt.Fprintf(s.opts.Trace, format+"\n", args...)
	}
}

func (s *Sim) start() {
	for i := 1; i <= s.sched.Nodes; i++ {
		s.ids = append(s.ids, raft.NodeID(i))
	}
	for _, id := range s.ids {
		h := &host{s: s, id: id}
		s.hosts = append(s.hosts, h)
		s.startNode(h)
	}
	if s.sched.PartitionGap > 0 {
		s.schedule(between(s.rnd, 0, s.sched.PartitionGap), event{kind: evPartition})
	}
	if s.sched.CrashGap > 0 {
		s.schedule(between(s.rnd, 0, s.sched.CrashGap), event{kind: evRandomCrash})
	}
	s.schedule(between(s.rnd, 0, s.sched.ProposeGap), event{kind: evPropose})
	s.schedule(s.sched.ChaosLen, event{kind: evQuiet})
}

func (s *Sim) loop() {
	for s.check.violation == nil && !s.done {
		e := heap.Pop(&s.events).(*event)
		s.now = e.at
		s.stats.Events++
		s.handle(e)
		s.trace = mix(mix(mix(s.trace, uint64(e.at)), uint64(e.kind)), s.outcomeHash())
	}
}

// outcomeHash folds every node's visible state into the trace, so two runs match
// only if they agreed on the state of the cluster, not merely on event timing.
func (s *Sim) outcomeHash() uint64 {
	var h uint64
	for _, x := range s.hosts {
		if x.node == nil {
			h = mix(h, 0)
			continue
		}
		h = mix(h, x.status.Term)
		h = mix(h, x.status.CommitIndex)
		h = mix(h, x.status.LastLogIndex)
		h = mix(h, uint64(x.status.Role))
	}
	return h
}

func (s *Sim) handle(e *event) {
	switch e.kind {
	case evTick:
		h := e.host
		if h.node == nil || h.incarnation != e.incarnation {
			return // a tick left over from before a crash
		}
		s.call(h, h.node.Tick)
		if h.incarnation == e.incarnation {
			s.schedule(s.nextTick(), event{kind: evTick, host: h, incarnation: h.incarnation})
		}
	case evDeliver:
		s.deliver(e)
	case evPartition:
		if !s.quiet {
			s.startPartition()
		}
	case evHeal:
		if !s.quiet {
			s.links.heal()
			s.tracef("heal")
			s.schedule(between(s.rnd, 1, s.sched.PartitionGap), event{kind: evPartition})
		}
	case evRandomCrash:
		if !s.quiet {
			s.crash(s.hosts[s.rnd.IntN(len(s.hosts))], crashBetween)
			s.schedule(between(s.rnd, 1, s.sched.CrashGap), event{kind: evRandomCrash})
		}
	case evCrashNode:
		if !s.quiet {
			s.crash(e.host, crashBetween)
		}
	case evRestart:
		if e.host.node == nil {
			s.startNode(e.host)
		}
	case evPropose:
		if !s.quiet {
			s.propose()
			s.schedule(between(s.rnd, 1, s.sched.ProposeGap), event{kind: evPropose})
		}
	case evQuiet:
		s.beginQuiet()
	case evQuietCheck:
		s.quietCheck()
	}
}

// call makes one input to a node, then settles what it caused.
func (s *Sim) call(h *host, input func() error) {
	err := input()
	if err != nil && !errors.Is(err, errCrash) {
		panic(fmt.Sprintf("node %d: %v", h.id, err))
	}
	if h.dying {
		s.crash(h, h.crashKind)
		return
	}
	h.afterStep()
}

func (s *Sim) nextTick() int64 {
	if s.sched.Lockstep {
		return TickLength
	}
	return between(s.rnd, TickLength-TickLength/10, TickLength+TickLength/10)
}

func (s *Sim) startNode(h *host) {
	h.incarnation++
	h.dying, h.status, h.seenCommit, h.applied, h.seenLeader, h.sawSentinel = false, raft.Status{}, 0, 0, 0, false
	n, err := raft.NewNode(raft.Config{
		ID: h.id, Peers: s.ids, Seed: s.rnd.Uint64(),
		ElectionTicks: s.sched.ElectionTicks, HeartbeatTicks: s.sched.HeartbeatTicks,
		MaxEntriesPerMessage: s.sched.MaxEntriesPerMessage,
		Storage:              h, Transport: h, StateMachine: h,
	})
	if err != nil {
		panic(err)
	}
	h.node = n
	h.status = n.Status()
	if s.now > 0 {
		s.stats.Restarts++
		s.tracef("restart node %d term %d log %d", h.id, h.status.Term, h.status.LastLogIndex)
	}
	// Lockstep clocks stay on the shared grid even after a restart.
	first := TickLength - s.now%TickLength
	if !s.sched.Lockstep {
		first = between(s.rnd, 0, TickLength)
	}
	s.schedule(first, event{kind: evTick, host: h, incarnation: h.incarnation})
}

// crash ends a node's life. Its disk stays. Messages already in flight still
// arrive at other nodes, as they would on a real network.
func (s *Sim) crash(h *host, kind string) {
	if h.node == nil {
		return
	}
	h.node, h.dying = nil, false
	h.incarnation++
	s.stats.Crashes++
	switch kind {
	case crashBetween:
		s.stats.CrashesBetween++
	case crashDuringSave:
		s.stats.CrashesDuringSave++
	case crashAfterSave:
		s.stats.CrashesAfterSave++
	}
	s.tracef("crash node %d (%s)", h.id, kind)
	if !s.quiet {
		s.schedule(between(s.rnd, 1, s.sched.MaxDowntime), event{kind: evRestart, host: h})
	}
}

// propose sends a command to a node that believes it leads. Stale leaders are
// fair game, and their commands are expected to be lost.
func (s *Sim) propose() {
	var leaders []*host
	for _, h := range s.hosts {
		if h.node != nil && h.status.Role == raft.Leader {
			leaders = append(leaders, h)
		}
	}
	if len(leaders) == 0 {
		return
	}
	h := leaders[s.rnd.IntN(len(leaders))]
	s.commands++
	data := []byte(fmt.Sprintf("s%d-c%d", s.sched.Seed, s.commands))
	s.call(h, func() error {
		_, _, err := h.node.Propose(data)
		return err
	})
	s.stats.Proposed++
	s.tracef("propose %s at node %d", data, h.id)
	if s.sched.LeaderHunt > 0 && h.node != nil {
		s.schedule(between(s.rnd, 1, s.sched.LeaderHunt), event{kind: evCrashNode, host: h})
	}
}

// beginQuiet stops all faults and brings every node back.
func (s *Sim) beginQuiet() {
	s.quiet = true
	s.quietStart, s.lastProgress = s.now, s.now
	s.links.heal()
	s.tracef("quiet phase begins")
	for _, h := range s.hosts {
		if h.node == nil {
			s.startNode(h)
		}
	}
	s.schedule(quietCheckGap, event{kind: evQuietCheck})
}

// quietCheck is the liveness test: a leader must emerge, commit a new command,
// and every node must apply it. A new sentinel replaces one that a leader
// change lost, so only a cluster that truly can't make progress runs out of
// budget.
func (s *Sim) quietCheck() {
	all := true
	var applied uint64
	for _, h := range s.hosts {
		all = all && h.sawSentinel
		applied += h.status.LastApplied
	}
	if all && s.sentinel != "" {
		s.done = true
		return
	}
	if applied > s.lastApplied {
		s.lastApplied, s.lastProgress = applied, s.now
	}
	if s.now-s.lastProgress >= quietStall {
		s.check.fail(CheckLiveness, 0, "nothing applied for %dms of quiet: %s", quietStall/Millisecond, s.describe())
		return
	}
	if s.now-s.quietStart >= quietLimit {
		s.check.fail(CheckLiveness, 0, "not every node caught up in %dms of quiet: %s", quietLimit/Millisecond, s.describe())
		return
	}
	if s.sentinel == "" || s.sentinelCount%50 == 0 {
		var leader *host
		for _, h := range s.hosts {
			if h.status.Role == raft.Leader && (leader == nil || h.status.Term > leader.status.Term) {
				leader = h
			}
		}
		if leader != nil {
			s.commands++
			s.sentinel = fmt.Sprintf("s%d-sentinel-%d", s.sched.Seed, s.commands)
			for _, h := range s.hosts {
				h.sawSentinel = false
			}
			data := []byte(s.sentinel)
			s.call(leader, func() error {
				_, _, err := leader.node.Propose(data)
				return err
			})
		}
	}
	s.sentinelCount++
	s.schedule(quietCheckGap, event{kind: evQuietCheck})
}

func (s *Sim) describe() string {
	out := ""
	for _, h := range s.hosts {
		st := h.status
		out += fmt.Sprintf("[%d %s term %d commit %d log %d] ", h.id, st.Role, st.Term, st.CommitIndex, st.LastLogIndex)
	}
	return out
}

// copiesOnDisk counts the disks holding the entry with this index and term.
func (s *Sim) copiesOnDisk(index, term uint64) int {
	n := 0
	for _, h := range s.hosts {
		if h.disk.term(index) == term {
			n++
		}
	}
	return n
}
