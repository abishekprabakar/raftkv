package kube

import (
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/abishekprabakar/raftkv/internal/chaos"
)

// history builds a short run on one key: appends from two clients, then the
// final read.
func history(final string, ops ...Op) []Op {
	at := int64(0)
	for i := range ops {
		ops[i].Key = "k"
		ops[i].Call, ops[i].Return = at, at+1
		at += 2
	}
	return append(ops, Op{Op: chaos.Op{Kind: chaos.Get, Key: "k", Value: final, OK: true, Call: at, Return: at + 1}, Final: true})
}

func write(client int, value string, ok bool) Op {
	return Op{Op: chaos.Op{Client: client, Kind: chaos.Append, Value: value, OK: ok}}
}

// P1. The durability counters can fail: they report a lost acknowledged write
// and a duplicated write, and stay quiet on a clean history.
func TestDurabilityCountsLostAndDuplicatedWrites(t *testing.T) {
	cases := []struct {
		name                      string
		history                   []Op
		lost, duplicated, applied int
		neverWritten, notRead     int
		linearizable              porcupine.CheckResult
	}{
		{
			name:         "clean",
			history:      history("c0.1;c1.1;c0.2;", write(0, "c0.1;", true), write(1, "c1.1;", true), write(0, "c0.2;", true)),
			linearizable: porcupine.Ok,
		},
		{
			// A write the client gave up on may have happened. Both outcomes pass.
			name:         "unknown write that took effect",
			history:      history("c0.1;c1.1;", write(0, "c0.1;", true), write(1, "c1.1;", false)),
			applied:      1,
			linearizable: porcupine.Ok,
		},
		{
			name:         "unknown write that did not take effect",
			history:      history("c0.1;", write(0, "c0.1;", true), write(1, "c1.1;", false)),
			linearizable: porcupine.Ok,
		},
		{
			name:         "an acknowledged write is missing",
			history:      history("c0.1;c0.2;", write(0, "c0.1;", true), write(1, "c1.1;", true), write(0, "c0.2;", true)),
			lost:         1,
			linearizable: porcupine.Illegal,
		},
		{
			name:         "every acknowledged write is missing, as after losing the volumes",
			history:      history("", write(0, "c0.1;", true), write(1, "c1.1;", true)),
			lost:         2,
			linearizable: porcupine.Illegal,
		},
		{
			name:         "a retried write was applied twice",
			history:      history("c0.1;c1.1;c1.1;", write(0, "c0.1;", true), write(1, "c1.1;", true)),
			duplicated:   1,
			linearizable: porcupine.Illegal,
		},
		{
			name:         "an unknown write was applied twice",
			history:      history("c0.1;c1.1;c1.1;", write(0, "c0.1;", true), write(1, "c1.1;", false)),
			duplicated:   1,
			linearizable: porcupine.Illegal,
		},
		{
			name:         "a value nobody wrote",
			history:      history("c0.1;c9.9;", write(0, "c0.1;", true)),
			neverWritten: 1,
			linearizable: porcupine.Illegal,
		},
	}
	for _, tc := range cases {
		v, err := Verify(tc.history, "")
		if err != nil {
			t.Fatal(err)
		}
		d := v.Durability
		if d.AckedLost != tc.lost || d.Duplicated != tc.duplicated || d.UnknownApplied != tc.applied ||
			d.NeverWritten != tc.neverWritten || d.KeysNotRead != tc.notRead {
			t.Errorf("%s: lost %d, duplicated %d, unknown applied %d, never written %d, not read %d; want %d, %d, %d, %d, %d",
				tc.name, d.AckedLost, d.Duplicated, d.UnknownApplied, d.NeverWritten, d.KeysNotRead,
				tc.lost, tc.duplicated, tc.applied, tc.neverWritten, tc.notRead)
		}
		if v.Linearizable != tc.linearizable {
			t.Errorf("%s: Porcupine says %s, want %s", tc.name, v.Linearizable, tc.linearizable)
		}
		clean := tc.lost+tc.duplicated+tc.neverWritten+tc.notRead == 0
		if v.OK() != clean {
			t.Errorf("%s: verdict OK = %v, want %v", tc.name, v.OK(), clean)
		}
	}

	// A key whose final read failed can't be checked, and that is not a pass.
	unread := []Op{write(0, "c0.1;", true), {Op: chaos.Op{Kind: chaos.Get, Key: "", OK: false}, Final: true}}
	if d := CheckDurability(unread); d.KeysNotRead != 1 || d.OK() {
		t.Errorf("a failed final read: %+v", d)
	}
}

// Percentiles are nearest-rank: always a value that was measured.
func TestSummarize(t *testing.T) {
	var values []float64
	for i := 100; i >= 1; i-- {
		values = append(values, float64(i))
	}
	d := Summarize(values)
	if d.N != 100 || d.Min != 1 || d.Median != 50 || d.P95 != 95 || d.P99 != 99 || d.Max != 100 {
		t.Errorf("1 to 100: %+v", d)
	}
	if d := Summarize([]float64{7, 3}); d.Median != 3 || d.P95 != 7 {
		t.Errorf("two values: %+v", d)
	}
	if d := Summarize(nil); d.N != 0 {
		t.Errorf("no values: %+v", d)
	}
}

// The rollout runs from the first event showing the new image to the first event
// after it in which the controller has caught up and every replica is updated
// and ready.
func TestRolloutSpan(t *testing.T) {
	at := func(seconds int) time.Duration { return time.Duration(seconds) * time.Second }
	old := stsView{image: "raftkv:v1", generation: 1, observed: 1, replicas: 3, ready: 3, updated: 3, currentRevision: "a", updateRevision: "a"}
	step := func(seconds int, change func(*stsView)) stsView {
		v := old
		v.at, v.image, v.generation = at(seconds), "raftkv:v2", 2
		change(&v)
		return v
	}
	w := &stsWatch{all: []stsView{
		old,
		// The spec has changed and the controller hasn't looked yet. The status
		// is still the old one, and must not be read as a finished rollout.
		step(10, func(v *stsView) {}),
		step(11, func(v *stsView) { v.observed, v.updateRevision, v.updated = 2, "b", 0 }),
		step(15, func(v *stsView) { v.observed, v.updateRevision, v.updated, v.ready = 2, "b", 1, 2 }),
		step(25, func(v *stsView) { v.observed, v.updateRevision, v.updated, v.ready = 2, "b", 3, 2 }),
		// All updated and ready, but the revision isn't marked current yet.
		step(27, func(v *stsView) { v.observed, v.updateRevision, v.updated = 2, "b", 3 }),
		step(28, func(v *stsView) { v.observed, v.updateRevision, v.currentRevision, v.updated = 2, "b", "b", 3 }),
	}}
	start, end, started, finished := w.span("raftkv:v2")
	if !started || !finished || start != at(10) || end != at(28) {
		t.Errorf("span = %v to %v, started %v, finished %v; want 10s to 28s", start, end, started, finished)
	}
	if _, _, started, finished := w.span("raftkv:v3"); started || finished {
		t.Error("found a rollout to an image that never appeared")
	}
	w.all = w.all[:5]
	if _, _, started, finished := w.span("raftkv:v2"); !started || finished {
		t.Errorf("an unfinished rollout: started %v, finished %v", started, finished)
	}
}

func TestOtherTag(t *testing.T) {
	for image, want := range map[string][2]string{"raftkv:v1": {"v1", "v2"}, "raftkv:v2": {"v2", "v1"}, "raftkv:dev": {"dev", "v1"}} {
		if from, to := OtherTag(image); from != want[0] || to != want[1] {
			t.Errorf("OtherTag(%q) = %q, %q; want %q, %q", image, from, to, want[0], want[1])
		}
	}
}

// A pod that has been deleted stays listed, and can stay Ready, until its
// process has gone. It must not count.
func TestAllReadyIgnoresPodsBeingDeleted(t *testing.T) {
	target := &Target{Nodes: make([]Node, 3)}
	ready := Pod{Ready: true}
	if !target.AllReady([]Pod{ready, ready, ready}) {
		t.Error("three ready pods are not all ready")
	}
	if target.AllReady([]Pod{ready, ready, {Ready: true, Deleting: true}}) {
		t.Error("a pod being deleted counted as ready")
	}
	if target.AllReady([]Pod{ready, ready}) || target.AllReady([]Pod{ready, ready, {}}) {
		t.Error("a missing or unready pod counted as ready")
	}
}

func TestStatsCountsFailedRequestsAndRetries(t *testing.T) {
	get := func(ok bool, retries int) Op {
		return Op{Op: chaos.Op{Kind: chaos.Get, OK: ok, Return: 2e6}, Retries: retries}
	}
	ops := []Op{write(0, "a;", true), write(0, "b;", false), get(true, 2), get(false, 5), {Op: chaos.Op{Kind: chaos.Get, OK: true}, Final: true}}
	s := Stats(ops)
	if s.Requests != 4 || s.Failed != 2 || s.WritesAcked != 1 || s.WritesUnknown != 1 || s.ReadsServed != 1 || s.ReadsFailed != 1 ||
		s.Retries != 7 || s.RequestsWithRetries != 2 {
		t.Errorf("%+v", s)
	}
}

// Terraform prints the cluster's client key when it destroys the cluster. None
// of it may reach a log.
func TestRedactHidesCredentials(t *testing.T) {
	output := `  # kind_cluster.this will be destroyed
      - client_key             = <<-EOT
            -----BEGIN RSA PRIVATE KEY-----
            MIIEowIBAAKCAQEAsecretsecretsecret
            moresecretmoresecret==
            -----END RSA PRIVATE KEY-----
        EOT -> null
      - kubeconfig             = <<-EOT
                client-certificate-data: LS0tLS1CRUdJTiBDRVJU
                client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVk=
                certificate-authority-data: LS0tLS1CRUdJTg==
        EOT -> null
      - name                   = "raftkv" -> null
`
	got := string(Redact([]byte(output)))
	for _, secret := range []string{"MIIEow", "moresecret", "PRIVATE KEY", "LS0tLS1"} {
		if strings.Contains(got, secret) {
			t.Errorf("%q survived redaction:\n%s", secret, got)
		}
	}
	for _, kept := range []string{"kind_cluster.this will be destroyed", "client_key", "client-key-data: [redacted]", `name                   = "raftkv"`, "[redacted]"} {
		if !strings.Contains(got, kept) {
			t.Errorf("%q was lost in redaction:\n%s", kept, got)
		}
	}
	plain := "Apply complete! Resources: 11 added, 0 changed, 0 destroyed.\n"
	if string(Redact([]byte(plain))) != plain {
		t.Error("output with no credentials was changed")
	}
}
