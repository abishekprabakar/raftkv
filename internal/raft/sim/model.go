package sim

import (
	"slices"
	"strings"

	"github.com/abishekprabakar/raftkv/internal/raft"
)

// The clients only ever append unique tokens to keys. That makes correctness easy
// to judge from the outside, with no knowledge of how the cluster works. A key's
// value is a list of tokens, and in any correct history:
//
//   - no token appears twice, because a retried write applies exactly once;
//   - every token came from some client;
//   - any two reads of a key agree, one being a prefix of the other, because
//     appends happen in a single order;
//   - a read sees every append acknowledged before it started, and everything an
//     earlier completed read saw, because a linearizable system never goes back.
//
// This is a cheap partial check for linearizability, and it can fail only on a real
// violation. M5 checks whole histories with Porcupine.

const tokenSeparator = ","

func splitTokens(value []byte) []string {
	if len(value) == 0 {
		return nil
	}
	return strings.Split(strings.TrimSuffix(string(value), tokenSeparator), tokenSeparator)
}

type keyModel struct {
	acked   []string // tokens acknowledged to clients, in order
	longest []string // the longest value any read has returned
	settled int      // tokens seen by reads that have finished
}

// kvModel is what clients collectively know about the store.
type kvModel struct {
	check  *checker
	keys   map[string]*keyModel
	issued map[string]bool // every token any client ever tried to write
}

func newKVModel(check *checker) *kvModel {
	return &kvModel{check: check, keys: map[string]*keyModel{}, issued: map[string]bool{}}
}

func (m *kvModel) key(k string) *keyModel {
	km := m.keys[k]
	if km == nil {
		km = &keyModel{}
		m.keys[k] = km
	}
	return km
}

// readStart records what a read must see, at the moment it begins.
func (m *kvModel) readStart(k string) (mustSeeAcked, mustSeeTokens int) {
	km := m.key(k)
	return len(km.acked), km.settled
}

func (m *kvModel) ack(k, token string) {
	km := m.key(k)
	km.acked = append(km.acked, token)
}

// readDone judges a finished read against what it had to see when it started.
func (m *kvModel) readDone(id raft.NodeID, k string, value []byte, mustSeeAcked, mustSeeTokens int) {
	km := m.key(k)
	tokens := splitTokens(value)
	seen := make(map[string]bool, len(tokens))
	for _, t := range tokens {
		if seen[t] {
			m.check.fail(CheckExactlyOnce, id, "key %s holds %s twice", k, t)
			return
		}
		seen[t] = true
		if !m.issued[t] {
			m.check.fail(CheckPhantom, id, "key %s holds %s, which no client wrote", k, t)
			return
		}
	}
	shared := min(len(tokens), len(km.longest))
	if !slices.Equal(tokens[:shared], km.longest[:shared]) {
		m.check.fail(CheckLinearizable, id, "reads of key %s disagree: %v and %v", k, tokens, km.longest)
		return
	}
	if len(tokens) < mustSeeTokens {
		m.check.fail(CheckLinearizable, id, "read of key %s went backwards: %d tokens after an earlier read saw %d", k, len(tokens), mustSeeTokens)
		return
	}
	for _, t := range km.acked[:mustSeeAcked] {
		if !seen[t] {
			m.check.fail(CheckLinearizable, id, "read of key %s misses %s, acknowledged before the read began", k, t)
			return
		}
	}
	if len(tokens) > len(km.longest) {
		km.longest = tokens
	}
	km.settled = max(km.settled, len(tokens))
}

// finalRead judges the read at the end of a schedule, which every acknowledged
// write must have reached.
func (m *kvModel) finalRead(id raft.NodeID, k string, value []byte) {
	km := m.key(k)
	m.readDone(id, k, value, len(km.acked), km.settled)
	if m.check.violation != nil {
		return
	}
	seen := map[string]bool{}
	for _, t := range splitTokens(value) {
		seen[t] = true
	}
	for _, t := range km.acked {
		if !seen[t] {
			m.check.fail(CheckLostWrite, id, "key %s lost acknowledged write %s", k, t)
			return
		}
	}
}
