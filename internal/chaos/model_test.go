package chaos

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func check(t *testing.T, history []Op) porcupine.CheckResult {
	t.Helper()
	return porcupine.CheckOperationsTimeout(Model, Operations(history), 10*time.Second)
}

// P1. The model accepts what a correct store could do and rejects what it can't.
// A checker that has never rejected anything proves nothing.
func TestModelRejectsBadHistories(t *testing.T) {
	cases := []struct {
		name    string
		history []Op
		want    porcupine.CheckResult
	}{
		{"read after write", []Op{
			{Client: 0, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Get, Key: "x", Value: "a", Call: 20, Return: 30, OK: true},
		}, porcupine.Ok},
		{"a read concurrent with a write may see either value", []Op{
			{Client: 0, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 0, Kind: Put, Key: "x", Value: "b", Call: 20, Return: 40, OK: true},
			{Client: 1, Kind: Get, Key: "x", Value: "a", Call: 25, Return: 30, OK: true},
		}, porcupine.Ok},
		{"an unknown write that never happened", []Op{
			{Client: 0, Kind: Append, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Append, Key: "x", Value: "b", Call: 5, Return: 15, OK: false},
			{Client: 2, Kind: Get, Key: "x", Value: "a", Call: 50, Return: 60, OK: true},
		}, porcupine.Ok},
		{"an unknown write that did happen", []Op{
			{Client: 0, Kind: Append, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Append, Key: "x", Value: "b", Call: 5, Return: 15, OK: false},
			{Client: 2, Kind: Get, Key: "x", Value: "ab", Call: 50, Return: 60, OK: true},
		}, porcupine.Ok},
		{"a failed read is ignored", []Op{
			{Client: 0, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Get, Key: "x", Call: 20, Return: 30, OK: false},
		}, porcupine.Ok},
		{"keys are independent", []Op{
			{Client: 0, Kind: Put, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Get, Key: "y", Value: "", Call: 20, Return: 30, OK: true},
		}, porcupine.Ok},

		// What the deposed leader would do with a plain read: return a value older
		// than one already acknowledged.
		{"stale read", []Op{
			{Client: 0, Kind: Put, Key: "x", Value: "1", Call: 0, Return: 10, OK: true},
			{Client: 0, Kind: Put, Key: "x", Value: "2", Call: 20, Return: 30, OK: true},
			{Client: 1, Kind: Get, Key: "x", Value: "1", Call: 40, Return: 50, OK: true},
		}, porcupine.Illegal},
		{"lost write", []Op{
			{Client: 0, Kind: Append, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Get, Key: "x", Value: "", Call: 20, Return: 30, OK: true},
		}, porcupine.Illegal},
		{"duplicated append", []Op{
			{Client: 0, Kind: Append, Key: "x", Value: "a", Call: 0, Return: 10, OK: true},
			{Client: 1, Kind: Get, Key: "x", Value: "aa", Call: 20, Return: 30, OK: true},
		}, porcupine.Illegal},
		{"an unknown write seen, then unseen", []Op{
			{Client: 0, Kind: Append, Key: "x", Value: "a", Call: 0, Return: 10, OK: false},
			{Client: 1, Kind: Get, Key: "x", Value: "a", Call: 20, Return: 30, OK: true},
			{Client: 1, Kind: Get, Key: "x", Value: "", Call: 40, Return: 50, OK: true},
		}, porcupine.Illegal},
	}
	for _, c := range cases {
		if got := check(t, c.history); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}
