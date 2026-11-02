package ikey

import "testing"

func TestOrdering(t *testing.T) {
	a1 := Encode(nil, []byte("a"), 1, KindValue)
	a5 := Encode(nil, []byte("a"), 5, KindValue)
	a5d := Encode(nil, []byte("a"), 5, KindDelete)
	b1 := Encode(nil, []byte("b"), 1, KindValue)
	if Compare(a5, a1) >= 0 {
		t.Fatal("newer seq must sort first")
	}
	if Compare(a1, b1) >= 0 {
		t.Fatal("user key order broken")
	}
	if Compare(a5, a5d) >= 0 {
		t.Fatal("value should sort before delete at equal seq")
	}
	s := SearchKey(nil, []byte("a"), 5)
	if Compare(s, a5) >= 0 || Compare(s, a5d) >= 0 {
		t.Fatal("search key must precede entries at the same seq")
	}
	a6 := Encode(nil, []byte("a"), 6, KindValue)
	if Compare(a6, s) >= 0 {
		t.Fatal("entries newer than the snapshot must precede the search key")
	}
	u, seq, kind := Split(a5d)
	if string(u) != "a" || seq != 5 || kind != KindDelete {
		t.Fatalf("split = %q %d %d", u, seq, kind)
	}
}
