package main

import "testing"

// H5. A StatefulSet pod's name gives its node ID.
func TestIDFromHostname(t *testing.T) {
	for host, want := range map[string]uint64{"raftkv-0": 1, "raftkv-2": 3, "my-store-raftkv-10": 11} {
		if got, err := idFromHostname(host); err != nil || got != want {
			t.Errorf("idFromHostname(%q) = %d, %v; want %d", host, got, err, want)
		}
	}
	for _, host := range []string{"raftkv", "raftkv-", "raftkv-a", "node1", "", "raftkv-1x", "raftkv-+1"} {
		if got, err := idFromHostname(host); err == nil {
			t.Errorf("idFromHostname(%q) = %d; want an error", host, got)
		}
	}
}
