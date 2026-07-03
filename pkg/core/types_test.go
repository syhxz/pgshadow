package core

import (
	"net/netip"
	"testing"
)

func conn(src string, sp uint16, dst string, dp uint16) ConnID {
	return ConnID{
		SrcIP:   netip.MustParseAddr(src),
		SrcPort: sp,
		DstIP:   netip.MustParseAddr(dst),
		DstPort: dp,
	}
}

func TestConnIDKeyStable(t *testing.T) {
	c := conn("10.0.0.1", 54321, "10.0.0.2", 5432)
	want := "10.0.0.1:54321-10.0.0.2:5432"
	if got := c.Key(); got != want {
		t.Fatalf("Key() = %q, want %q", got, want)
	}
	// Key must be deterministic across repeated calls.
	if c.Key() != c.Key() {
		t.Fatal("Key() is not deterministic")
	}
	// String mirrors Key.
	if c.String() != c.Key() {
		t.Fatalf("String() = %q, want %q", c.String(), c.Key())
	}
}

func TestConnIDKeyUnique(t *testing.T) {
	// Distinct four-tuples must produce distinct keys, including direction.
	a := conn("10.0.0.1", 54321, "10.0.0.2", 5432)
	b := conn("10.0.0.2", 5432, "10.0.0.1", 54321)
	if a.Key() == b.Key() {
		t.Fatalf("reversed direction produced identical keys: %q", a.Key())
	}

	differ := []ConnID{
		conn("10.0.0.1", 1, "10.0.0.2", 5432),
		conn("10.0.0.1", 2, "10.0.0.2", 5432),
		conn("10.0.0.3", 1, "10.0.0.2", 5432),
		conn("10.0.0.1", 1, "10.0.0.4", 5432),
		conn("10.0.0.1", 1, "10.0.0.2", 5433),
	}
	seen := map[string]bool{}
	for _, c := range differ {
		k := c.Key()
		if seen[k] {
			t.Fatalf("duplicate key for distinct ConnID: %q", k)
		}
		seen[k] = true
	}
}

func TestConnIDUsableAsMapKey(t *testing.T) {
	m := map[ConnID]int{}
	c := conn("192.168.1.10", 40000, "192.168.1.20", 5432)
	m[c]++
	m[c]++
	if m[c] != 2 {
		t.Fatalf("ConnID map key count = %d, want 2", m[c])
	}
}

func TestTxStatusValues(t *testing.T) {
	if TxIdle != 0 || TxInTx != 1 || TxFailed != 2 {
		t.Fatalf("unexpected TxStatus iota values: %d %d %d", TxIdle, TxInTx, TxFailed)
	}
}
