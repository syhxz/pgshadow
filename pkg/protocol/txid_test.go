// Unit tests for TxID generation (R3.8) — task 3.4.
//
// These tests verify the TxIDGenerator allocation primitives and exercise the
// expected EventBuilder usage pattern: statements within one transaction share
// a TxID; a new transaction (new BEGIN, or a statement after COMMIT/ROLLBACK)
// gets a fresh, greater TxID; and per-connection state is independent.
package protocol

import (
	"net/netip"
	"sync"
	"testing"

	"pgshadow/pkg/core"
)

// mkConn builds a distinct ConnID per source port for test isolation.
func mkConn(srcPort uint16) core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr("10.0.0.1"),
		SrcPort: srcPort,
		DstIP:   netip.MustParseAddr("10.0.0.2"),
		DstPort: 5432,
	}
}

// Current must report 0 before any TxID has been allocated for a connection.
func TestCurrentZeroBeforeAdvance(t *testing.T) {
	g := NewTxIDGenerator()
	if got := g.Current(mkConn(1001)); got != 0 {
		t.Fatalf("Current before Advance = %d, want 0", got)
	}
}

// Advance must produce strictly-increasing TxIDs starting at 1.
func TestAdvanceMonotonic(t *testing.T) {
	g := NewTxIDGenerator()
	conn := mkConn(1002)

	var prev uint64
	for i := 0; i < 5; i++ {
		got := g.Advance(conn)
		if i == 0 && got != 1 {
			t.Fatalf("first Advance = %d, want 1", got)
		}
		if got <= prev {
			t.Fatalf("Advance #%d = %d, not greater than previous %d", i, got, prev)
		}
		prev = got
	}
}

// All statements within one transaction share the same TxID: after a single
// Advance (BEGIN), repeated Current calls return the same value.
func TestSameTxIDWithinTransaction(t *testing.T) {
	g := NewTxIDGenerator()
	conn := mkConn(1003)

	tx := g.Advance(conn) // BEGIN
	for i := 0; i < 4; i++ {
		if got := g.Current(conn); got != tx {
			t.Fatalf("Current within transaction = %d, want stable %d", got, tx)
		}
	}
}

// Reset (COMMIT/ROLLBACK) clears the active group: Current returns 0 and the
// next Advance (new BEGIN) yields a new, greater TxID.
func TestNewTxIDAfterCommitOrRollback(t *testing.T) {
	g := NewTxIDGenerator()
	conn := mkConn(1004)

	tx1 := g.Advance(conn) // BEGIN
	if got := g.Current(conn); got != tx1 {
		t.Fatalf("Current in first transaction = %d, want %d", got, tx1)
	}

	g.Reset(conn) // COMMIT / ROLLBACK
	if got := g.Current(conn); got != 0 {
		t.Fatalf("Current after Reset = %d, want 0", got)
	}

	tx2 := g.Advance(conn) // new BEGIN
	if tx2 <= tx1 {
		t.Fatalf("TxID after new BEGIN = %d, want greater than %d", tx2, tx1)
	}
	if got := g.Current(conn); got != tx2 {
		t.Fatalf("Current in second transaction = %d, want %d", got, tx2)
	}
}

// A new BEGIN before the previous group is Reset still advances to a fresh
// TxID, and subsequent statements follow the new group.
func TestNewBeginAdvancesGroup(t *testing.T) {
	g := NewTxIDGenerator()
	conn := mkConn(1005)

	tx1 := g.Advance(conn)
	tx2 := g.Advance(conn)
	if tx2 <= tx1 {
		t.Fatalf("second BEGIN TxID = %d, want greater than %d", tx2, tx1)
	}
	if got := g.Current(conn); got != tx2 {
		t.Fatalf("Current after second BEGIN = %d, want %d", got, tx2)
	}
}

// Per-connection independence: allocation and reset on one connection must not
// affect another connection's TxID.
func TestPerConnIndependence(t *testing.T) {
	g := NewTxIDGenerator()
	a := mkConn(2001)
	b := mkConn(2002)

	txA := g.Advance(a)
	txB := g.Advance(b)
	if txA == txB {
		t.Fatalf("distinct connections share TxID %d", txA)
	}

	// Resetting a must not disturb b.
	g.Reset(a)
	if got := g.Current(a); got != 0 {
		t.Fatalf("Current(a) after Reset(a) = %d, want 0", got)
	}
	if got := g.Current(b); got != txB {
		t.Fatalf("Current(b) after Reset(a) = %d, want unchanged %d", got, txB)
	}

	// Advancing a again must not disturb b.
	g.Advance(a)
	if got := g.Current(b); got != txB {
		t.Fatalf("Current(b) after Advance(a) = %d, want unchanged %d", got, txB)
	}
}

// The zero-value generator must be usable without NewTxIDGenerator (lazy init).
func TestZeroValueUsable(t *testing.T) {
	var g TxIDGenerator
	conn := mkConn(3001)

	if got := g.Current(conn); got != 0 {
		t.Fatalf("zero-value Current = %d, want 0", got)
	}
	if got := g.Advance(conn); got != 1 {
		t.Fatalf("zero-value first Advance = %d, want 1", got)
	}
	if got := g.Current(conn); got != 1 {
		t.Fatalf("zero-value Current after Advance = %d, want 1", got)
	}
}

// Concurrent Advance calls across many connections must each yield a unique
// TxID, validating the documented concurrency safety.
func TestConcurrentAdvanceUnique(t *testing.T) {
	g := NewTxIDGenerator()
	const n = 200

	var (
		mu   sync.Mutex
		seen = make(map[uint64]struct{}, n)
		wg   sync.WaitGroup
	)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(port uint16) {
			defer wg.Done()
			id := g.Advance(mkConn(port))
			mu.Lock()
			seen[id] = struct{}{}
			mu.Unlock()
		}(uint16(4000 + i))
	}
	wg.Wait()

	if len(seen) != n {
		t.Fatalf("got %d unique TxIDs across %d concurrent Advances, want %d", len(seen), n, n)
	}
}
