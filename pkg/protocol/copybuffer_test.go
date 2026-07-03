// Unit tests for COPY data-stream capture (CopyTracker / CopyBuffer), task 3.6
// (R3.10). They reuse the connID helper from eventbuilder_test.go (same package).
package protocol

import (
	"bytes"
	"testing"
)

// helper: assert two [][]byte are segment-for-segment equal.
func equalSegments(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Multi-chunk CopyData followed by CopyDone yields the COPY SQL and the ordered
// segments, preserving per-message segmentation for SQLEvent.CopyData (R3.10).
func TestCopyTracker_MultiChunkThenDone(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4001)

	tr.Start(conn, "COPY t FROM STDIN")
	if !tr.Active(conn) {
		t.Fatal("connection should be in COPY mode after Start")
	}

	chunks := [][]byte{[]byte("1\tfoo\n"), []byte("2\tbar\n"), []byte("3\tbaz\n")}
	for i, c := range chunks {
		if !tr.Data(conn, c) {
			t.Fatalf("chunk %d should be accepted", i)
		}
	}

	sql, data, ok := tr.Done(conn)
	if !ok {
		t.Fatal("CopyDone after CopyData should yield ok=true")
	}
	if sql != "COPY t FROM STDIN" {
		t.Fatalf("unexpected SQL: %q", sql)
	}
	if !equalSegments(data, chunks) {
		t.Fatalf("assembled data mismatch: got %v want %v", data, chunks)
	}
	if tr.Active(conn) {
		t.Fatal("COPY state should be cleared after Done")
	}
}

// Done returns segments that do not alias the caller's input buffers: mutating
// the original payload after Data must not change the buffered COPY data.
func TestCopyTracker_DataIsCopied(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4002)

	tr.Start(conn, "COPY t FROM STDIN")
	payload := []byte("row\n")
	tr.Data(conn, payload)
	payload[0] = 'X' // mutate after handing off

	_, data, ok := tr.Done(conn)
	if !ok || len(data) != 1 {
		t.Fatalf("expected one buffered segment, got ok=%v data=%v", ok, data)
	}
	if !bytes.Equal(data[0], []byte("row\n")) {
		t.Fatalf("buffered segment aliased caller input: %q", data[0])
	}
}

// CopyFail discards the buffered data: no COPY data is recoverable and the
// connection leaves COPY mode (R3.10).
func TestCopyTracker_CopyFailDiscards(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4003)

	tr.Start(conn, "COPY t FROM STDIN")
	tr.Data(conn, []byte("partial\n"))
	tr.Fail(conn)

	if tr.Active(conn) {
		t.Fatal("COPY state should be cleared after CopyFail")
	}
	// A Done after Fail must not resurrect any data.
	if _, _, ok := tr.Done(conn); ok {
		t.Fatal("Done after CopyFail should yield ok=false")
	}
}

// Discard (connection close / idle timeout) drops an in-progress COPY that never
// received its terminating CopyDone/CopyFail (R3.10).
func TestCopyTracker_DiscardOnClose(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4004)

	tr.Start(conn, "COPY t FROM STDIN")
	tr.Data(conn, []byte("orphan\n"))
	tr.Discard(conn)

	if tr.Active(conn) {
		t.Fatal("COPY state should be cleared after Discard")
	}
}

// Two connections running COPY concurrently keep independent buffers; finishing
// one must not disturb the other (per-ConnID isolation).
func TestCopyTracker_PerConnIsolation(t *testing.T) {
	tr := NewCopyTracker()
	a := connID(4005)
	b := connID(4006)

	tr.Start(a, "COPY a FROM STDIN")
	tr.Start(b, "COPY b FROM STDIN")
	tr.Data(a, []byte("a1\n"))
	tr.Data(b, []byte("b1\n"))
	tr.Data(a, []byte("a2\n"))

	// Fail one connection; the other must be untouched.
	tr.Fail(a)
	if tr.Active(a) {
		t.Fatal("connection a should be cleared after Fail")
	}
	if !tr.Active(b) {
		t.Fatal("connection b should still be in COPY mode")
	}

	sql, data, ok := tr.Done(b)
	if !ok || sql != "COPY b FROM STDIN" {
		t.Fatalf("connection b Done unexpected: ok=%v sql=%q", ok, sql)
	}
	if !equalSegments(data, [][]byte{[]byte("b1\n")}) {
		t.Fatalf("connection b data leaked from a: %v", data)
	}
}

// CopyData with no active COPY is ignored gracefully (returns false, stores
// nothing) and a subsequent CopyDone yields no data.
func TestCopyTracker_StrayCopyData(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4007)

	if tr.Data(conn, []byte("stray\n")) {
		t.Fatal("CopyData with no active COPY should return false")
	}
	if tr.Active(conn) {
		t.Fatal("stray CopyData must not start a COPY")
	}
	if _, _, ok := tr.Done(conn); ok {
		t.Fatal("CopyDone with no active COPY should yield ok=false")
	}
}

// A CopyDone with no active COPY is handled gracefully.
func TestCopyTracker_StrayCopyDone(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4008)

	if _, _, ok := tr.Done(conn); ok {
		t.Fatal("stray CopyDone should yield ok=false")
	}
}

// Exceeding the per-COPY byte cap marks the COPY overflowed: buffered data is
// released, further CopyData is rejected, and CopyDone yields no data so partial
// COPY content is never replayed.
func TestCopyTracker_OverflowDiscards(t *testing.T) {
	tr := NewCopyTrackerWithLimit(10) // 10-byte cap
	conn := connID(4009)

	tr.Start(conn, "COPY t FROM STDIN")
	if !tr.Data(conn, []byte("12345")) { // 5 bytes, under cap
		t.Fatal("first chunk under cap should be accepted")
	}
	if tr.Data(conn, []byte("678901")) { // would bring total to 11 > 10
		t.Fatal("chunk breaching cap should be rejected")
	}
	// Further data stays rejected while overflowed.
	if tr.Data(conn, []byte("x")) {
		t.Fatal("data after overflow should be rejected")
	}

	sql, data, ok := tr.Done(conn)
	if ok {
		t.Fatalf("overflowed COPY should yield ok=false, got sql=%q data=%v", sql, data)
	}
	if tr.Active(conn) {
		t.Fatal("COPY state should be cleared after Done on overflow")
	}
}

// A chunk exactly at the cap boundary is accepted (cap is inclusive).
func TestCopyTracker_ExactCapAccepted(t *testing.T) {
	tr := NewCopyTrackerWithLimit(4)
	conn := connID(4010)

	tr.Start(conn, "COPY t FROM STDIN")
	if !tr.Data(conn, []byte("abcd")) { // exactly 4 bytes == cap
		t.Fatal("chunk exactly at cap should be accepted")
	}
	_, data, ok := tr.Done(conn)
	if !ok || !equalSegments(data, [][]byte{[]byte("abcd")}) {
		t.Fatalf("expected single 4-byte segment, got ok=%v data=%v", ok, data)
	}
}

// Starting a new COPY supersedes an unfinished one on the same connection.
func TestCopyTracker_RestartSupersedes(t *testing.T) {
	tr := NewCopyTracker()
	conn := connID(4011)

	tr.Start(conn, "COPY old FROM STDIN")
	tr.Data(conn, []byte("stale\n"))
	tr.Start(conn, "COPY new FROM STDIN") // supersedes
	tr.Data(conn, []byte("fresh\n"))

	sql, data, ok := tr.Done(conn)
	if !ok || sql != "COPY new FROM STDIN" {
		t.Fatalf("expected new COPY to win, got ok=%v sql=%q", ok, sql)
	}
	if !equalSegments(data, [][]byte{[]byte("fresh\n")}) {
		t.Fatalf("stale data leaked into restarted COPY: %v", data)
	}
}

// A zero/negative limit falls back to DefaultMaxCopyBytes.
func TestNewCopyTrackerWithLimit_DefaultFallback(t *testing.T) {
	tr := NewCopyTrackerWithLimit(0)
	if tr.maxBytes != DefaultMaxCopyBytes {
		t.Fatalf("expected default cap %d, got %d", DefaultMaxCopyBytes, tr.maxBytes)
	}
}
