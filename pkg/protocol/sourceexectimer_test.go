package protocol

import (
	"testing"
	"time"
)

// TestSourceExecTimer_StartCompleteElapsed verifies that a query start followed
// by a backend completion yields the correct elapsed duration (R10.7).
func TestSourceExecTimer_StartCompleteElapsed(t *testing.T) {
	timer := NewSourceExecTimer()
	conn := connID(2001)

	start := time.Unix(100, 0)
	timer.OnClientQuery(conn, start)

	end := start.Add(25 * time.Millisecond)
	elapsed, ok := timer.OnBackendComplete(conn, end)
	if !ok {
		t.Fatalf("expected ok=true for a completion matching a pending start")
	}
	if elapsed != 25*time.Millisecond {
		t.Errorf("elapsed = %v, want %v", elapsed, 25*time.Millisecond)
	}
}

// TestSourceExecTimer_CompleteWithoutStart verifies that a completion with no
// pending start returns ok=false and a zero duration, so callers leave
// SourceExecTime unset rather than recording a bogus value (R10.7).
func TestSourceExecTimer_CompleteWithoutStart(t *testing.T) {
	timer := NewSourceExecTimer()
	conn := connID(2002)

	elapsed, ok := timer.OnBackendComplete(conn, time.Unix(100, 0))
	if ok {
		t.Errorf("expected ok=false for a completion with no pending start")
	}
	if elapsed != 0 {
		t.Errorf("elapsed = %v, want 0 when no pending start", elapsed)
	}
}

// TestSourceExecTimer_DuplicateCompleteIsUnmatched verifies that the pending
// start is consumed by the first completion, so a second completion with no new
// query returns ok=false.
func TestSourceExecTimer_DuplicateCompleteIsUnmatched(t *testing.T) {
	timer := NewSourceExecTimer()
	conn := connID(2003)

	timer.OnClientQuery(conn, time.Unix(100, 0))
	if _, ok := timer.OnBackendComplete(conn, time.Unix(100, 0).Add(time.Millisecond)); !ok {
		t.Fatalf("expected ok=true for the first completion")
	}
	if _, ok := timer.OnBackendComplete(conn, time.Unix(100, 0).Add(2*time.Millisecond)); ok {
		t.Errorf("expected ok=false for a duplicate completion after the start was consumed")
	}
}

// TestSourceExecTimer_PerConnIsolation verifies that pending starts are tracked
// independently per ConnID: completing one connection does not affect another,
// and each yields its own elapsed time (R10.7).
func TestSourceExecTimer_PerConnIsolation(t *testing.T) {
	timer := NewSourceExecTimer()
	connA := connID(2004)
	connB := connID(2005)

	base := time.Unix(200, 0)
	timer.OnClientQuery(connA, base)
	timer.OnClientQuery(connB, base.Add(5*time.Millisecond))

	// Complete B first; it must report its own interval, leaving A pending.
	elapsedB, okB := timer.OnBackendComplete(connB, base.Add(15*time.Millisecond))
	if !okB {
		t.Fatalf("expected ok=true completing connB")
	}
	if elapsedB != 10*time.Millisecond {
		t.Errorf("connB elapsed = %v, want %v", elapsedB, 10*time.Millisecond)
	}

	// A is still pending and unaffected by B's completion.
	elapsedA, okA := timer.OnBackendComplete(connA, base.Add(40*time.Millisecond))
	if !okA {
		t.Fatalf("expected ok=true completing connA")
	}
	if elapsedA != 40*time.Millisecond {
		t.Errorf("connA elapsed = %v, want %v", elapsedA, 40*time.Millisecond)
	}
}

// TestSourceExecTimer_SequentialStatements verifies that multiple statements on
// the same connection are each timed independently across start→complete cycles.
func TestSourceExecTimer_SequentialStatements(t *testing.T) {
	timer := NewSourceExecTimer()
	conn := connID(2006)

	cases := []struct {
		start   time.Time
		elapsed time.Duration
	}{
		{time.Unix(300, 0), 5 * time.Millisecond},
		{time.Unix(301, 0), 12 * time.Millisecond},
		{time.Unix(302, 0), 1 * time.Millisecond},
	}

	for i, c := range cases {
		timer.OnClientQuery(conn, c.start)
		got, ok := timer.OnBackendComplete(conn, c.start.Add(c.elapsed))
		if !ok {
			t.Fatalf("statement %d: expected ok=true", i)
		}
		if got != c.elapsed {
			t.Errorf("statement %d: elapsed = %v, want %v", i, got, c.elapsed)
		}
	}
}

// TestSourceExecTimer_OverwritesPendingStart verifies that a second query
// before any completion overwrites the pending start, so the timer measures the
// most recent statement rather than a stale interval.
func TestSourceExecTimer_OverwritesPendingStart(t *testing.T) {
	timer := NewSourceExecTimer()
	conn := connID(2007)

	timer.OnClientQuery(conn, time.Unix(400, 0))
	// Second query arrives before any completion; it replaces the pending start.
	second := time.Unix(400, 0).Add(30 * time.Millisecond)
	timer.OnClientQuery(conn, second)

	elapsed, ok := timer.OnBackendComplete(conn, second.Add(7*time.Millisecond))
	if !ok {
		t.Fatalf("expected ok=true")
	}
	if elapsed != 7*time.Millisecond {
		t.Errorf("elapsed = %v, want %v (measured from the most recent query)", elapsed, 7*time.Millisecond)
	}
}

// TestSourceExecTimer_Forget verifies that Forget discards a pending start so a
// later completion on the forgotten connection is unmatched.
func TestSourceExecTimer_Forget(t *testing.T) {
	timer := NewSourceExecTimer()
	conn := connID(2008)

	timer.OnClientQuery(conn, time.Unix(500, 0))
	timer.Forget(conn)

	if _, ok := timer.OnBackendComplete(conn, time.Unix(500, 0).Add(time.Millisecond)); ok {
		t.Errorf("expected ok=false after Forget cleared the pending start")
	}
}
