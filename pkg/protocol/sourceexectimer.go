// This file implements the SourceExecTimer (R10.7) — task 3.7.
//
// In bidirectional capture mode pgshadow observes both the client→server and
// server→client halves of each connection. The SourceExecTimer turns that
// bidirectional traffic into a per-statement source-side execution time, which
// the metrics layer later compares against the time the same statement takes on
// the replay target (R10.5/R10.7, the source-vs-target comparison).
//
// Timing model:
//
//   - When a client query is observed for a ConnID at time T_start — a Simple
//     Query ('Q'), or the Extended_Query Execute ('E') / the Sync ('S') that
//     triggers backend execution — OnClientQuery records T_start as the pending
//     start for that connection.
//   - When the backend completes the statement at time T_end — observed via
//     CommandComplete ('C') or the ReadyForQuery ('Z') that ends the command —
//     OnBackendComplete returns the elapsed T_end - T_start and clears the
//     pending start.
//
// State is tracked per ConnID so concurrent connections are fully isolated, and
// sequential statements on one connection are each timed independently. All
// measured timestamps are injected by the caller (the parse pipeline observes
// the capture timestamp on each message); the timer never reads the wall clock
// for the measured points, which keeps it deterministic and unit-testable.
//
// Final wiring into the parse pipeline / EventBuilder, which will call
// OnClientQuery / OnBackendComplete and stamp core.SQLEvent.SourceExecTime with
// the returned duration, is deferred to pipeline assembly (task 12.2). This
// file delivers SourceExecTimer as a standalone, fully unit-tested component.
package protocol

import (
	"time"

	"pgshadow/pkg/core"
)

// SourceExecTimer measures source-side statement execution time from
// bidirectionally-captured traffic by tracking the request→response interval
// per ConnID (R10.7). It holds one pending start timestamp per connection; the
// zero value is not usable — construct it with NewSourceExecTimer.
type SourceExecTimer struct {
	pending map[core.ConnID]time.Time // start timestamp of the in-flight statement, per connection
}

// NewSourceExecTimer constructs an empty SourceExecTimer ready to track
// per-connection request→response intervals.
func NewSourceExecTimer() *SourceExecTimer {
	return &SourceExecTimer{pending: make(map[core.ConnID]time.Time)}
}

// OnClientQuery records the start time of a statement for conn. It is called
// when a client message that triggers backend execution is observed — a Simple
// Query ('Q'), or the Extended_Query Execute ('E') / Sync ('S'). The supplied
// timestamp is the capture time of that message (injected for determinism).
//
// If a previous statement on the same connection never observed a completion
// (e.g. its response was missed or the connection pipelined), the pending start
// is overwritten so the timer measures the most recent statement rather than
// reporting a stale interval.
func (t *SourceExecTimer) OnClientQuery(conn core.ConnID, ts time.Time) {
	if t.pending == nil {
		t.pending = make(map[core.ConnID]time.Time)
	}
	t.pending[conn] = ts
}

// OnBackendComplete reports the source-side execution time for the in-flight
// statement on conn and clears the pending start. It is called when the backend
// signals completion — CommandComplete ('C') or the ReadyForQuery ('Z') that
// ends the command — with ts the capture time of that message.
//
// It returns (elapsed, true) when a matching OnClientQuery was recorded, where
// elapsed is ts minus the recorded start. When there is no pending start for
// the connection (a completion with no preceding query, or a duplicate
// completion), it returns (0, false) so callers leave SourceExecTime unset
// rather than recording a bogus duration.
//
// A completion timestamp earlier than the recorded start (possible only with
// out-of-order capture timestamps) yields a negative duration alongside true;
// callers may treat a non-positive duration as unavailable.
func (t *SourceExecTimer) OnBackendComplete(conn core.ConnID, ts time.Time) (time.Duration, bool) {
	start, ok := t.pending[conn]
	if !ok {
		return 0, false
	}
	delete(t.pending, conn)
	return ts.Sub(start), true
}

// Forget discards any pending start for conn. It is called when a connection is
// closed or times out so the timer does not retain state for dead connections.
func (t *SourceExecTimer) Forget(conn core.ConnID) {
	delete(t.pending, conn)
}
