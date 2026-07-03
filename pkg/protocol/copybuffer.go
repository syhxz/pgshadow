// This file implements COPY data-stream capture (R3.10) — task 3.6.
//
// When a `COPY ... FROM STDIN` statement is observed on a connection, the
// client follows the Simple/Extended query with a sequence of CopyData ('d')
// messages carrying the bulk-load payload, terminated by either CopyDone ('c')
// — the load succeeded and all rows were sent — or CopyFail ('f') — the client
// aborted the load. To let the Replayer reproduce the bulk load, the parser
// must buffer the CopyData payloads per connection and hand the assembled data
// to the originating COPY statement's SQL_Event (core.SQLEvent.CopyData).
//
// CopyBuffer (declared in protocol.go) is the per-COPY accumulator: it holds the
// originating COPY SQL and the ordered CopyData payload segments. CopyTracker is
// the per-ConnID state machine that owns the in-progress CopyBuffers and drives
// them through the d* → c | f lifecycle. CopyTracker is the unit wired into the
// EventBuilder during pipeline assembly (task 12.2); here it is delivered as a
// standalone, fully unit-tested component and intentionally does not modify
// eventbuilder.go.
//
// Memory is bounded per active COPY: a single in-progress COPY may accumulate at
// most MaxBytes bytes (DefaultMaxCopyBytes when unset). Once that cap is
// exceeded the buffered segments are released immediately and the COPY is marked
// as overflowed, so a single oversized bulk load can never grow the process
// heap without bound. An overflowed COPY yields no data on CopyDone (ok=false):
// partial COPY data is never replayed, mirroring the oversized-SQL skip policy
// (R3.5, R3.6).
package protocol

import "pgshadow/pkg/core"

// COPY sub-protocol message type bytes (client→server). These are distinct from
// the SQL-bearing message types inspected by the EventBuilder and are not parse
// errors when they arrive (R3.10).
const (
	msgCopyData byte = 'd' // CopyData: one chunk of COPY FROM payload
	msgCopyDone byte = 'c' // CopyDone: client finished sending COPY data
	msgCopyFail byte = 'f' // CopyFail: client aborted the COPY
)

// DefaultMaxCopyBytes caps the total CopyData payload buffered for a single
// in-progress COPY (256 MiB). It bounds memory for one bulk load; beyond it the
// COPY is marked overflowed and its buffered data is discarded.
const DefaultMaxCopyBytes = 256 << 20

// appendSegment stores an independent copy of one CopyData payload segment on
// the buffer, preserving wire order. The payload is copied so the CopyBuffer
// never aliases the caller's (or the framing layer's) backing array.
func (cb *CopyBuffer) appendSegment(payload []byte) {
	seg := make([]byte, len(payload))
	copy(seg, payload)
	cb.Data = append(cb.Data, seg)
}

// CopyTracker tracks in-progress COPY FROM data streams per ConnID and drives
// each through the CopyData → CopyDone | CopyFail lifecycle (R3.10). It is
// stateful and not safe for concurrent use; callers hold one tracker per parser
// pipeline, consistent with the other per-ConnID parser collaborators.
type CopyTracker struct {
	active   map[core.ConnID]*CopyBuffer // in-progress COPY per connection
	sizes    map[core.ConnID]int         // bytes buffered for the active COPY
	overflow map[core.ConnID]bool        // active COPY exceeded MaxBytes
	maxBytes int                         // per-COPY cap; <=0 means DefaultMaxCopyBytes
}

// NewCopyTracker constructs a CopyTracker that caps a single in-progress COPY at
// DefaultMaxCopyBytes.
func NewCopyTracker() *CopyTracker {
	return NewCopyTrackerWithLimit(DefaultMaxCopyBytes)
}

// NewCopyTrackerWithLimit constructs a CopyTracker capping a single in-progress
// COPY at maxBytes. A maxBytes <= 0 selects DefaultMaxCopyBytes.
func NewCopyTrackerWithLimit(maxBytes int) *CopyTracker {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxCopyBytes
	}
	return &CopyTracker{
		active:   make(map[core.ConnID]*CopyBuffer),
		sizes:    make(map[core.ConnID]int),
		overflow: make(map[core.ConnID]bool),
		maxBytes: maxBytes,
	}
}

// Active reports whether a COPY FROM data stream is currently being buffered for
// the connection.
func (t *CopyTracker) Active(conn core.ConnID) bool {
	_, ok := t.active[conn]
	return ok
}

// Start enters COPY mode for the connection, recording the originating COPY
// statement's SQL so the assembled data can later be attached to its SQL_Event.
// Any previously in-progress COPY for the same connection is discarded first: a
// new COPY statement supersedes an unfinished one (e.g. a stream truncated by a
// missing CopyDone), which keeps per-connection state bounded and consistent.
func (t *CopyTracker) Start(conn core.ConnID, sql string) {
	t.clear(conn)
	t.active[conn] = &CopyBuffer{SQL: sql}
}

// Data appends one CopyData ('d') payload to the connection's in-progress COPY.
// It returns true when the payload was accepted into an active, non-overflowed
// COPY. It returns false when there is no active COPY for the connection (a
// stray CopyData is ignored gracefully rather than treated as an error) or when
// the active COPY has already overflowed its byte cap.
func (t *CopyTracker) Data(conn core.ConnID, payload []byte) bool {
	buf, ok := t.active[conn]
	if !ok {
		// Stray CopyData with no COPY in progress: ignore gracefully (R3.10).
		return false
	}
	if t.overflow[conn] {
		// Already over the cap; keep discarding until CopyDone/CopyFail.
		return false
	}
	if t.sizes[conn]+len(payload) > t.maxBytes {
		// This segment would breach the per-COPY cap. Mark the COPY overflowed
		// and release what we have so memory stays bounded; the eventual
		// CopyDone yields no data (ok=false) so partial data is never replayed.
		t.overflow[conn] = true
		buf.Data = nil
		t.sizes[conn] = 0
		return false
	}
	buf.appendSegment(payload)
	t.sizes[conn] += len(payload)
	return true
}

// Done finalizes the connection's COPY on CopyDone ('c'). When a complete,
// non-overflowed COPY was in progress it returns the originating SQL, the
// ordered CopyData segments, and ok=true; the per-connection state is cleared
// either way. It returns ok=false when there was no active COPY or when the COPY
// overflowed its byte cap (its data was already discarded).
func (t *CopyTracker) Done(conn core.ConnID) (sql string, data [][]byte, ok bool) {
	buf, present := t.active[conn]
	overflowed := t.overflow[conn]
	t.clear(conn)
	if !present || overflowed {
		return "", nil, false
	}
	return buf.SQL, buf.Data, true
}

// Fail discards the connection's in-progress COPY on CopyFail ('f'): the client
// aborted the load, so the buffered data must not be replayed. It is a no-op
// when no COPY is in progress.
func (t *CopyTracker) Fail(conn core.ConnID) {
	t.clear(conn)
}

// Discard drops any in-progress COPY for the connection. It is invoked on
// connection close or idle timeout so a COPY stream that never received its
// terminating CopyDone/CopyFail does not leak buffered payload (R3.10). It is a
// no-op when no COPY is in progress.
func (t *CopyTracker) Discard(conn core.ConnID) {
	t.clear(conn)
}

// clear removes all per-connection COPY state for conn.
func (t *CopyTracker) clear(conn core.ConnID) {
	delete(t.active, conn)
	delete(t.sizes, conn)
	delete(t.overflow, conn)
}
