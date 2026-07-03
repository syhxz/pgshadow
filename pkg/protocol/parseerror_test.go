package protocol

import (
	"strings"
	"testing"
	"time"
)

// TestBuild_OversizedSimpleQuerySkippedAndRecorded verifies that a Simple_Query
// whose SQL exceeds MaxSQLLength is skipped, recorded via the parse-error hook,
// does not advance the per-connection sequence, and that a subsequent valid
// message on the same connection is still parsed (R3.5, R3.6).
func TestBuild_OversizedSimpleQuerySkippedAndRecorded(t *testing.T) {
	var parseErrors int
	b := NewEventBuilderWithHook(
		Config{ExtendedQuery: true, MaxSQLLength: 8},
		func() { parseErrors++ },
	)
	conn := connID(1001)

	big := strings.Repeat("x", 9) // 9 bytes > MaxSQLLength of 8
	ev, ok := b.Build(conn, msg('Q', append([]byte(big), 0)), time.Unix(1, 0))
	if ok || ev != nil {
		t.Fatalf("expected oversized Simple_Query to be skipped, got (%+v,%v)", ev, ok)
	}
	if parseErrors != 1 {
		t.Fatalf("parse-error hook called %d times, want 1", parseErrors)
	}

	// A subsequent valid message must still be parsed and receive Seq 1, proving
	// the oversized message neither aborted the stream nor consumed a sequence.
	ev, ok = b.Build(conn, msg('Q', []byte("select 1\x00")), time.Unix(2, 0))
	if !ok || ev == nil {
		t.Fatalf("expected subsequent valid message to parse, got (%+v,%v)", ev, ok)
	}
	if ev.SQL != "select 1" {
		t.Errorf("SQL = %q, want %q", ev.SQL, "select 1")
	}
	if ev.Seq != 1 {
		t.Errorf("Seq = %d, want 1 (oversized message must not advance sequence)", ev.Seq)
	}
	if parseErrors != 1 {
		t.Errorf("parse-error hook called %d times after valid message, want 1", parseErrors)
	}
}

// TestBuild_OversizedParseSkippedAndRecorded verifies the same oversized
// handling for an Extended_Query Parse ('P') message (R3.5, R3.6).
func TestBuild_OversizedParseSkippedAndRecorded(t *testing.T) {
	var parseErrors int
	b := NewEventBuilderWithHook(
		Config{ExtendedQuery: true, MaxSQLLength: 16},
		func() { parseErrors++ },
	)
	conn := connID(2002)

	big := strings.Repeat("a", 17) // 17 bytes > MaxSQLLength of 16
	payload := parsePayload("stmt1", big, []uint32{23})
	ev, ok := b.Build(conn, msg('P', payload), time.Now())
	if ok || ev != nil {
		t.Fatalf("expected oversized Parse to be skipped, got (%+v,%v)", ev, ok)
	}
	if parseErrors != 1 {
		t.Fatalf("parse-error hook called %d times, want 1", parseErrors)
	}

	// Subsequent valid Parse still parsed with Seq 1.
	ev, ok = b.Build(conn, msg('P', parsePayload("ok", "select 2", nil)), time.Now())
	if !ok || ev == nil {
		t.Fatalf("expected subsequent valid Parse to parse, got (%+v,%v)", ev, ok)
	}
	if ev.Seq != 1 {
		t.Errorf("Seq = %d, want 1", ev.Seq)
	}
}

// TestBuild_MalformedParseSkippedAndRecorded verifies that an undecodable Parse
// payload is skipped, recorded as a parse error, and that parsing continues on
// the same connection (R3.6).
func TestBuild_MalformedParseSkippedAndRecorded(t *testing.T) {
	var parseErrors int
	b := NewEventBuilderWithHook(
		Config{ExtendedQuery: true, MaxSQLLength: 1 << 20},
		func() { parseErrors++ },
	)
	conn := connID(3003)

	// No NUL terminators: cannot be decoded as a Parse message.
	ev, ok := b.Build(conn, msg('P', []byte("no terminators here")), time.Now())
	if ok || ev != nil {
		t.Fatalf("expected malformed Parse to be skipped, got (%+v,%v)", ev, ok)
	}
	if parseErrors != 1 {
		t.Fatalf("parse-error hook called %d times, want 1", parseErrors)
	}

	ev, ok = b.Build(conn, msg('Q', []byte("select 3\x00")), time.Now())
	if !ok || ev == nil {
		t.Fatalf("expected subsequent valid message to parse, got (%+v,%v)", ev, ok)
	}
	if ev.Seq != 1 {
		t.Errorf("Seq = %d, want 1", ev.Seq)
	}
}

// TestBuild_UnrecognizedMessageTypeNotParseError verifies that an unrecognized
// message type byte (e.g. a Greenplum private extension) produces no event and
// is NOT recorded as a parse error, and that subsequent valid messages parse
// normally (R3.11).
func TestBuild_UnrecognizedMessageTypeNotParseError(t *testing.T) {
	var parseErrors int
	b := NewEventBuilderWithHook(
		Config{ExtendedQuery: true, MaxSQLLength: 1 << 20},
		func() { parseErrors++ },
	)
	conn := connID(4004)

	// Type byte 'W' is not a SQL-bearing message the EventBuilder recognizes.
	ev, ok := b.Build(conn, msg('W', []byte("gp-extension payload")), time.Now())
	if ok || ev != nil {
		t.Fatalf("expected unrecognized type to yield no event, got (%+v,%v)", ev, ok)
	}
	if parseErrors != 0 {
		t.Fatalf("unrecognized type must not be a parse error, hook called %d times", parseErrors)
	}

	ev, ok = b.Build(conn, msg('Q', []byte("select 4\x00")), time.Now())
	if !ok || ev == nil || ev.Seq != 1 {
		t.Fatalf("expected subsequent valid message at Seq 1, got (%+v,%v)", ev, ok)
	}
}

// TestBuild_OversizedBoundaryExactLengthKept verifies the boundary: SQL exactly
// at MaxSQLLength is kept; one byte over is skipped (R3.5).
func TestBuild_OversizedBoundaryExactLengthKept(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true, MaxSQLLength: 10})
	conn := connID(5005)

	exact := strings.Repeat("b", 10) // == MaxSQLLength
	ev, ok := b.Build(conn, msg('Q', append([]byte(exact), 0)), time.Now())
	if !ok || ev == nil {
		t.Fatalf("SQL exactly at MaxSQLLength should be kept, got (%+v,%v)", ev, ok)
	}
	if len(ev.SQL) != 10 {
		t.Errorf("len(SQL) = %d, want 10", len(ev.SQL))
	}
}

// TestBuild_DefaultMaxSQLLengthAppliedWhenUnset verifies that an unset
// MaxSQLLength falls back to the 1 MiB default rather than treating everything
// as oversized (R3.5).
func TestBuild_DefaultMaxSQLLengthAppliedWhenUnset(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true}) // MaxSQLLength == 0
	conn := connID(6006)

	ev, ok := b.Build(conn, msg('Q', []byte("select 1\x00")), time.Now())
	if !ok || ev == nil {
		t.Fatalf("expected normal SQL to be kept under default max length, got (%+v,%v)", ev, ok)
	}
}

// TestBuild_NilParseErrorHookSafe verifies that skipping an oversized message
// does not panic when no hook is wired (R3.6 nil-safe seam).
func TestBuild_NilParseErrorHookSafe(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true, MaxSQLLength: 4})
	conn := connID(7007)

	ev, ok := b.Build(conn, msg('Q', append([]byte("toolong"), 0)), time.Now())
	if ok || ev != nil {
		t.Fatalf("expected oversized message to be skipped, got (%+v,%v)", ev, ok)
	}
}
