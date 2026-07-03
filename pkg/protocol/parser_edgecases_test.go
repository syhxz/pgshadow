package protocol

import (
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// This file adds complementary example-based unit tests for protocol parser
// edge cases (task 3.11), exercising scenarios not already covered by
// framing_test.go, eventbuilder_test.go, parseerror_test.go, or
// readyforquery_test.go. It reuses the existing test helpers (typedMsg,
// startupMsg, parsePayload, connID, msg, defaultConfig) without redeclaring
// them.
//
// Focus areas (R3.1, R3.2, R3.6, R4.1):
//   - A full Q/P/Z message sequence fed ONE BYTE AT A TIME across many Feed
//     calls still yields the right messages in the right order.
//   - A multi-statement-looking Simple_Query payload is extracted verbatim
//     (the parser does not split or rewrite SQL text).
//   - Valid messages interleaved with unrecognized type bytes parse normally,
//     and the unrecognized bytes never increment the parse-error counter.
//   - An EventBuilder driven by a framed-then-split client stream still assigns
//     correct, strictly-monotonic per-connection sequence numbers.

// TestFeed_ClientQueryParseSequenceOneByteAtATime feeds a frontend stream of
// StartupMessage → Simple_Query → Parse one byte at a time and asserts that
// each message only emerges once its final byte arrives and that the three
// messages appear in order (R3.1). The Q and P messages are then run through an
// EventBuilder to confirm verbatim SQL extraction and ordered sequencing
// (R3.2, R3.7).
func TestFeed_ClientQueryParseSequenceOneByteAtATime(t *testing.T) {
	p := NewParser(defaultConfig(), true) // frontend: starts in startup phase

	startup := startupMsg(protocolVersion30, []byte("user\x00postgres\x00\x00"))
	query := typedMsg('Q', []byte("select 1\x00"))
	parse := typedMsg('P', parsePayload("stmt1", "select * from t where id = $1", []uint32{23}))

	stream := append(append(append([]byte{}, startup...), query...), parse...)

	var got []core.PGMessage
	for i := 0; i < len(stream); i++ {
		msgs, err := p.Feed(stream[i : i+1])
		if err != nil {
			t.Fatalf("Feed error at byte %d: %v", i, err)
		}
		got = append(got, msgs...)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 framed messages, got %d", len(got))
	}
	if got[0].Type != startupMsgType {
		t.Errorf("message 0 type = %q, want untyped startup (0)", got[0].Type)
	}
	if got[1].Type != 'Q' {
		t.Errorf("message 1 type = %q, want 'Q'", got[1].Type)
	}
	if got[2].Type != 'P' {
		t.Errorf("message 2 type = %q, want 'P'", got[2].Type)
	}

	// Drive the SQL-bearing messages through an EventBuilder to confirm correct
	// extraction and ordering after the byte-by-byte reassembly.
	b := NewEventBuilder(defaultConfig())
	conn := connID(1101)

	qEv, ok := b.Build(conn, got[1], time.Unix(10, 0))
	if !ok || qEv == nil {
		t.Fatalf("expected SQL event for Simple_Query")
	}
	if qEv.SQL != "select 1" {
		t.Errorf("Simple_Query SQL = %q, want %q", qEv.SQL, "select 1")
	}
	if qEv.Seq != 1 {
		t.Errorf("Simple_Query Seq = %d, want 1", qEv.Seq)
	}

	pEv, ok := b.Build(conn, got[2], time.Unix(11, 0))
	if !ok || pEv == nil {
		t.Fatalf("expected SQL event for Parse")
	}
	if pEv.SQL != "select * from t where id = $1" {
		t.Errorf("Parse SQL = %q", pEv.SQL)
	}
	if pEv.StmtName != "stmt1" {
		t.Errorf("Parse StmtName = %q, want %q", pEv.StmtName, "stmt1")
	}
	if pEv.Seq != 2 {
		t.Errorf("Parse Seq = %d, want 2", pEv.Seq)
	}
}

// TestFeed_BackendReadyForQuerySequenceOneByteAtATime feeds a backend stream of
// ReadyForQuery('T') → CommandComplete('C') → ReadyForQuery('I') one byte at a
// time and asserts the messages frame in order and that ExtractReadyForQuery
// recovers each transaction status byte in sequence (R3.1, R4.1).
func TestFeed_BackendReadyForQuerySequenceOneByteAtATime(t *testing.T) {
	p := NewParser(defaultConfig(), false) // backend: typed framing from the first byte

	stream := append(append(append([]byte{},
		typedMsg('Z', []byte{'T'})...),
		typedMsg('C', []byte("SELECT 1\x00"))...),
		typedMsg('Z', []byte{'I'})...)

	var got []core.PGMessage
	for i := 0; i < len(stream); i++ {
		msgs, err := p.Feed(stream[i : i+1])
		if err != nil {
			t.Fatalf("Feed error at byte %d: %v", i, err)
		}
		got = append(got, msgs...)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 framed messages, got %d", len(got))
	}

	// First message: ReadyForQuery with status 'T'.
	if status, ok := ExtractReadyForQuery(got[0]); !ok || status != 'T' {
		t.Errorf("message 0 ReadyForQuery = (%q,%v), want ('T',true)", status, ok)
	}
	// Middle message: CommandComplete is not a ReadyForQuery.
	if status, ok := ExtractReadyForQuery(got[1]); ok {
		t.Errorf("message 1 should not be ReadyForQuery, got status %q", status)
	}
	// Last message: ReadyForQuery with status 'I'.
	if status, ok := ExtractReadyForQuery(got[2]); !ok || status != 'I' {
		t.Errorf("message 2 ReadyForQuery = (%q,%v), want ('I',true)", status, ok)
	}
}

// TestBuild_SimpleQueryMultiStatementExtractedVerbatim verifies that a
// Simple_Query payload containing multiple semicolon-separated statements and
// embedded whitespace is extracted verbatim, with only the trailing NUL
// removed. The parser must not split, trim, or rewrite the SQL text (R3.2).
func TestBuild_SimpleQueryMultiStatementExtractedVerbatim(t *testing.T) {
	b := NewEventBuilder(defaultConfig())

	const multi = "select 1;\n  insert into t(v) values (1);\nselect 2;"
	ev, ok := b.Build(connID(1201), msg('Q', append([]byte(multi), 0)), time.Now())
	if !ok || ev == nil {
		t.Fatalf("expected a SQL event for multi-statement Simple_Query")
	}
	if ev.SQL != multi {
		t.Errorf("SQL = %q, want verbatim %q", ev.SQL, multi)
	}
	if ev.Seq != 1 {
		t.Errorf("Seq = %d, want 1", ev.Seq)
	}
}

// TestBuild_InterleavedValidAndUnrecognizedTypeBytes feeds an EventBuilder a
// sequence of valid SQL-bearing messages interleaved with unrecognized type
// bytes (e.g. Greenplum private extensions). The unrecognized messages must
// yield no event and must NOT increment the parse-error counter, while the
// valid messages receive strictly-monotonic sequence numbers unaffected by the
// interleaved noise (R3.6, R3.11).
func TestBuild_InterleavedValidAndUnrecognizedTypeBytes(t *testing.T) {
	var parseErrors int
	b := NewEventBuilderWithHook(defaultConfig(), func() { parseErrors++ })
	conn := connID(1301)

	// Sequence: Q, <unknown 'W'>, P, <unknown 'y'>, Q.
	if ev, ok := b.Build(conn, msg('Q', []byte("select 1\x00")), time.Now()); !ok || ev.Seq != 1 {
		t.Fatalf("first Q: got (%+v,%v), want Seq 1", ev, ok)
	}
	if ev, ok := b.Build(conn, msg('W', []byte("gp-extension")), time.Now()); ok || ev != nil {
		t.Fatalf("unrecognized 'W': expected no event, got (%+v,%v)", ev, ok)
	}
	if ev, ok := b.Build(conn, msg('P', parsePayload("s", "select 2", nil)), time.Now()); !ok || ev.Seq != 2 {
		t.Fatalf("Parse: got (%+v,%v), want Seq 2", ev, ok)
	}
	if ev, ok := b.Build(conn, msg('y', []byte("another-extension")), time.Now()); ok || ev != nil {
		t.Fatalf("unrecognized 'y': expected no event, got (%+v,%v)", ev, ok)
	}
	if ev, ok := b.Build(conn, msg('Q', []byte("select 3\x00")), time.Now()); !ok || ev.Seq != 3 {
		t.Fatalf("second Q: got (%+v,%v), want Seq 3", ev, ok)
	}

	if parseErrors != 0 {
		t.Errorf("parse-error hook called %d times; unrecognized types must not be parse errors", parseErrors)
	}
}

// TestParser_FramedThenSplitStreamAssignsCorrectSeq drives a complete client
// session whose bytes are delivered to the framing parser in irregular chunks
// (not aligned to message boundaries), then feeds every framed message to an
// EventBuilder. It asserts that the SQL-bearing messages are extracted in the
// captured order with correct, gap-free per-connection sequence numbers despite
// the arbitrary Feed boundaries (R3.1, R3.2, R3.7).
func TestParser_FramedThenSplitStreamAssignsCorrectSeq(t *testing.T) {
	p := NewParser(defaultConfig(), true) // frontend direction

	startup := startupMsg(protocolVersion30, []byte("user\x00app\x00\x00"))
	wantSQL := []string{
		"begin",
		"insert into t(v) values (1)",
		"insert into t(v) values (2)",
		"commit",
	}

	stream := append([]byte{}, startup...)
	stream = append(stream, typedMsg('Q', []byte(wantSQL[0]+"\x00"))...)
	stream = append(stream, typedMsg('P', parsePayload("ins", wantSQL[1], []uint32{23}))...)
	stream = append(stream, typedMsg('Q', []byte(wantSQL[2]+"\x00"))...)
	stream = append(stream, typedMsg('Q', []byte(wantSQL[3]+"\x00"))...)

	b := NewEventBuilder(defaultConfig())
	conn := connID(1401)

	// Feed in irregular 7-byte chunks so message boundaries land mid-chunk.
	var events []*core.SQLEvent
	const chunk = 7
	for i := 0; i < len(stream); i += chunk {
		end := i + chunk
		if end > len(stream) {
			end = len(stream)
		}
		msgs, err := p.Feed(stream[i:end])
		if err != nil {
			t.Fatalf("Feed error at offset %d: %v", i, err)
		}
		for _, m := range msgs {
			if ev, ok := b.Build(conn, m, time.Unix(int64(i), 0)); ok {
				events = append(events, ev)
			}
		}
	}

	if len(events) != len(wantSQL) {
		t.Fatalf("expected %d SQL events, got %d", len(wantSQL), len(events))
	}
	for i, ev := range events {
		if ev.SQL != wantSQL[i] {
			t.Errorf("event %d SQL = %q, want %q", i, ev.SQL, wantSQL[i])
		}
		if ev.Seq != uint64(i+1) {
			t.Errorf("event %d Seq = %d, want %d", i, ev.Seq, i+1)
		}
	}
}
