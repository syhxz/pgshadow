// This file implements SQL extraction and the EventBuilder (R3.2, R3.3, R3.4,
// R3.7) — task 3.2.
//
// The EventBuilder consumes the framed core.PGMessage values produced by the
// framing layer (framing.go, task 3.1) and turns the client-side messages that
// carry SQL into core.SQLEvent values. Two message kinds carry SQL:
//
//   - Simple_Query ('Q'): the entire payload is the SQL text with a single
//     trailing NUL terminator, which is stripped (R3.2).
//   - Parse ('P'): the Extended_Query prepared-statement definition, framed as
//     a NUL-terminated statement name, a NUL-terminated SQL string, and a list
//     of declared parameter type OIDs. Parse is only treated as SQL-bearing
//     when the extended query flag is enabled (R3.3, R3.4).
//
// Every produced event is stamped with a strictly-monotonic per-ConnID sequence
// number (R3.7), which is the basis for the end-to-end per-connection ordering
// property. Messages that carry no SQL (Bind, Sync, Execute, Describe, Close,
// Flush, Terminate, CopyData/CopyDone/CopyFail, and the untyped startup
// message) produce no event and do not advance the sequence.
//
// TxID assignment (TxIDGenerator, task 3.4), Bind→Parse parameter-value
// correlation (BindCorrelator, task 3.5), COPY data-stream capture (CopyBuffer,
// task 3.6), oversized/malformed handling (task 3.3), and source execution
// timing (task 3.7) are implemented by other tasks; this file references those
// collaborators only where the stub fields already exist and never duplicates
// their logic.
package protocol

import (
	"bytes"
	"encoding/binary"
	"time"

	"pgshadow/pkg/core"
)

// Client→server message type bytes that the EventBuilder inspects. Only Simple
// Query and Parse carry SQL text; the rest are listed for documentation and to
// make the "no SQL" branch explicit.
const (
	msgSimpleQuery byte = 'Q' // Simple_Query (R3.2)
	msgParse       byte = 'P' // Parse / Extended_Query statement definition (R3.3)
)

// EventBuilder converts client-side PGMessages into SQL_Events and stamps each
// with a strictly-monotonic per-ConnID sequence number (R3.7). It also holds
// references to the collaborators implemented by later tasks (TxID generation,
// Bind correlation, COPY buffering); those remain nil until wired by their
// respective tasks and Build degrades gracefully when they are absent.
type EventBuilder struct {
	cfg          Config                      // parser config; ExtendedQuery gates Parse (R3.4)
	seq          map[core.ConnID]uint64      // per-connection monotonic sequence (R3.7)
	txGen        *TxIDGenerator              // TxID allocation (R3.8, task 3.4)
	binder       *BindCorrelator             // P→B correlation (R3.9, task 3.5)
	copyState    map[core.ConnID]*CopyBuffer // COPY data stream tracking (R3.10, task 3.6)
	onParseError ParseErrorHook              // nil-safe parse-error metrics hook (R3.6, task 3.3)
}

// NewEventBuilder constructs an EventBuilder for the given parser configuration
// with no parse-error metrics hook. The extended query flag defaults to true
// when unset is the responsibility of config loading (R3.4); the builder honors
// whatever value cfg carries.
func NewEventBuilder(cfg Config) *EventBuilder {
	return NewEventBuilderWithHook(cfg, nil)
}

// NewEventBuilderWithHook constructs an EventBuilder, wiring hook (which may be
// nil) as the callback invoked whenever a malformed or oversized message is
// skipped (R3.6). The hook is the only coupling to the metrics layer and is
// injected to avoid an import cycle, mirroring filter.NewFilterWithHook.
func NewEventBuilderWithHook(cfg Config, hook ParseErrorHook) *EventBuilder {
	return &EventBuilder{
		cfg:          cfg,
		seq:          make(map[core.ConnID]uint64),
		copyState:    make(map[core.ConnID]*CopyBuffer),
		onParseError: hook,
	}
}

// Build extracts a SQL_Event from a single client-side message. It returns
// (event, true) when the message carries SQL, or (nil, false) when it does not
// (e.g. Bind, Sync). The returned event carries the next strictly-monotonic
// sequence number for the connection (R3.7).
//
// Oversized SQL (longer than MaxSQLLength) and malformed messages (e.g. a Parse
// payload that cannot be decoded) are skipped and recorded through the
// parse-error hook, and parsing continues on the same connection (R3.5, R3.6).
func (b *EventBuilder) Build(conn core.ConnID, msg core.PGMessage, ts time.Time) (*core.SQLEvent, bool) {
	switch msg.Type {
	case msgSimpleQuery:
		sql := extractSimpleQuerySQL(msg.Payload)
		if b.oversized(sql) {
			// SQL longer than MaxSQLLength is oversized: skip, record, and
			// continue without advancing the per-connection sequence (R3.5, R3.6).
			b.recordParseError()
			return nil, false
		}
		return b.emit(conn, ts, sql, "", false, nil), true

	case msgParse:
		// Parse is only SQL-bearing when extended query handling is enabled
		// (R3.3, R3.4). When disabled, the Extended_Query path is ignored.
		if !b.cfg.ExtendedQuery {
			return nil, false
		}
		stmtName, sql, oids, ok := parseParseMessage(msg.Payload)
		if !ok {
			// Malformed Parse payload (e.g. missing NUL terminators): skip,
			// record, and continue parsing the same connection (R3.6).
			b.recordParseError()
			return nil, false
		}
		if b.oversized(sql) {
			b.recordParseError()
			return nil, false
		}
		return b.emit(conn, ts, sql, stmtName, true, oids), true

	default:
		// Bind, Sync, Execute, Describe, Close, Flush, Terminate, CopyData,
		// CopyDone, CopyFail, the untyped startup message, and any unrecognized
		// type byte (including Greenplum private extensions, R3.11) carry no SQL
		// at this stage. They produce no event, do not advance the
		// per-connection sequence, and are NOT recorded as parse errors: an
		// unrecognized message type is not a parse error (R3.11).
		return nil, false
	}
}

// oversized reports whether the extracted SQL text exceeds the effective
// maximum SQL length, marking the originating message as oversized (R3.5).
// A zero or negative effective limit is treated as "no limit" (defense-in-depth
// against misconfiguration silently discarding all SQL).
func (b *EventBuilder) oversized(sql string) bool {
	max := effectiveMaxSQLLength(b.cfg)
	if max <= 0 {
		return false
	}
	return len(sql) > max
}

// recordParseError invokes the parse-error metrics hook when one is wired. It is
// nil-safe so the EventBuilder works with or without a Collector attached (R3.6).
func (b *EventBuilder) recordParseError() {
	if b.onParseError != nil {
		b.onParseError()
	}
}

// emit constructs a SQL_Event for the connection, assigning the next sequence
// number and attaching declared parameter OIDs (values are filled later by Bind
// correlation, task 3.5). It centralizes sequence allocation so that every
// SQL-bearing path advances the per-ConnID counter exactly once.
func (b *EventBuilder) emit(conn core.ConnID, ts time.Time, sql, stmtName string, extended bool, oids []uint32) *core.SQLEvent {
	if b.seq == nil {
		b.seq = make(map[core.ConnID]uint64)
	}
	b.seq[conn]++
	seq := b.seq[conn]

	ev := &core.SQLEvent{
		Conn:      conn,
		SQL:       sql,
		Timestamp: ts,
		Seq:       seq,
		Extended:  extended,
		StmtName:  stmtName,
	}

	// Declared parameter type OIDs from the Parse message (R3.3). Actual
	// parameter values are correlated from the matching Bind message by
	// task 3.5; here only the OIDs are known.
	if len(oids) > 0 {
		ev.Params = make([]core.ParamInfo, len(oids))
		for i, oid := range oids {
			ev.Params[i] = core.ParamInfo{OID: oid}
		}
	}

	// TxID assignment is owned by the TxIDGenerator (task 3.4). Reference it
	// only when present so this task neither duplicates nor blocks that logic.
	if b.txGen != nil {
		ev.TxID = b.txGen.Current(conn)
	}

	return ev
}

// extractSimpleQuerySQL returns the SQL text of a Simple_Query ('Q') message:
// the payload with a single trailing NUL terminator removed (R3.2). A payload
// without a trailing NUL is returned unchanged.
func extractSimpleQuerySQL(payload []byte) string {
	if n := len(payload); n > 0 && payload[n-1] == 0 {
		return string(payload[:n-1])
	}
	return string(payload)
}

// parseParseMessage decodes a Parse ('P') message payload into its statement
// name, SQL text, and declared parameter type OIDs (R3.3). The wire format is:
//
//	String  statement name (NUL-terminated; empty for the unnamed statement)
//	String  query text     (NUL-terminated)
//	Int16   number of parameter type OIDs (n)
//	Int32   parameter type OID  × n
//
// It returns ok=false when the two required NUL-terminated strings cannot be
// located. A truncated or absent parameter-count section is tolerated: the
// statement name and SQL are still returned with as many OIDs as are present.
func parseParseMessage(payload []byte) (stmtName, sql string, oids []uint32, ok bool) {
	i := bytes.IndexByte(payload, 0)
	if i < 0 {
		return "", "", nil, false
	}
	stmtName = string(payload[:i])
	rest := payload[i+1:]

	j := bytes.IndexByte(rest, 0)
	if j < 0 {
		return "", "", nil, false
	}
	sql = string(rest[:j])
	rest = rest[j+1:]

	// Parameter type OIDs are optional from this layer's perspective: if the
	// Int16 count is missing or the OID array is truncated, return what we have.
	if len(rest) < 2 {
		return stmtName, sql, nil, true
	}
	n := int(binary.BigEndian.Uint16(rest[:2]))
	rest = rest[2:]
	for k := 0; k < n && len(rest) >= 4; k++ {
		oids = append(oids, binary.BigEndian.Uint32(rest[:4]))
		rest = rest[4:]
	}
	return stmtName, sql, oids, true
}
