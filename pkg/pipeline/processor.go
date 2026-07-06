// Package pipeline implements the stream processor that turns ordered
// per-direction byte streams into filtered SQL_Events on the queue.
//
// It owns the per-ConnID parser/sequence state and the standalone protocol
// collaborators (EventBuilder, TxIDGenerator, BindCorrelator, CopyTracker,
// SourceExecTimer), wiring them into one place.
//
// Concurrency: every StreamHandler callback (OnBytes/OnClose) is invoked from
// the single processing goroutine that drives the reassembler, so the
// processor's own maps need no locking. The collaborators it calls
// (StateMachine, TxIDGenerator, BindCorrelator) carry their own synchronization
// and the queue is safe for concurrent producers/consumers.
package pipeline

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"time"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/protocol"
	"pgshadow/pkg/queue"
)

// Client→server message type bytes handled directly by the stream processor.
// EventBuilder owns Simple_Query ('Q') and Parse ('P') SQL extraction; the
// Extended_Query execution sub-protocol (Bind, the COPY data stream) is driven
// here because it spans multiple messages and multiple collaborators.
const (
	MsgParse    byte = 'P' // Parse: Extended_Query statement definition (R3.3)
	MsgBind     byte = 'B' // Bind: actual parameter values for execution (R3.9)
	MsgCopyData byte = 'd' // CopyData: one COPY FROM payload chunk (R3.10)
	MsgCopyDone byte = 'c' // CopyDone: COPY FROM finished (R3.10)
	MsgCopyFail byte = 'f' // CopyFail: COPY FROM aborted (R3.10)
)

// msgStartup is the synthetic type byte used for untyped startup-phase messages
// (StartupMessage, SSLRequest, CancelRequest). See protocol.startupMsgType.
const msgStartup byte = 0

// cancelRequestCode identifies a CancelRequest message (Issue #5). The first 4
// bytes of the startup payload carry this code. CancelRequest connections are
// ephemeral (no SQL follows) and should not accumulate protocol state.
const cancelRequestCode uint32 = 80877102

// Server→client message type bytes that bound a backend command. ReadyForQuery
// drives the transaction state machine (R12.1/R4.1); both close a statement for
// source execution timing (R10.7).
const (
	MsgCommandComplete byte = 'C' // CommandComplete: statement finished (R10.7)
	MsgReadyForQuery   byte = 'Z' // ReadyForQuery: transaction status (R4.1)
)

// ConnParsers holds the two stateful framing parsers for one connection: one
// for each direction. PG framing is per-(ConnID, direction): the client stream
// begins with the untyped startup message while the server stream is typed from
// its first byte (see pkg/protocol framing).
type ConnParsers struct {
	Client protocol.Parser
	Server protocol.Parser
}

// StreamProcessor is the capture.StreamHandler that turns ordered per-direction
// byte streams into filtered SQL_Events on the queue. It owns the per-ConnID
// parser/sequence state and the standalone protocol collaborators delivered by
// tasks 3.4–3.7, wiring them into one place (their intended integration point).
type StreamProcessor struct {
	cfg       protocol.Config
	q         queue.Queue
	collector metrics.Collector

	sm     filter.StateMachine
	flt    filter.Filter
	cls    filter.Classifier
	eb     *protocol.EventBuilder
	txGen  *protocol.TxIDGenerator
	binder *protocol.BindCorrelator
	copies *protocol.CopyTracker
	timer  *protocol.SourceExecTimer

	parsers      map[core.ConnID]*ConnParsers
	seq          map[core.ConnID]uint64
	lastSQL      map[core.ConnID]string
	onBytesCount int

	// DebugMode enables verbose pipeline debug logging.
	DebugMode bool
}

// NewStreamProcessor builds the stream processor and its collaborators from the
// parser and filter configuration. The metrics collector (which may be nil in
// tests) is wired through nil-safe hooks: parse-error skips increment
// Collector.ParseError (R3.6) and every filter decision increments
// Collector.Classified (R5). Both hooks are the import-cycle-avoiding adapters
// the protocol and filter packages expose. It returns an error only when the
// filter mode is invalid; configuration validation normally rejects that first.
func NewStreamProcessor(pcfg protocol.Config, fcfg filter.Config, q queue.Queue, collector metrics.Collector) (*StreamProcessor, error) {
	parseHook := func() {
		if collector != nil {
			collector.ParseError()
		}
	}
	classifiedHook := func(class filter.StmtClass, kept bool) {
		if collector != nil {
			collector.Classified(class, kept)
		}
	}

	flt, err := filter.NewFilterWithHook(fcfg, classifiedHook)
	if err != nil {
		return nil, err
	}

	return &StreamProcessor{
		cfg:       pcfg,
		q:         q,
		collector: collector,
		sm:        filter.NewStateMachine(),
		flt:       flt,
		cls:       filter.NewClassifier(fcfg.BuiltinFunctions),
		eb:        protocol.NewEventBuilderWithHook(pcfg, parseHook),
		txGen:     protocol.NewTxIDGenerator(),
		binder:    protocol.NewBindCorrelator(),
		copies:    protocol.NewCopyTracker(),
		timer:     protocol.NewSourceExecTimer(),
		parsers:   make(map[core.ConnID]*ConnParsers),
		seq:       make(map[core.ConnID]uint64),
		lastSQL:   make(map[core.ConnID]string),
	}, nil
}

// OnBytesCount returns the number of OnBytes invocations (for testing).
func (sp *StreamProcessor) OnBytesCount() int {
	return sp.onBytesCount
}

// OnBytes frames the contiguous bytes for one direction of one connection and
// dispatches each framed message. A framing error is recorded as a parse error
// and processing continues with whatever messages were framed before it, so one
// corrupt message never tears down a connection's stream (R3.6).
func (sp *StreamProcessor) OnBytes(conn core.ConnID, fromClient bool, data []byte) {
	sp.onBytesCount++
	if sp.DebugMode && (sp.onBytesCount <= 20 || sp.onBytesCount%100 == 0) {
		fmt.Fprintf(os.Stderr, "pgshadow: DEBUG OnBytes #%d conn=%v fromClient=%v len=%d\n", sp.onBytesCount, conn, fromClient, len(data))
	}
	cp := sp.parsersFor(conn)
	p := cp.Server
	if fromClient {
		p = cp.Client
	}

	msgs, err := p.Feed(data)
	if sp.DebugMode && (sp.onBytesCount <= 20 || sp.onBytesCount%100 == 0) {
		fmt.Fprintf(os.Stderr, "pgshadow: DEBUG OnBytes #%d msgs=%d err=%v\n", sp.onBytesCount, len(msgs), err)
		if len(data) >= 4 && len(msgs) == 0 {
			fmt.Fprintf(os.Stderr, "pgshadow: DEBUG   data[0:4]=%x\n", data[0:4])
		}
		for i, m := range msgs {
			fmt.Fprintf(os.Stderr, "pgshadow: DEBUG   msg[%d] type=%c(%d) len=%d\n", i, m.Type, m.Type, m.Length)
		}
	}
	if err != nil && sp.collector != nil {
		sp.collector.ParseError()
	}

	// The capture layer does not surface a per-chunk timestamp through the
	// StreamHandler, so processing time is the observation point for pacing and
	// source-exec timing. Using one timestamp per chunk keeps all messages in a
	// chunk consistent.
	ts := time.Now()
	for _, m := range msgs {
		if fromClient {
			sp.handleClient(conn, m, ts)
		} else {
			sp.handleServer(conn, m, ts)
		}
	}
}

// OnClose releases every piece of per-connection state when the connection is
// observed to close or is flushed as idle (R2.5), so nothing leaks for dead
// connections.
func (sp *StreamProcessor) OnClose(conn core.ConnID) {
	sp.sm.Forget(conn)
	sp.binder.Forget(conn)
	sp.copies.Discard(conn)
	sp.timer.Forget(conn)
	sp.txGen.Reset(conn)
	delete(sp.parsers, conn)
	delete(sp.seq, conn)
	delete(sp.lastSQL, conn)
}

// parsersFor returns (creating on first use) the per-direction framing parsers
// for a connection.
func (sp *StreamProcessor) parsersFor(conn core.ConnID) *ConnParsers {
	cp := sp.parsers[conn]
	if cp == nil {
		cp = &ConnParsers{
			Client: protocol.NewParser(sp.cfg, true),
			Server: protocol.NewParser(sp.cfg, false),
		}
		sp.parsers[conn] = cp
	}
	return cp
}

// resetPoolerSession clears per-connection protocol state when a connection
// pooler (PgBouncer/Odyssey) reassigns the server connection to a different
// client session. The signal is a DISCARD ALL, RESET ALL, or DEALLOCATE ALL
// statement which poolers send as server_reset_query between sessions. This
// prevents prepared statement cross-contamination across logical sessions that
// share the same TCP four-tuple (Issue #1).
func (sp *StreamProcessor) resetPoolerSession(conn core.ConnID) {
	sp.binder.Forget(conn)
	sp.copies.Discard(conn)
	sp.txGen.Reset(conn)
	sp.timer.Forget(conn)
}

// isPoolerResetQuery returns true if sql is a session-reset command typically
// issued by a connection pooler between client sessions.
func isPoolerResetQuery(sql string) bool {
	if len(sql) < 9 { // shortest match: "RESET ALL" = 9 chars
		return false
	}
	upper := strings.ToUpper(strings.TrimSpace(sql))
	return upper == "DISCARD ALL" ||
		upper == "RESET ALL" ||
		upper == "DEALLOCATE ALL" ||
		strings.HasPrefix(upper, "DISCARD ALL;") ||
		strings.HasPrefix(upper, "RESET ALL;") ||
		strings.HasPrefix(upper, "DEALLOCATE ALL;")
}

// handleClient routes one client→server message. Simple_Query is extracted by
// the EventBuilder and dispatched immediately. Parse defines a prepared
// statement (registered for later Bind correlation) but does not itself emit a
// replayable event — the event is produced when the Bind supplies parameter
// values (R3.9). COPY FROM buffers its data stream and emits a single event,
// with the payload attached, on CopyDone (R3.10).
func (sp *StreamProcessor) handleClient(conn core.ConnID, msg core.PGMessage, ts time.Time) {
	// Issue #5: CancelRequest is a startup-phase message (type=0) on an ephemeral
	// TCP connection. It carries no SQL and the connection closes immediately after.
	// Skip it to avoid creating orphan ConnID state with no useful content.
	if msg.Type == msgStartup && len(msg.Payload) >= 4 {
		code := binary.BigEndian.Uint32(msg.Payload[0:4])
		if code == cancelRequestCode {
			return
		}
	}

	switch msg.Type {
	case MsgParse:
		// Parse carries SQL + declared parameter OIDs. Record it so a later
		// Bind on the same connection can be reunited with the SQL and OIDs.
		if ev, ok := sp.eb.Build(conn, msg, ts); ok {
			sp.binder.OnParse(conn, ev.StmtName, ev.SQL, oidsOf(ev.Params))
		}

	case MsgBind:
		// Bind supplies the actual parameter values; correlate them with the
		// Parse to build a complete parameterized event (R3.9). An orphaned or
		// malformed Bind correlates to nothing and is skipped.
		if res, ok := sp.binder.OnBind(conn, msg.Payload); ok {
			ev := &core.SQLEvent{
				Conn:      conn,
				SQL:       res.SQL,
				StmtName:  res.StmtName,
				Params:    res.Params,
				Extended:  true,
				Timestamp: ts,
			}
			sp.dispatch(conn, ev, ts)
			// Bind begins an execution that the backend will run (R10.7).
			// Only start the timer when the Bind successfully correlated to a
			// Parse; orphan Binds do not produce events and must not pollute
			// the SourceExecTime metric with unrelated timings.
			sp.timer.OnClientQuery(conn, ts)
		} else {
			// Issue #20: Orphaned Bind — no matching Parse on this connection.
			// This typically happens after pgshadow restart when existing
			// connections had Parsed statements before capture began. Record as
			// a parse error so it's visible in metrics and operators know
			// Extended Query events are being lost until clients re-Parse.
			if sp.collector != nil {
				sp.collector.ParseError()
			}
		}

	case MsgCopyData:
		sp.copies.Data(conn, msg.Payload)

	case MsgCopyDone:
		if sql, data, ok := sp.copies.Done(conn); ok {
			ev := &core.SQLEvent{Conn: conn, SQL: sql, CopyData: data, Timestamp: ts}
			sp.dispatch(conn, ev, ts)
		}

	case MsgCopyFail:
		sp.copies.Fail(conn)

	default:
		// Simple_Query ('Q') and any other SQL-bearing message recognized by
		// the EventBuilder. Non-SQL messages return ok=false and are ignored.
		ev, ok := sp.eb.Build(conn, msg, ts)
		if !ok {
			return
		}
		if sp.cls.Classify(ev.SQL) == filter.ClassCopyFrom {
			// COPY ... FROM STDIN: the rows arrive in the CopyData stream that
			// follows. Buffer them and emit the event, with the payload
			// attached, on CopyDone (R3.10).
			sp.copies.Start(conn, ev.SQL)
			sp.timer.OnClientQuery(conn, ts)
			return
		}
		sp.dispatch(conn, ev, ts)
		sp.timer.OnClientQuery(conn, ts)
	}
}

// dispatch stamps the event with its transaction id and per-connection sequence
// number, applies the filter decision against the current transaction state,
// enqueues kept events, and then advances the transaction/TxID state for the
// next statement.
//
// Sequence numbers are assigned here (not taken from the EventBuilder) so that
// every emission path — Simple_Query, Bind-correlated Extended_Query, and
// CopyDone — draws from a single strictly-monotonic per-ConnID counter (R3.7).
func (sp *StreamProcessor) dispatch(conn core.ConnID, ev *core.SQLEvent, ts time.Time) {
	class := sp.cls.Classify(ev.SQL)

	// TxID grouping (R3.8): a new group begins on BEGIN, or on the first
	// DML/DDL/COPY observed while no group is active (implicit transaction).
	switch {
	case class == filter.ClassBegin:
		sp.txGen.Advance(conn)
	case sp.txGen.Current(conn) == 0 &&
		(class == filter.ClassDML || class == filter.ClassDDL || class == filter.ClassCopyFrom):
		sp.txGen.Advance(conn)
	}
	ev.TxID = sp.txGen.Current(conn)

	sp.seq[conn]++
	ev.Seq = sp.seq[conn]

	// Remember the statement so source execution time (R10.7) can be attributed
	// when the backend signals completion on the server stream.
	sp.lastSQL[conn] = ev.SQL

	state := sp.sm.State(conn)
	action, _ := sp.flt.Decide(ev, state)
	if action == filter.Keep {
		sp.q.Enqueue(ev)
	}

	// Advance unidirectional transaction inference for the next statement
	// (R4.3–R4.5); the authoritative bidirectional update is OnReadyForQuery.
	sp.sm.OnStatement(conn, class)
	if class == filter.ClassCommitRollback {
		sp.txGen.Reset(conn)
	}

	// Connection pooler session boundary detection (Issue #1): when a pooler
	// (PgBouncer/Odyssey) reuses a server connection for a different client,
	// it sends DISCARD ALL / RESET ALL / DEALLOCATE ALL as its
	// server_reset_query. Clear per-connection protocol state so that prepared
	// statements from the previous logical session don't contaminate the next.
	if sp.cfg.ConnectionPooler && class == filter.ClassUtility && isPoolerResetQuery(ev.SQL) {
		sp.resetPoolerSession(conn)
	}
}

// handleServer routes one server→client message. ReadyForQuery is the
// authoritative, bidirectional transaction-state signal routed into the state
// machine (R12.1, R4.1). ReadyForQuery and CommandComplete also close a
// statement for source-side execution timing (R10.7).
func (sp *StreamProcessor) handleServer(conn core.ConnID, msg core.PGMessage, ts time.Time) {
	switch msg.Type {
	case MsgReadyForQuery:
		if status, ok := protocol.ExtractReadyForQuery(msg); ok {
			sp.sm.OnReadyForQuery(conn, status)
		}
		sp.recordExec(conn, ts)
	case MsgCommandComplete:
		sp.recordExec(conn, ts)
	}
}

// recordExec computes the source-side execution time for the just-completed
// statement and records it to metrics, leaving the target side zero (it is
// filled by the replayer). A completion with no pending request, or a
// non-positive interval, is ignored.
func (sp *StreamProcessor) recordExec(conn core.ConnID, ts time.Time) {
	d, ok := sp.timer.OnBackendComplete(conn, ts)
	if !ok || d <= 0 {
		return
	}
	if sp.collector != nil {
		sp.collector.ExecTime(sp.lastSQL[conn], d, 0)
	}
}

// oidsOf extracts the declared parameter type OIDs from a Parse-built event's
// parameter slice for handing to the BindCorrelator.
func oidsOf(params []core.ParamInfo) []uint32 {
	if len(params) == 0 {
		return nil
	}
	oids := make([]uint32, len(params))
	for i, p := range params {
		oids[i] = p.OID
	}
	return oids
}

// --- Test accessors ----------------------------------------------------------
// These are exported for white-box testing from cmd/pgshadow tests. They expose
// internal state that is not part of the public API contract.

// StateMachine returns the internal filter state machine (for test assertions).
func (sp *StreamProcessor) StateMachine() filter.StateMachine { return sp.sm }

// HasParser returns whether a parser exists for the given connection.
func (sp *StreamProcessor) HasParser(conn core.ConnID) bool {
	_, ok := sp.parsers[conn]
	return ok
}

// SeqFor returns the current sequence number for a connection.
func (sp *StreamProcessor) SeqFor(conn core.ConnID) uint64 { return sp.seq[conn] }

// HasSeq returns whether a sequence entry exists for the connection.
func (sp *StreamProcessor) HasSeq(conn core.ConnID) bool {
	_, ok := sp.seq[conn]
	return ok
}
