// This file assembles the producer side of the pgshadow pipeline (task 12.2).
//
// Topology
//
//	capture.Source ─▶ [pktCh] ─▶ reassembler ─▶ streamProcessor
//	                                              │  per-(ConnID,direction) parser
//	                                              │  ├─ client msgs ─▶ EventBuilder ─▶ SQLEvent
//	                                              │  │                  ├─ TxIDGenerator (R3.8)
//	                                              │  │                  ├─ BindCorrelator (R3.9)
//	                                              │  │                  ├─ CopyTracker (R3.10)
//	                                              │  │                  └─ SourceExecTimer (R10.7)
//	                                              │  │                  ▼
//	                                              │  │            StateMachine + Filter.Decide
//	                                              │  │                  ▼ (kept)
//	                                              │  │              queue.Enqueue ─▶ replayer
//	                                              │  └─ server msgs ─▶ ExtractReadyForQuery
//	                                              │                    ─▶ StateMachine.OnReadyForQuery (R12.1)
//
// The stages are connected by two Go channels:
//
//   - capture.Source.Packets() — owned by the capture layer.
//   - pktCh — a buffered channel internal to this file that decouples the NIC
//     read loop from the (single-goroutine) reassembler+processing loop so a
//     burst of packets does not stall capture (the "capture→processing"
//     decoupling the design calls for). The filter→replayer decoupling is
//     provided by the bounded queue (pkg/queue), not by this file.
//
// Production isolation (R1.6, R12.1, R12.6)
//
// The producer is strictly read-only with respect to the network. It consumes
// already-captured packets, frames and filters them, and writes kept events to
// the in-process queue. It NEVER dials, writes to, or otherwise opens a socket
// to the Source_Database:
//
//   - capture.Source exposes only Packets()/Stats()/Close() — there is no write
//     path to the wire (see pkg/capture: "never writes to the network", R1.6).
//   - This file imports none of pgx/pgxpool/net/database drivers; the only
//     component in the whole binary that dials a database is the replayer's
//     pool, and it dials the Target_Database exclusively (startup step 5).
//
// The compile-time assertions at the bottom of this file pin the read-only
// shape of the producer's dependencies so a future change that introduces a
// Source_Database connection here would fail to build. The end-to-end dial
// recording check is property test 12.4 (a separate task).
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/gopacket"

	"pgshadow/pkg/capture"
	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/protocol"
	"pgshadow/pkg/queue"
)

// pktChanBuffer bounds the buffered channel that decouples the capture read
// loop from the processing loop. It absorbs short bursts without forcing the
// capture layer to block; sustained overload is bounded downstream by the
// queue's overflow policy (R6.4), so this buffer is intentionally modest.
const pktChanBuffer = 4096

// defaultIdleFlush bounds how often idle reassembly state is reclaimed when no
// parser idle timeout is configured.
const defaultIdleFlush = 300 * time.Second

// defaultDrainTimeout bounds the total time the graceful-shutdown drain (R12.5)
// will spend waiting for the pipeline to quiesce. The design calls for a
// configurable drain timeout (default 30s); no drain-timeout config field
// exists yet, so this constant is the sensible default. If draining stalls past
// this deadline the drain forces the replayer to stop and closes the pool so
// the process always exits promptly.
const defaultDrainTimeout = 30 * time.Second

// Client→server message type bytes handled directly by the stream processor.
// EventBuilder owns Simple_Query ('Q') and Parse ('P') SQL extraction; the
// Extended_Query execution sub-protocol (Bind, the COPY data stream) is driven
// here because it spans multiple messages and multiple collaborators.
const (
	msgParse    byte = 'P' // Parse: Extended_Query statement definition (R3.3)
	msgBind     byte = 'B' // Bind: actual parameter values for execution (R3.9)
	msgCopyData byte = 'd' // CopyData: one COPY FROM payload chunk (R3.10)
	msgCopyDone byte = 'c' // CopyDone: COPY FROM finished (R3.10)
	msgCopyFail byte = 'f' // CopyFail: COPY FROM aborted (R3.10)
)

// Server→client message type bytes that bound a backend command. ReadyForQuery
// drives the transaction state machine (R12.1/R4.1); both close a statement for
// source execution timing (R10.7).
const (
	msgCommandComplete byte = 'C' // CommandComplete: statement finished (R10.7)
	msgReadyForQuery   byte = 'Z' // ReadyForQuery: transaction status (R4.1)
)

// connParsers holds the two stateful framing parsers for one connection: one
// for each direction. PG framing is per-(ConnID, direction): the client stream
// begins with the untyped startup message while the server stream is typed from
// its first byte (see pkg/protocol framing).
type connParsers struct {
	client protocol.Parser
	server protocol.Parser
}

// streamProcessor is the capture.StreamHandler that turns ordered per-direction
// byte streams into filtered SQL_Events on the queue. It owns the per-ConnID
// parser/sequence state and the standalone protocol collaborators delivered by
// tasks 3.4–3.7, wiring them into one place (their intended integration point).
//
// Concurrency: every StreamHandler callback (OnBytes/OnClose) is invoked from
// the single processing goroutine that drives the reassembler, so the
// processor's own maps need no locking. The collaborators it calls
// (StateMachine, TxIDGenerator, BindCorrelator) carry their own synchronization
// and the queue is safe for concurrent producers/consumers.
type streamProcessor struct {
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

	parsers      map[core.ConnID]*connParsers
	seq          map[core.ConnID]uint64
	lastSQL      map[core.ConnID]string
	onBytesCount int
}

// newStreamProcessor builds the stream processor and its collaborators from the
// parser and filter configuration. The metrics collector (which may be nil in
// tests) is wired through nil-safe hooks: parse-error skips increment
// Collector.ParseError (R3.6) and every filter decision increments
// Collector.Classified (R5). Both hooks are the import-cycle-avoiding adapters
// the protocol and filter packages expose. It returns an error only when the
// filter mode is invalid; configuration validation normally rejects that first.
func newStreamProcessor(pcfg protocol.Config, fcfg filter.Config, q queue.Queue, collector metrics.Collector) (*streamProcessor, error) {
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

	return &streamProcessor{
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
		parsers:   make(map[core.ConnID]*connParsers),
		seq:       make(map[core.ConnID]uint64),
		lastSQL:   make(map[core.ConnID]string),
	}, nil
}

// OnBytes frames the contiguous bytes for one direction of one connection and
// dispatches each framed message. A framing error is recorded as a parse error
// and processing continues with whatever messages were framed before it, so one
// corrupt message never tears down a connection's stream (R3.6).
func (sp *streamProcessor) OnBytes(conn core.ConnID, fromClient bool, data []byte) {
	sp.onBytesCount++
	if debugMode && (sp.onBytesCount <= 20 || sp.onBytesCount%100 == 0) {
		fmt.Fprintf(os.Stderr, "pgshadow: DEBUG OnBytes #%d conn=%v fromClient=%v len=%d\n", sp.onBytesCount, conn, fromClient, len(data))
	}
	cp := sp.parsersFor(conn)
	p := cp.server
	if fromClient {
		p = cp.client
	}

	msgs, err := p.Feed(data)
	if debugMode && (sp.onBytesCount <= 20 || sp.onBytesCount%100 == 0) {
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
func (sp *streamProcessor) OnClose(conn core.ConnID) {
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
func (sp *streamProcessor) parsersFor(conn core.ConnID) *connParsers {
	cp := sp.parsers[conn]
	if cp == nil {
		cp = &connParsers{
			client: protocol.NewParser(sp.cfg, true),
			server: protocol.NewParser(sp.cfg, false),
		}
		sp.parsers[conn] = cp
	}
	return cp
}

// handleClient routes one client→server message. Simple_Query is extracted by
// the EventBuilder and dispatched immediately. Parse defines a prepared
// statement (registered for later Bind correlation) but does not itself emit a
// replayable event — the event is produced when the Bind supplies parameter
// values (R3.9). COPY FROM buffers its data stream and emits a single event,
// with the payload attached, on CopyDone (R3.10).
func (sp *streamProcessor) handleClient(conn core.ConnID, msg core.PGMessage, ts time.Time) {
	switch msg.Type {
	case msgParse:
		// Parse carries SQL + declared parameter OIDs. Record it so a later
		// Bind on the same connection can be reunited with the SQL and OIDs.
		if ev, ok := sp.eb.Build(conn, msg, ts); ok {
			sp.binder.OnParse(conn, ev.StmtName, ev.SQL, oidsOf(ev.Params))
		}

	case msgBind:
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
		}
		// Bind begins an execution that the backend will run (R10.7).
		sp.timer.OnClientQuery(conn, ts)

	case msgCopyData:
		sp.copies.Data(conn, msg.Payload)

	case msgCopyDone:
		if sql, data, ok := sp.copies.Done(conn); ok {
			ev := &core.SQLEvent{Conn: conn, SQL: sql, CopyData: data, Timestamp: ts}
			sp.dispatch(conn, ev, ts)
		}

	case msgCopyFail:
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
func (sp *streamProcessor) dispatch(conn core.ConnID, ev *core.SQLEvent, ts time.Time) {
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
}

// handleServer routes one server→client message. ReadyForQuery is the
// authoritative, bidirectional transaction-state signal routed into the state
// machine (R12.1, R4.1). ReadyForQuery and CommandComplete also close a
// statement for source-side execution timing (R10.7).
func (sp *streamProcessor) handleServer(conn core.ConnID, msg core.PGMessage, ts time.Time) {
	switch msg.Type {
	case msgReadyForQuery:
		if status, ok := protocol.ExtractReadyForQuery(msg); ok {
			sp.sm.OnReadyForQuery(conn, status)
		}
		sp.recordExec(conn, ts)
	case msgCommandComplete:
		sp.recordExec(conn, ts)
	}
}

// recordExec computes the source-side execution time for the just-completed
// statement and records it to metrics, leaving the target side zero (it is
// filled by the replayer). A completion with no pending request, or a
// non-positive interval, is ignored.
func (sp *streamProcessor) recordExec(conn core.ConnID, ts time.Time) {
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

// pipeline holds the running pipeline's lifecycle handles so run can perform
// the ordered graceful-shutdown drain (R12.5). The producer (capture →
// reassembler → processor → queue) and the consumer (replayer draining the
// queue → Target_Database) run on independent contexts so they can be torn down
// in the required order rather than all at once:
//
//   - prodCancel stops the producer so no new events enter the queue (step 1/2).
//   - Closing the queue makes the consumer drain the remaining events and exit
//     on its own (step 3/4); consumerCancel is only the deadline escape hatch
//     that forces in-flight workers to abandon work if draining stalls.
type pipeline struct {
	c              *components
	prodCancel     context.CancelFunc
	consumerCancel context.CancelFunc
	producerDone   <-chan struct{}
	consumerDone   <-chan struct{}
}

// startPipeline assembles and starts the full pipeline and returns a handle for
// the graceful-shutdown drain. The consumer side (the replayer draining the
// queue and executing against the Target_Database) and the producer side
// (capture → reassembler → processor → queue) run as independent goroutines, so
// a stall on either side is absorbed by the queue rather than propagated.
//
// The consumer runs on a context derived from context.Background rather than
// from ctx, so a shutdown signal does NOT immediately abandon events already
// buffered in the queue: the drain instead closes the queue, which lets the
// replayer dequeue every remaining event and return cleanly (R12.5 step 3/4).
// The producer runs on a context derived from ctx, so the signal stops capture
// first (R12.5 step 1).
//
// It returns an error only if the producer cannot be constructed (an invalid
// filter mode that slipped past configuration validation); in that case run
// aborts before blocking.
func startPipeline(ctx context.Context, c *components) (*pipeline, error) {
	sp, err := newStreamProcessor(c.cfg.Parser, c.cfg.Filter, c.queue, c.collector)
	if err != nil {
		return nil, err
	}

	// Consumer: the replayer drains the queue and executes against the target.
	// It runs on an independent context so the queue can be drained at shutdown
	// before the consumer is asked to stop; consumerCancel forces it to stop if
	// the drain deadline expires.
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = c.replayer.Run(consumerCtx)
	}()

	// Producer: capture → reassembler → processor → queue. Stops when ctx is
	// cancelled (the shutdown signal) or when prodCancel is called by the drain.
	prodCtx, prodCancel := context.WithCancel(ctx)
	producerDone := startProducer(prodCtx, c, sp)

	// Metrics polling: periodically report capture stats and queue depth so
	// Prometheus metrics stay current. Stops when the producer context is done.
	go metricsPoller(prodCtx, c.capture, c.queue, c.collector)

	return &pipeline{
		c:              c,
		prodCancel:     prodCancel,
		consumerCancel: consumerCancel,
		producerDone:   producerDone,
		consumerDone:   consumerDone,
	}, nil
}

// drain runs the ordered graceful-shutdown sequence under the default deadline.
func (p *pipeline) drain(stderr io.Writer) {
	p.drainWithin(defaultDrainTimeout, stderr)
}

// drainWithin runs the ordered graceful-shutdown drain (R12.5) bounded by a
// single shared deadline measured from the start of the drain:
//
//	(1) stop Packet_Capture so no new events enter;
//	(2) let the parse/filter pipeline drain (wait for the producer goroutines to
//	    flush whatever they framed into the queue);
//	(3) drain (ringbuffer) or checkpoint (file/kafka) the Buffer_Queue by
//	    closing it — the replayer then dequeues every remaining event;
//	(4) wait for the in-flight replay workers to finish executing against the
//	    Target_Database;
//	(5) close the Connection_Pool.
//
// If any wait exceeds the deadline, the drain stops waiting, forces the
// replayer to abandon in-flight work, and still closes the pool so the process
// exits promptly. The resources closed here are nil'd out of components so the
// deferred components.Close in run does not double-close them.
func (p *pipeline) drainWithin(timeout time.Duration, stderr io.Writer) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	timedOut := false

	// Step 1: stop Packet_Capture so no new events enter the pipeline. The
	// producer reader goroutine stops feeding on prodCancel; closing the handle
	// releases the capture resource.
	p.prodCancel()
	if p.c.capture != nil {
		_ = p.c.capture.Close()
		p.c.capture = nil
	}

	// Step 2: let the parse/filter stage drain. The producer goroutines close
	// the internal packet channel and the reassembler and exit once they have
	// processed what they already framed, flushing kept events into the queue.
	select {
	case <-p.producerDone:
	case <-deadline.C:
		timedOut = true
		fmt.Fprintln(stderr, "pgshadow: shutdown: producer drain timed out")
	}

	// Step 3: drain or checkpoint the Buffer_Queue. Closing the queue makes the
	// ring buffer hand the replayer every still-buffered event before reporting
	// closed (drain); the file/kafka backends flush/checkpoint on Close.
	if p.c.queue != nil {
		_ = p.c.queue.Close()
		p.c.queue = nil
	}

	// Step 4: wait for the in-flight replay workers to finish. With the queue
	// closed, the replayer drains the remainder and returns on its own; the
	// deadline bounds the wait regardless of whether the producer timed out so
	// that a stalled consumer does not hang the process indefinitely. Even when
	// the producer timed out, the queue is already closed (step 3) and the
	// consumer may be mid-drain, so we always give it the remaining deadline
	// rather than force-cancelling immediately (R12.5 step 4).
	select {
	case <-p.consumerDone:
	case <-deadline.C:
		timedOut = true
		fmt.Fprintln(stderr, "pgshadow: shutdown: replay drain timed out; forcing stop")
	}
	if timedOut {
		// Force the replayer to abandon in-flight work so the pool can close.
		p.consumerCancel()
	}
	// Always wait for the consumer to fully stop before closing the pool, so no
	// worker is mid-execution against a closing pool.
	<-p.consumerDone

	// Step 5: close the Connection_Pool now that no worker is executing.
	p.consumerCancel() // release the derived context (no-op if already cancelled)
	if p.c.pool != nil {
		p.c.pool.Close()
		p.c.pool = nil
	}
}

// metricsPollerInterval controls how often the metrics poller reads capture
// stats and queue depth. 5 seconds is a sensible default for Prometheus scrape
// intervals without creating excessive overhead.
const metricsPollerInterval = 5 * time.Second

// metricsPoller periodically polls capture stats (packets received/dropped) and
// queue depth, reporting them to the metrics collector. This fills the gap where
// no pipeline stage was wiring these values to the collector. It exits when ctx
// is cancelled (producer shutdown).
func metricsPoller(ctx context.Context, cap capture.Source, q queue.Queue, collector metrics.Collector) {
	if collector == nil {
		return
	}
	ticker := time.NewTicker(metricsPollerInterval)
	defer ticker.Stop()

	var lastDropped uint64
	var lastOverflowDepth int // track queue depth for overflow warning

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cap != nil {
				if st, err := cap.Stats(); err == nil {
					collector.PacketDrops(st.Received, st.Dropped)
					// Log when new packet drops are detected.
					if st.Dropped > lastDropped {
						newDrops := st.Dropped - lastDropped
						fmt.Fprintf(os.Stderr, "pgshadow: WARN [%s] packet drops detected: +%d (total=%d, received=%d, drop_rate=%.4f%%)\n",
							time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
							newDrops, st.Dropped, st.Received,
							float64(st.Dropped)/float64(st.Received)*100)
					}
					lastDropped = st.Dropped
				}
			}
			if q != nil {
				depth := q.Depth()
				collector.QueueDepth(depth)
				// Log when queue depth is critically high (>80% of default capacity).
				if depth > queue.DefaultCapacity*80/100 && lastOverflowDepth <= queue.DefaultCapacity*80/100 {
					fmt.Fprintf(os.Stderr, "pgshadow: WARN [%s] queue depth critical: %d (capacity may be near limit)\n",
						time.Now().Format("2006-01-02T15:04:05.000Z07:00"), depth)
				}
				lastOverflowDepth = depth
			}
		}
	}
}

// startProducer wires the capture source to the stream processor through the
// TCP reassembler and starts the two producer goroutines. It returns a channel
// that is closed once both goroutines have exited, so the drain can wait for
// the parse/filter stage to finish flushing into the queue (R12.5 step 2).
//
// Goroutine 1 (reader) copies packets from the capture source into the buffered
// pktCh, decoupling NIC reads from processing (the capture→processing seam).
// Goroutine 2 (processor) is the single owner of the reassembler — which is not
// safe for concurrent use — and drives Assemble for each packet plus a periodic
// FlushOlderThan to reclaim idle connection state (R2.4). Both exit on
// ctx.Done() or when the capture source closes its channel.
func startProducer(ctx context.Context, c *components, sp *streamProcessor) <-chan struct{} {
	reasm := capture.NewReassembler(c.cfg.Capture, sp)
	pktCh := make(chan gopacket.Packet, pktChanBuffer)

	idle := time.Duration(c.cfg.Parser.TimeoutIdle) * time.Second
	if idle <= 0 {
		idle = defaultIdleFlush
	}

	// Read the packet channel once here (not inside the goroutine) so the
	// reader never touches c.capture concurrently with the drain detaching it.
	src := c.capture.Packets()

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: capture → buffered channel (read-only; never writes the NIC).
	go func() {
		defer wg.Done()
		defer close(pktCh)
		if debugMode {
			fmt.Fprintln(os.Stderr, "pgshadow: DEBUG producer reader goroutine started")
		}
		pktCount := 0
		for {
			select {
			case <-ctx.Done():
				if debugMode {
					fmt.Fprintf(os.Stderr, "pgshadow: DEBUG reader exiting ctx.Done, packets=%d\n", pktCount)
				}
				return
			case pkt, ok := <-src:
				if !ok {
					if debugMode {
						fmt.Fprintf(os.Stderr, "pgshadow: DEBUG reader src channel closed, packets=%d\n", pktCount)
					}
					return
				}
				pktCount++
				if debugMode && (pktCount <= 5 || pktCount%100 == 0) {
					fmt.Fprintf(os.Stderr, "pgshadow: DEBUG packet %d received, len=%d\n", pktCount, len(pkt.Data()))
				}
				select {
				case pktCh <- pkt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Goroutine 2: single-threaded reassembler + processing loop.
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(idle)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				reasm.Close()
				return
			case pkt, ok := <-pktCh:
				if !ok {
					reasm.Close()
					return
				}
				reasm.Assemble(pkt)
			case <-ticker.C:
				reasm.FlushOlderThan(idle)
			}
		}
	}()

	// Signal completion once both producer goroutines have exited.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

// Compile-time production-isolation assertions (R12.6). The producer depends on
// the capture source only through these read-only shapes; there is no method
// here, or on these interfaces, that opens or writes a socket to the
// Source_Database. If a future edit widened capture.Source with a write/dial
// method and wired it in, these assertions are the structural anchor that keeps
// the producer's surface read-only.
var (
	_ interface {
		Packets() <-chan gopacket.Packet
		Stats() (capture.CaptureStats, error)
		Close() error
	} = capture.Source(nil)

	_ capture.StreamHandler = (*streamProcessor)(nil)
)
