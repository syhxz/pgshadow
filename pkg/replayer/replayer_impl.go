// Package replayer — worker/lane routing and concurrency modes (task 10.2).
//
// This file implements the Replayer interface declared in replayer.go. A
// dispatcher reads SQL_Events from the Buffer_Queue and routes them to
// per-ConnID ordered lanes; lane goroutines drain their lane in captured order
// and execute events through an injectable execution seam. The three
// concurrency modes (R7.9) differ only in how lanes are scheduled and how many
// events may execute concurrently:
//
//   - bounded  (R7.10): per-ConnID lanes; concurrency capped at
//     min(Workers, PoolMaxConns); same ConnID serialized via session affinity.
//   - faithful (R7.11): per-ConnID lanes; one dedicated connection per active
//     ConnID; concurrency capped at MaxConcurrency.
//   - serial   (R7.12): a single global lane drained by a single worker in
//     captured order.
//
// Regardless of mode, events that share a source ConnID always replay serially
// in captured Seq order (R7.13), because the dispatcher dequeues in FIFO order
// (the queue preserves per-ConnID order, R6.6) and a lane is drained by exactly
// one goroutine.
//
// Target-failure isolation (R7.7, R7.8, R12.3): lane workers only ever block on
// the Target_Database (inside the execution seam). The dispatcher keeps
// draining the queue into lanes; when the target is slow/down events accumulate
// upstream in the bounded Buffer_Queue (overflow applied there) so production
// traffic is never delayed or blocked. When the target recovers, the blocked
// worker returns and buffered events replay in per-ConnID order.
//
// To keep routing/concurrency unit-testable WITHOUT a live database, execution
// goes through the Executor seam rather than touching pgx directly. The
// production wiring (NewReplayer) supplies a poolExecutor that leases the
// ConnID's connection from the Pool (task 10.1) and runs ev.SQL via pgx; tasks
// 10.3 (pacing/rate-limit/error-policy) and 10.4 (Greenplum dialect) decorate
// this same Executor seam. Unit tests inject a fake Executor that records
// per-ConnID execution order.
package replayer

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"pgshadow/pkg/core"
	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayguard"
)

// defaultWorkers is the worker count applied when none is configured (R7.2).
const defaultWorkers = 32

// defaultPoolMaxConns mirrors the pool default (R9.2) and bounds bounded-mode
// concurrency so distinct ConnIDs never run more concurrently than the pool can
// serve (R7.10).
const defaultPoolMaxConns = 100

// laneBufferSize is the small in-order buffer of each per-ConnID lane. It lets
// the dispatcher stay a few events ahead of a busy worker without unbounded
// memory; the authoritative backpressure/accumulation buffer is the upstream
// Buffer_Queue (R7.7).
const laneBufferSize = 64

// serialLaneKey is the routing key used for every event in serial mode so that
// all ConnIDs share one global lane and replay in captured order (R7.12).
const serialLaneKey = "__serial__"

// Executor is the injectable execution seam for the replayer. It executes a
// single SQL_Event for the given source ConnID against the Target_Database.
//
// This is the decoration point for later tasks: task 10.3 wraps an Executor to
// add pacing, rate limiting, and error policy; task 10.4 wraps it to add
// Greenplum-specific (autocommit-per-statement) behavior. The production
// Executor (poolExecutor) leases the ConnID's connection from the Pool under
// session affinity (R7.5) and runs ev.SQL via pgx. Unit tests inject a fake.
type Executor interface {
	Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error
}

// ExecutorFunc adapts a plain function to the Executor interface. It is handy
// both for tests and for the decorators added by tasks 10.3/10.4.
type ExecutorFunc func(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error

// Exec implements Executor.
func (f ExecutorFunc) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	return f(ctx, conn, ev)
}

// replayer is the concrete Replayer. It is database-independent: all execution
// flows through exec, and connection lifecycle through releaseConn, so the
// routing/concurrency logic is exercised in tests without a live database.
type replayer struct {
	q           queue.Queue
	exec        Executor
	releaseConn func(conn core.ConnID) // retires a ConnID's leased connection
	execHook    func(err error)        // optional post-execution hook for metrics
	guard       *replayguard.Guard     // optional safeguard for backpressure checks (#10)

	serial      bool // single global lane (R7.12)
	concurrency int  // max events executing concurrently across lanes

	// cancel stops the replay when the error policy is Abort (R7.6). It is set
	// for the duration of Run and invoked by the error-policy decorator via
	// requestAbort; production is unaffected because only the replay context is
	// cancelled (R12.4).
	cancelMu sync.Mutex
	cancel   context.CancelFunc
}

// compile-time assertion that *replayer satisfies the Replayer interface.
var _ Replayer = (*replayer)(nil)

// NewReplayer builds the production replayer: events are consumed from q and
// executed against the Target_Database via pool. Connections are leased per
// ConnID under session affinity (R7.5, R7.10) and released when a lane retires.
// The execution seam is decorated (task 10.3) so each statement is paced by
// SpeedFactor (R7.4), gated by the RateLimitQPS token bucket (R7.3), and
// subject to the configured skip/retry/abort error policy (R7.6). The abort
// policy stops replay by cancelling the run context (R12.4).
// The optional safeguard filters/rewrites events before execution.
func NewReplayer(cfg Config, q queue.Queue, pool Pool) Replayer {
	return NewReplayerWithHook(cfg, q, pool, nil, nil)
}

// NewReplayerWithHook is like NewReplayer but accepts an optional post-execution
// hook that is called after every replay attempt with the execution error (nil
// on success). This is used to wire metrics.Collector.ReplayResult without
// creating an import cycle between the replayer and metrics packages.
// The sg parameter is the optional safeguard; pass nil to disable.
func NewReplayerWithHook(cfg Config, q queue.Queue, pool Pool, execHook func(error), sg *replayguard.Guard) Replayer {
	r := &replayer{
		q:           q,
		releaseConn: pool.Release,
		execHook:    execHook,
		guard:       sg,
	}
	r.configureMode(cfg)
	r.exec = buildExecutor(cfg, poolExecutor{pool: pool, sessionAffinity: cfg.SessionAffinity}, r.requestAbort, realSleep, sg)
	return r
}

// requestAbort stops an in-flight replay by cancelling the run context. It is
// the abort hook handed to the error-policy decorator (R7.6); when no replay is
// running it is a no-op. Only the replay context is cancelled, so production
// traffic is unaffected (R12.4).
func (r *replayer) requestAbort() {
	r.cancelMu.Lock()
	cancel := r.cancel
	r.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// newReplayer is the internal constructor shared by production and tests. A nil
// release hook is treated as a no-op so tests need not supply a Pool.
func newReplayer(cfg Config, q queue.Queue, exec Executor, release func(conn core.ConnID)) *replayer {
	if release == nil {
		release = func(core.ConnID) {}
	}
	r := &replayer{
		q:           q,
		exec:        exec,
		releaseConn: release,
	}
	r.configureMode(cfg)
	return r
}

// configureMode derives the lane topology and concurrency cap from cfg (R7.9).
func (r *replayer) configureMode(cfg Config) {
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers // R7.2 default
	}
	poolMax := cfg.PoolMaxConns
	if poolMax <= 0 {
		poolMax = defaultPoolMaxConns // R9.2 default
	}

	switch cfg.Mode {
	case Serial:
		// Single global lane, single worker, captured order (R7.12).
		r.serial = true
		r.concurrency = 1
	case Faithful:
		// One dedicated connection per active ConnID, capped at MaxConcurrency
		// (R7.11). Fall back to the worker count when no cap is configured.
		r.serial = false
		cap := cfg.MaxConcurrency
		if cap <= 0 {
			cap = workers
		}
		r.concurrency = cap
	default: // Bounded (R7.10) is the default mode (R7.9).
		// Distinct ConnIDs run concurrently up to min(Workers, PoolMaxConns).
		r.serial = false
		c := workers
		if poolMax < c {
			c = poolMax
		}
		r.concurrency = c
	}
	if r.concurrency < 1 {
		r.concurrency = 1
	}
}

// Run consumes SQL_Events from the queue and replays them, returning when the
// queue is closed and drained or when ctx is cancelled (graceful stop). The
// calling goroutine acts as the dispatcher; lane goroutines do the executing so
// that a worker blocked on the Target_Database never blocks the dispatcher from
// routing further events (R7.7).
//
// Lanes use an unbounded buffer so that the dispatcher is never blocked by a
// slow lane worker. This prevents priority-inversion deadlocks when multiple
// lanes hold cross-dependent row locks: a lane waiting on a lock can no longer
// prevent the dispatcher from delivering the COMMIT that releases that lock on
// another lane. Memory growth is bounded upstream by backpressure (pausing
// Kafka consumption when total queue depth exceeds thresholds).
func (r *replayer) Run(ctx context.Context) error {
	// Derive a cancellable context so the Abort error policy can stop replay
	// from inside the execution seam (R7.6) without affecting production
	// (R12.4). The parent ctx still drives graceful shutdown.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.cancelMu.Lock()
	r.cancel = cancel
	r.cancelMu.Unlock()

	// sem caps how many events execute concurrently across all lanes. For
	// bounded this is min(Workers, PoolMaxConns) (R7.10); for faithful it is
	// MaxConcurrency (R7.11); for serial it is 1 (R7.12).
	sem := make(chan struct{}, r.concurrency)

	var laneWG sync.WaitGroup
	lanes := make(map[string]*unboundedLane)

dispatch:
	for {
		// Backpressure check (#10): pause dequeue when queue depth or replay lag
		// exceeds configured thresholds. This prevents the replayer from falling
		// further behind by slowing consumption, giving the target DB time to
		// recover. The upstream Buffer_Queue continues to accept production
		// events (with overflow policy) so production is never blocked (R7.7).
		// The lane depth is included so that unbounded lane buffers cannot grow
		// without limit when the target is slow or locks cause stalls.
		if r.guard != nil {
			laneDepth := 0
			for _, l := range lanes {
				laneDepth += l.Len()
			}
			for r.guard.CheckBackpressure(r.q.Depth()+laneDepth, 0) {
				select {
				case <-ctx.Done():
					break dispatch
				case <-time.After(100 * time.Millisecond):
					// Re-check after a brief pause; recompute lane depth.
					laneDepth = 0
					for _, l := range lanes {
						laneDepth += l.Len()
					}
				}
			}
		}

		ev, ok := r.q.Dequeue(ctx)
		if !ok {
			// Queue closed and drained, or ctx done.
			break
		}

		key := r.laneKey(ev)
		lane, exists := lanes[key]
		if !exists {
			lane = newUnboundedLane(laneBufferSize)
			lanes[key] = lane
			laneWG.Add(1)
			go r.runLaneUnbounded(ctx, lane, sem, &laneWG)
		}

		// Route the event onto its unbounded lane. This never blocks,
		// eliminating the priority-inversion deadlock where a full lane
		// channel prevented the dispatcher from delivering COMMIT events
		// to other lanes that hold cross-dependent locks.
		lane.Enqueue(ev)
	}

	// No more events will be routed; close lanes so workers drain and exit.
	for _, lane := range lanes {
		lane.Close()
	}
	laneWG.Wait()
	return ctx.Err()
}

// laneKey selects the routing key for ev: a single global key in serial mode
// (R7.12) or the source ConnID otherwise so each ConnID gets its own ordered
// lane (R7.13).
func (r *replayer) laneKey(ev *core.SQLEvent) string {
	if r.serial {
		return serialLaneKey
	}
	return ev.Conn.Key()
}

// runLaneUnbounded drains one unbounded lane in captured order and executes each
// event through the seam. It is the unbounded-lane counterpart of runLane.
// Because exactly one goroutine drains a lane and executes sequentially, events
// sharing a ConnID always replay serially in captured Seq order (R7.13). The
// semaphore bounds cross-lane concurrency to the mode's cap. ConnIDs the lane
// touched are released when it retires, freeing their pooled connections.
func (r *replayer) runLaneUnbounded(ctx context.Context, lane *unboundedLane, sem chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	seen := make(map[string]core.ConnID)
	defer func() {
		for _, c := range seen {
			r.releaseConn(c)
		}
	}()

	for {
		ev, ok := lane.Dequeue()
		if !ok {
			return
		}

		seen[ev.Conn.Key()] = ev.Conn

		// Acquire a concurrency slot before touching the target.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// Execute against the target. The worker blocks here (and only here)
		// when the target is slow/down; on recovery it returns and continues
		// (R7.8). Error policy/retry is layered on by task 10.3 via the seam.
		r.execAndRelease(ctx, ev, sem)
	}
}

// runLane drains one lane in captured order and executes each event through the
// seam. Because exactly one goroutine drains a lane and executes sequentially,
// events sharing a ConnID always replay serially in captured Seq order (R7.13).
// The semaphore bounds cross-lane concurrency to the mode's cap. ConnIDs the
// lane touched are released when it retires, freeing their pooled connections.
func (r *replayer) runLane(ctx context.Context, lane <-chan *core.SQLEvent, sem chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	seen := make(map[string]core.ConnID)
	defer func() {
		for _, c := range seen {
			r.releaseConn(c)
		}
	}()

	for ev := range lane {
		seen[ev.Conn.Key()] = ev.Conn

		// Acquire a concurrency slot before touching the target.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}

		// Execute against the target. The worker blocks here (and only here)
		// when the target is slow/down; on recovery it returns and continues
		// (R7.8). Error policy/retry is layered on by task 10.3 via the seam.
		r.execAndRelease(ctx, ev, sem)
	}
}

// execAndRelease runs a single event through the executor and releases the
// semaphore slot afterward. The deferred release ensures the slot is freed even
// if the executor panics, preventing a permanent deadlock of other lanes. This
// is a named method rather than an inline closure to avoid a heap allocation on
// every event in the hot path.
func (r *replayer) execAndRelease(ctx context.Context, ev *core.SQLEvent, sem chan struct{}) {
	defer func() { <-sem }()
	err := r.exec.Exec(ctx, ev.Conn, ev)
	if r.execHook != nil {
		r.execHook(err)
	}
}

// poolExecutor is the production Executor. It leases the connection bound to
// the event's ConnID (same ConnID → same connection under session affinity,
// R7.5/R7.10) and runs ev.SQL. This is intentionally minimal: pacing, rate
// limiting, and error policy (task 10.3) and Greenplum dialect handling (task
// 10.4) decorate this seam rather than living here.
type poolExecutor struct {
	pool            Pool
	sessionAffinity bool
}

// Exec implements Executor using the affinity-aware pool. Under session
// affinity the lane retirement handles connection release; without session
// affinity each Exec acquires and releases its own connection to prevent leaks.
//
// When the event carries CopyData (COPY FROM STDIN, R3.10), the low-level
// pgconn.CopyFrom is used to stream the captured CopyData segments to the
// target in a single COPY operation, faithfully replaying bulk-import traffic.
//
// When the event carries parameters from Extended_Query (R3.9), the low-level
// pgconn.ExecParams is used to replay the statement with its original parameter
// values, OIDs, and format codes — faithfully reproducing the parameterized
// query as captured from the source wire traffic.
func (e poolExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	c, err := e.pool.AcquireFor(ctx, conn)
	if err != nil {
		return err
	}
	if !e.sessionAffinity {
		defer c.Release()
	}
	if len(ev.CopyData) > 0 {
		// COPY FROM STDIN: stream buffered CopyData segments to the target via
		// the low-level pgconn COPY protocol. Each segment is one CopyData ('d')
		// message payload captured from the source wire traffic.
		_, err = c.Conn().PgConn().CopyFrom(ctx, copyDataReader(ev.CopyData), ev.SQL)
		return err
	}
	if ev.Extended && len(ev.Params) > 0 {
		// Extended_Query with parameters: use the low-level pgconn.ExecParams to
		// replay with the original parameter OIDs, values, and format codes so
		// that the target database receives an identical parameterized query.
		paramOIDs, paramValues, paramFormats := splitParams(ev.Params)
		_, err = c.Conn().PgConn().ExecParams(ctx, ev.SQL, paramValues, paramOIDs, paramFormats, nil).Close()
		return err
	}
	_, err = c.Exec(ctx, ev.SQL)
	return err
}

// splitParams unpacks []core.ParamInfo into the separate slices that
// pgconn.ExecParams expects: OIDs, raw values, and format codes.
func splitParams(params []core.ParamInfo) (oids []uint32, values [][]byte, formats []int16) {
	n := len(params)
	oids = make([]uint32, n)
	values = make([][]byte, n)
	formats = make([]int16, n)
	for i, p := range params {
		oids[i] = p.OID
		values[i] = p.Value
		formats[i] = p.Format
	}
	return
}

// copyDataReader concatenates the captured CopyData segments into a single
// io.Reader suitable for pgconn.CopyFrom. Using io.MultiReader avoids copying
// all segments into a contiguous buffer, keeping memory proportional to the
// largest individual segment rather than the total COPY payload.
func copyDataReader(data [][]byte) io.Reader {
	readers := make([]io.Reader, len(data))
	for i, chunk := range data {
		readers[i] = bytes.NewReader(chunk)
	}
	return io.MultiReader(readers...)
}
