package replayer

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// --- Test doubles ------------------------------------------------------------

// fakeQueue is a channel-backed queue.Queue for driving the replayer without a
// real backend. Once closed and drained, Dequeue reports ok=false so the
// dispatcher loop terminates (matching the real queue contract).
type fakeQueue struct {
	ch     chan *core.SQLEvent
	closed atomicBool
}

func newFakeQueue(capacity int) *fakeQueue {
	return &fakeQueue{ch: make(chan *core.SQLEvent, capacity)}
}

func (q *fakeQueue) Enqueue(ev *core.SQLEvent) (dropped bool) {
	q.ch <- ev
	return false
}

func (q *fakeQueue) Dequeue(ctx context.Context) (*core.SQLEvent, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case ev, ok := <-q.ch:
		return ev, ok
	}
}

func (q *fakeQueue) Depth() int { return len(q.ch) }

func (q *fakeQueue) Close() error {
	if q.closed.swapTrue() {
		return nil
	}
	close(q.ch)
	return nil
}

// atomicBool is a tiny helper to close the fake queue exactly once.
type atomicBool struct {
	v int32
}

func (b *atomicBool) swapTrue() (was bool) {
	return !atomic.CompareAndSwapInt32(&b.v, 0, 1)
}

// recordingExecutor records the per-ConnID order in which events execute as
// well as a flat global order, so tests can assert ordering guarantees.
type recordingExecutor struct {
	mu      sync.Mutex
	perConn map[string][]uint64 // ConnID.Key() -> executed Seq order
	global  []string            // flat "connKey/seq" order across all lanes
	hook    func()              // optional per-exec hook (e.g. block/barrier)
}

func newRecordingExecutor() *recordingExecutor {
	return &recordingExecutor{perConn: make(map[string][]uint64)}
}

func (e *recordingExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	if e.hook != nil {
		e.hook()
	}
	e.mu.Lock()
	e.perConn[conn.Key()] = append(e.perConn[conn.Key()], ev.Seq)
	e.global = append(e.global, conn.Key()+"/"+ev.SQL)
	e.mu.Unlock()
	return nil
}

func (e *recordingExecutor) order(conn core.ConnID) []uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]uint64, len(e.perConn[conn.Key()]))
	copy(out, e.perConn[conn.Key()])
	return out
}

func (e *recordingExecutor) total() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, s := range e.perConn {
		n += len(s)
	}
	return n
}

// ev builds a SQL_Event for the given ConnID/Seq.
func ev(conn core.ConnID, seq uint64, sql string) *core.SQLEvent {
	return &core.SQLEvent{Conn: conn, Seq: seq, SQL: sql, Timestamp: time.Now()}
}

// runToCompletion enqueues events, closes the queue, and runs the replayer
// until it drains (background context, so Run returns nil).
func runToCompletion(t *testing.T, r *replayer, q *fakeQueue, events []*core.SQLEvent) {
	t.Helper()
	for _, e := range events {
		q.Enqueue(e)
	}
	q.Close()
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

// --- Per-ConnID captured-order preservation (R7.13) across all modes ---------

func assertPerConnOrder(t *testing.T, exec *recordingExecutor, conns []core.ConnID, perConn int) {
	t.Helper()
	for _, c := range conns {
		got := exec.order(c)
		if len(got) != perConn {
			t.Fatalf("conn %s: expected %d events, got %d (%v)", c, perConn, len(got), got)
		}
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Fatalf("conn %s: out-of-order Seq at %d: %v", c, i, got)
			}
		}
	}
}

func TestPerConnIDOrder_AllModes(t *testing.T) {
	conns := []core.ConnID{connID(1001), connID(1002), connID(1003)}
	const perConn = 20

	// Build an interleaved event stream: round-robin across ConnIDs so the
	// dispatcher sees them mixed, but each ConnID's Seq is strictly increasing.
	var events []*core.SQLEvent
	for i := uint64(1); i <= perConn; i++ {
		for _, c := range conns {
			events = append(events, ev(c, i, "stmt"))
		}
	}

	for _, mode := range []ConcurrencyMode{Bounded, Faithful, Serial} {
		t.Run(mode.String(), func(t *testing.T) {
			exec := newRecordingExecutor()
			q := newFakeQueue(len(events))
			r := newReplayer(Config{Mode: mode, Workers: 8, MaxConcurrency: 8, PoolMaxConns: 8}, q, exec, nil)
			runToCompletion(t, r, q, events)

			if exec.total() != len(events) {
				t.Fatalf("expected %d executions, got %d", len(events), exec.total())
			}
			assertPerConnOrder(t, exec, conns, perConn)
		})
	}
}

// --- Serial mode: single global lane preserves captured order (R7.12) --------

func TestSerialMode_GlobalCapturedOrder(t *testing.T) {
	conns := []core.ConnID{connID(1), connID(2), connID(3)}
	var events []*core.SQLEvent
	var wantGlobal []string
	for i := uint64(1); i <= 10; i++ {
		for _, c := range conns {
			e := ev(c, i, sqlForSeq(c, i))
			events = append(events, e)
			wantGlobal = append(wantGlobal, c.Key()+"/"+e.SQL)
		}
	}

	exec := newRecordingExecutor()
	q := newFakeQueue(len(events))
	r := newReplayer(Config{Mode: Serial}, q, exec, nil)
	runToCompletion(t, r, q, events)

	exec.mu.Lock()
	gotGlobal := append([]string(nil), exec.global...)
	exec.mu.Unlock()

	if len(gotGlobal) != len(wantGlobal) {
		t.Fatalf("expected %d executions, got %d", len(wantGlobal), len(gotGlobal))
	}
	for i := range wantGlobal {
		if gotGlobal[i] != wantGlobal[i] {
			t.Fatalf("serial order mismatch at %d: want %q got %q", i, wantGlobal[i], gotGlobal[i])
		}
	}
}

func sqlForSeq(c core.ConnID, seq uint64) string {
	return c.Key() + "#" + string(rune('a'+seq))
}

// --- Bounded mode concurrency cap = min(Workers, PoolMaxConns) (R7.10) -------

func TestBoundedMode_ConcurrencyCap(t *testing.T) {
	// min(Workers=5, PoolMaxConns=3) == 3.
	const wantCap = 3
	assertConcurrencyCap(t, Config{Mode: Bounded, Workers: 5, PoolMaxConns: 3}, wantCap, 12)
}

// --- Faithful mode: one connection per ConnID up to MaxConcurrency (R7.11) ---

func TestFaithfulMode_ConcurrencyCap(t *testing.T) {
	const wantCap = 4
	releases := newReleaseRecorder()
	maxConns := assertConcurrencyCapWithRelease(t, Config{Mode: Faithful, MaxConcurrency: wantCap, Workers: 100, PoolMaxConns: 100}, wantCap, 16, releases)
	// Each concurrently active ConnID runs on its own lane/connection, so the
	// number of distinct ConnIDs in flight at the cap equals the cap.
	if maxConns != wantCap {
		t.Fatalf("faithful: expected %d distinct ConnIDs concurrently in flight, got %d", wantCap, maxConns)
	}
	// Every active ConnID should have had its connection released on retirement.
	if releases.count() != 16 {
		t.Fatalf("faithful: expected 16 connection releases, got %d", releases.count())
	}
}

// assertConcurrencyCap drives nLanes distinct ConnIDs (one event each) through
// a replayer whose executor blocks until released, and asserts that the number
// of simultaneously in-flight executions never exceeds wantCap and reaches it.
func assertConcurrencyCap(t *testing.T, cfg Config, wantCap, nLanes int) {
	t.Helper()
	assertConcurrencyCapWithRelease(t, cfg, wantCap, nLanes, nil)
}

func assertConcurrencyCapWithRelease(t *testing.T, cfg Config, wantCap, nLanes int, releases *releaseRecorder) int {
	t.Helper()

	var inFlight int32
	var maxInFlight int32
	gate := make(chan struct{})

	// Track the maximum number of distinct ConnIDs concurrently in flight.
	var connMu sync.Mutex
	active := make(map[string]struct{})
	maxDistinct := 0

	bex := &recordingExecutor{perConn: make(map[string][]uint64)}
	blocking := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		connMu.Lock()
		active[conn.Key()] = struct{}{}
		if len(active) > maxDistinct {
			maxDistinct = len(active)
		}
		connMu.Unlock()

		cur := atomic.AddInt32(&inFlight, 1)
		for {
			m := atomic.LoadInt32(&maxInFlight)
			if cur <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, cur) {
				break
			}
		}
		<-gate // block until the test releases (simulates work in progress)
		atomic.AddInt32(&inFlight, -1)

		connMu.Lock()
		delete(active, conn.Key())
		connMu.Unlock()
		return bex.Exec(ctx, conn, e)
	})

	var release func(core.ConnID)
	if releases != nil {
		release = releases.release
	}

	q := newFakeQueue(nLanes)
	r := newReplayer(cfg, q, blocking, release)

	var events []*core.SQLEvent
	for i := 0; i < nLanes; i++ {
		events = append(events, ev(connID(uint16(2000+i)), 1, "stmt"))
	}
	for _, e := range events {
		q.Enqueue(e)
	}
	q.Close()

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background()) }()

	// Wait until the cap is saturated (wantCap events blocked in flight).
	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt32(&inFlight) == int32(wantCap)
	}, "in-flight executions to reach the cap")

	// Give any over-scheduling a brief chance to manifest, then assert the cap
	// was never exceeded.
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt32(&maxInFlight); got > int32(wantCap) {
		t.Fatalf("concurrency exceeded cap: max in flight %d > cap %d", got, wantCap)
	}
	if got := atomic.LoadInt32(&inFlight); got != int32(wantCap) {
		t.Fatalf("expected exactly %d in flight at saturation, got %d", wantCap, got)
	}

	// Release all blocked workers; the remaining events should drain.
	close(gate)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not complete after releasing workers")
	}

	if int(atomic.LoadInt32(&maxInFlight)) != wantCap {
		t.Fatalf("expected max concurrency to reach %d, got %d", wantCap, maxInFlight)
	}
	if bex.total() != nLanes {
		t.Fatalf("expected %d total executions, got %d", nLanes, bex.total())
	}

	connMu.Lock()
	defer connMu.Unlock()
	return maxDistinct
}

// releaseRecorder counts connection releases on lane retirement.
type releaseRecorder struct {
	mu   sync.Mutex
	seen map[string]int
}

func newReleaseRecorder() *releaseRecorder { return &releaseRecorder{seen: make(map[string]int)} }

func (r *releaseRecorder) release(c core.ConnID) {
	r.mu.Lock()
	r.seen[c.Key()]++
	r.mu.Unlock()
}

func (r *releaseRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

// --- Target-down isolation + resume (R7.7, R7.8, R12.3) ----------------------

func TestTargetDown_BuffersThenResumes(t *testing.T) {
	conns := []core.ConnID{connID(1), connID(2), connID(3)}
	const perConn = 8

	var events []*core.SQLEvent
	for i := uint64(1); i <= perConn; i++ {
		for _, c := range conns {
			events = append(events, ev(c, i, "stmt"))
		}
	}

	// The executor blocks while the "target is down"; the gate is closed to
	// simulate recovery.
	gate := make(chan struct{})
	var executed int32
	exec := newRecordingExecutor()
	exec.hook = func() {
		<-gate
		atomic.AddInt32(&executed, 1)
	}

	// A small queue capacity proves the producer keeps making progress while
	// the target is down: enqueue must not block on the stalled replayer.
	q := newFakeQueue(len(events))
	r := newReplayer(Config{Mode: Bounded, Workers: 4, PoolMaxConns: 4}, q, exec, nil)

	// Producer: enqueue everything while the target is down. This models
	// production traffic accumulating in the Buffer_Queue (R7.7).
	enqDone := make(chan struct{})
	go func() {
		for _, e := range events {
			q.Enqueue(e)
		}
		close(enqDone)
	}()

	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(context.Background()) }()

	// The producer must finish enqueuing even though no event has executed:
	// the replayer never blocks the production/enqueue path (R7.7, R12.3).
	select {
	case <-enqDone:
	case <-time.After(time.Second):
		t.Fatal("producer blocked while target was down — production was not isolated")
	}

	// Nothing should have executed yet because the target is "down".
	if got := atomic.LoadInt32(&executed); got != 0 {
		t.Fatalf("expected 0 executions while target down, got %d", got)
	}

	// Target recovers: release workers and close the queue so Run drains.
	close(gate)
	q.Close()

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not resume/drain after target recovery")
	}

	if exec.total() != len(events) {
		t.Fatalf("expected all %d events replayed after recovery, got %d", len(events), exec.total())
	}
	assertPerConnOrder(t, exec, conns, perConn)
}

// --- Graceful stop on context cancel -----------------------------------------

func TestRun_ContextCancelStops(t *testing.T) {
	q := newFakeQueue(1)
	// Executor blocks forever unless ctx is cancelled, modeling a stuck target.
	exec := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		<-ctx.Done()
		return ctx.Err()
	})
	r := newReplayer(Config{Mode: Bounded, Workers: 2, PoolMaxConns: 2}, q, exec, nil)

	q.Enqueue(ev(connID(1), 1, "stmt"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error on cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on context cancel")
	}
}

// --- COPY FROM STDIN replay (R3.10) ------------------------------------------

// TestCopyDataEvent_PassedToExecutor verifies that a SQLEvent carrying CopyData
// is routed through the replayer and delivered intact to the executor, so the
// production poolExecutor can invoke pgconn.CopyFrom.
func TestCopyDataEvent_PassedToExecutor(t *testing.T) {
	var receivedCopyData [][]byte
	var receivedSQL string
	exec := ExecutorFunc(func(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
		receivedSQL = ev.SQL
		receivedCopyData = ev.CopyData
		return nil
	})

	q := newFakeQueue(1)
	r := newReplayer(Config{Mode: Bounded, Workers: 2, PoolMaxConns: 2}, q, exec, nil)

	copyEvent := &core.SQLEvent{
		Conn:     connID(1),
		Seq:      1,
		SQL:      "COPY t FROM STDIN",
		CopyData: [][]byte{[]byte("row1\n"), []byte("row2\n"), []byte("row3\n")},
	}
	q.Enqueue(copyEvent)
	q.Close()

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if receivedSQL != "COPY t FROM STDIN" {
		t.Fatalf("expected SQL %q, got %q", "COPY t FROM STDIN", receivedSQL)
	}
	if len(receivedCopyData) != 3 {
		t.Fatalf("expected 3 CopyData segments, got %d", len(receivedCopyData))
	}
	for i, want := range []string{"row1\n", "row2\n", "row3\n"} {
		if string(receivedCopyData[i]) != want {
			t.Fatalf("CopyData[%d] = %q, want %q", i, receivedCopyData[i], want)
		}
	}
}

// TestCopyDataReader verifies that copyDataReader concatenates segments into a
// single contiguous stream, suitable for pgconn.CopyFrom.
func TestCopyDataReader(t *testing.T) {
	data := [][]byte{
		[]byte("hello "),
		[]byte("world"),
		[]byte("!\n"),
	}
	r := copyDataReader(data)
	var buf [64]byte
	n, _ := io.ReadFull(r, buf[:])
	got := string(buf[:n])
	want := "hello world!\n"
	if got != want {
		t.Fatalf("copyDataReader produced %q, want %q", got, want)
	}
}

// --- Extended Query parameterized replay (R3.9) ------------------------------

// TestExtendedQueryEvent_ParamsPassedToExecutor verifies that a SQLEvent with
// Extended=true and Params is routed through the replayer and delivered intact
// to the executor, ensuring the replay side does not discard parameters.
func TestExtendedQueryEvent_ParamsPassedToExecutor(t *testing.T) {
	var receivedSQL string
	var receivedParams []core.ParamInfo
	var receivedExtended bool
	exec := ExecutorFunc(func(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
		receivedSQL = ev.SQL
		receivedParams = ev.Params
		receivedExtended = ev.Extended
		return nil
	})

	q := newFakeQueue(1)
	r := newReplayer(Config{Mode: Bounded, Workers: 2, PoolMaxConns: 2}, q, exec, nil)

	paramEvent := &core.SQLEvent{
		Conn:     connID(1),
		Seq:      1,
		SQL:      "SELECT * FROM users WHERE id = $1 AND name = $2",
		Extended: true,
		Params: []core.ParamInfo{
			{OID: 23, Value: []byte("42"), Format: 0},      // int4, text format
			{OID: 25, Value: []byte("alice"), Format: 0},   // text, text format
		},
	}
	q.Enqueue(paramEvent)
	q.Close()

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if receivedSQL != "SELECT * FROM users WHERE id = $1 AND name = $2" {
		t.Fatalf("expected SQL with placeholders, got %q", receivedSQL)
	}
	if !receivedExtended {
		t.Fatal("expected Extended=true on received event")
	}
	if len(receivedParams) != 2 {
		t.Fatalf("expected 2 params, got %d", len(receivedParams))
	}
	if receivedParams[0].OID != 23 || string(receivedParams[0].Value) != "42" || receivedParams[0].Format != 0 {
		t.Fatalf("param[0] mismatch: %+v", receivedParams[0])
	}
	if receivedParams[1].OID != 25 || string(receivedParams[1].Value) != "alice" || receivedParams[1].Format != 0 {
		t.Fatalf("param[1] mismatch: %+v", receivedParams[1])
	}
}

// TestExtendedQueryEvent_NullParam verifies that a NULL parameter (nil Value)
// is preserved through the replay pipeline.
func TestExtendedQueryEvent_NullParam(t *testing.T) {
	var receivedParams []core.ParamInfo
	exec := ExecutorFunc(func(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
		receivedParams = ev.Params
		return nil
	})

	q := newFakeQueue(1)
	r := newReplayer(Config{Mode: Bounded, Workers: 2, PoolMaxConns: 2}, q, exec, nil)

	paramEvent := &core.SQLEvent{
		Conn:     connID(1),
		Seq:      1,
		SQL:      "INSERT INTO t(a) VALUES ($1)",
		Extended: true,
		Params: []core.ParamInfo{
			{OID: 25, Value: nil, Format: 0}, // NULL param
		},
	}
	q.Enqueue(paramEvent)
	q.Close()

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(receivedParams) != 1 {
		t.Fatalf("expected 1 param, got %d", len(receivedParams))
	}
	if receivedParams[0].Value != nil {
		t.Fatalf("expected nil Value for NULL param, got %v", receivedParams[0].Value)
	}
}

// TestSplitParams verifies the helper that unpacks ParamInfo into the separate
// slices that pgconn.ExecParams expects.
func TestSplitParams(t *testing.T) {
	params := []core.ParamInfo{
		{OID: 23, Value: []byte("1"), Format: 0},
		{OID: 25, Value: []byte("hello"), Format: 0},
		{OID: 17, Value: []byte{0x01, 0x02}, Format: 1}, // bytea, binary
		{OID: 23, Value: nil, Format: 0},                 // NULL
	}
	oids, values, formats := splitParams(params)

	if len(oids) != 4 || len(values) != 4 || len(formats) != 4 {
		t.Fatalf("expected 4 elements each, got oids=%d values=%d formats=%d",
			len(oids), len(values), len(formats))
	}
	if oids[0] != 23 || oids[1] != 25 || oids[2] != 17 || oids[3] != 23 {
		t.Fatalf("OIDs mismatch: %v", oids)
	}
	if string(values[0]) != "1" || string(values[1]) != "hello" {
		t.Fatalf("values mismatch: %v", values)
	}
	if values[2][0] != 0x01 || values[2][1] != 0x02 {
		t.Fatalf("binary value mismatch: %v", values[2])
	}
	if values[3] != nil {
		t.Fatalf("expected nil for NULL param, got %v", values[3])
	}
	if formats[0] != 0 || formats[1] != 0 || formats[2] != 1 || formats[3] != 0 {
		t.Fatalf("formats mismatch: %v", formats)
	}
}

// --- helpers -----------------------------------------------------------------

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
