package replayer

// Complementary unit tests for task 10.7: pacing (R7.3, R7.4), error policy
// (R7.6), and the affinity pool's dead-connection / exhaustion behavior (R9.5,
// R9.6). These cover edge cases NOT already exercised by exec_decorators_test.go
// (task 10.3) and pool_test.go (task 10.1): they reuse the test doubles and
// helpers declared there (fakeSleeper, fakeSource/fakeConn, ExecutorFunc, ev,
// connID, noopExec, errTarget) without redeclaring them.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// --- Pacing edge cases (R7.4) ------------------------------------------------

// At SpeedFactor 1.0 the decorator reproduces the captured inter-arrival gaps
// exactly, statement by statement (the original timing is preserved).
func TestPacingExecutor_FactorOneReproducesGaps(t *testing.T) {
	fs := &fakeSleeper{}
	p := newPacingExecutor(ExecutorFunc(noopExec), 1.0, fs.sleep)

	c := connID(1)
	base := time.Unix(0, 0)
	offsets := []time.Duration{
		0,                      // first: no delay
		40 * time.Millisecond,  // gap 40ms
		40 * time.Millisecond,  // another gap 40ms
		300 * time.Millisecond, // gap 300ms
	}
	cum := time.Duration(0)
	for i, off := range offsets {
		cum += off
		e := ev(c, uint64(i+1), "stmt")
		e.Timestamp = base.Add(cum)
		if err := p.Exec(context.Background(), c, e); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	want := []time.Duration{40 * time.Millisecond, 40 * time.Millisecond, 300 * time.Millisecond}
	got := fs.recorded()
	if len(got) != len(want) {
		t.Fatalf("expected %d sleeps, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sleep[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// Pacing is global across lanes: when statements from different ConnIDs
// interleave so the next capture timestamp PRECEDES the previous one, the
// negative gap must not produce a (negative) sleep. Only a forward gap delays.
func TestPacingExecutor_InterleavedLanesNoNegativeSleep(t *testing.T) {
	fs := &fakeSleeper{}
	p := newPacingExecutor(ExecutorFunc(noopExec), 1.0, fs.sleep)

	base := time.Unix(0, 0)
	steps := []struct {
		conn core.ConnID
		at   time.Duration
	}{
		{connID(1), 100 * time.Millisecond}, // first: no delay
		{connID(2), 50 * time.Millisecond},  // earlier than prev -> negative gap -> no sleep
		{connID(1), 200 * time.Millisecond}, // forward gap 150ms from prev (50ms) -> sleep 150ms
	}
	for i, s := range steps {
		e := ev(s.conn, uint64(i+1), "stmt")
		e.Timestamp = base.Add(s.at)
		if err := p.Exec(context.Background(), s.conn, e); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	got := fs.recorded()
	want := []time.Duration{150 * time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("expected exactly %d recorded sleep, got %v", len(want), got)
	}
	if got[0] != want[0] {
		t.Fatalf("sleep = %v, want %v", got[0], want[0])
	}
}

// A very large SpeedFactor shrinks the scaled gap below a nanosecond, which
// truncates to a zero delay (effectively ASAP) rather than a negative or
// fractional duration.
func TestPacingDelay_FractionalRoundsTowardZero(t *testing.T) {
	// 100ns / 1000 = 0.1ns -> truncates to 0.
	if d := pacingDelay(100*time.Nanosecond, 1000); d != 0 {
		t.Fatalf("expected sub-nanosecond scaled gap to truncate to 0, got %v", d)
	}
	// A 1s gap at factor 1e9 is 1ns: still strictly positive.
	if d := pacingDelay(time.Second, 1e9); d != time.Nanosecond {
		t.Fatalf("expected 1ns delay, got %v", d)
	}
}

// realSleep returns immediately for a non-positive duration without arming a
// timer (the ASAP / no-gap path).
func TestRealSleep_NonPositiveReturnsImmediately(t *testing.T) {
	start := time.Now()
	realSleep(context.Background(), 0)
	realSleep(context.Background(), -5*time.Second)
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("non-positive realSleep should return immediately, took %v", elapsed)
	}
}

// realSleep abandons an outstanding delay promptly when the context is
// cancelled, so a graceful stop is not held up by a long pacing delay.
func TestRealSleep_CancelReturnsEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	realSleep(ctx, time.Hour) // would block ~forever without cancellation
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("realSleep should return shortly after cancel, took %v", elapsed)
	}
}

// --- Rate limiting edge case (R7.3) ------------------------------------------

// When the context is already cancelled, the limiter wait fails up front and
// the wrapped executor is never invoked (no statement runs against the target).
func TestRateLimitExecutor_AlreadyCancelledContextSkipsExec(t *testing.T) {
	var calls int32
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	rl := &rateLimitExecutor{next: next, limiter: newRateLimiter(1000)}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := connID(1)
	if err := rl.Exec(ctx, c, ev(c, 1, "stmt")); err == nil {
		t.Fatal("expected the limiter wait to fail under a cancelled context")
	}
	if atomic.LoadInt32(&calls) != 0 {
		t.Fatalf("wrapped executor must not run when the wait fails, got %d calls", calls)
	}
}

// --- Error policy edge cases (R7.6) ------------------------------------------

// Abort with a nil hook still propagates the failure and does not panic (the
// nil hook is treated as a no-op).
func TestErrorPolicy_AbortNilHookNoPanic(t *testing.T) {
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		return errTarget
	})
	ep := newErrorPolicyExecutor(next, Abort, nil)
	c := connID(1)
	if err := ep.Exec(context.Background(), c, ev(c, 1, "stmt")); err == nil {
		t.Fatal("abort must propagate the failure even with a nil hook")
	}
}

// Retry stops re-attempting as soon as the context is cancelled mid-loop,
// surfacing the error instead of exhausting the full attempt budget.
func TestErrorPolicy_RetryStopsOnMidLoopCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	next := ExecutorFunc(func(c context.Context, conn core.ConnID, e *core.SQLEvent) error {
		// Cancel the run on the second invocation (the first retry attempt) so
		// the loop's next iteration short-circuits.
		if atomic.AddInt32(&calls, 1) == 2 {
			cancel()
		}
		return errTarget
	})
	ep := newErrorPolicyExecutor(next, Retry, nil)

	c := connID(1)
	err := ep.Exec(ctx, c, ev(c, 1, "stmt"))
	if err == nil {
		t.Fatal("a cancelled retry should surface the failure")
	}
	// initial attempt (1) + one retry that cancels (2); the loop then aborts
	// before consuming the full defaultRetryAttempts budget.
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Fatalf("expected retry to stop at 2 attempts on cancel, got %d", got)
	}
	if defaultRetryAttempts <= 2 {
		t.Skip("retry budget too small to demonstrate early stop")
	}
}

// --- Pool: dead-connection replacement (R9.5) --------------------------------

// A connection that dies, is replaced, and then dies again is replaced a second
// time: each death discards the dead handle and leases a fresh one for the same
// ConnID.
func TestAffinity_DeadConnectionReplacedTwice(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, true)
	ctx := context.Background()
	id := connID(42)

	h1, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	c1 := h1.(*fakeConn)
	c1.kill()

	h2, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	c2 := h2.(*fakeConn)
	if h2 == h1 {
		t.Fatal("first death should yield a replacement")
	}
	c2.kill()

	h3, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire 3: %v", err)
	}
	if h3 == h1 || h3 == h2 {
		t.Fatal("second death should yield a distinct replacement")
	}
	if c1.released != 1 || c2.released != 1 {
		t.Fatalf("both dead connections must be released exactly once, got c1=%d c2=%d", c1.released, c2.released)
	}
	if err := h3.Ping(ctx); err != nil {
		t.Fatalf("final replacement should be alive: %v", err)
	}
	if src.acquired != 3 {
		t.Fatalf("expected 3 underlying acquires, got %d", src.acquired)
	}
}

// --- Pool: exhaustion behavior (R9.6) ----------------------------------------

// Under session affinity, reusing the same ConnID does not consume an
// additional connection slot, so a single-slot pool never exhausts for repeated
// same-ConnID acquires.
func TestAffinity_ReuseAvoidsExhaustion(t *testing.T) {
	src := newFakeSource(1) // only one slot
	p := newAffinityPool(src, true)
	id := connID(5)

	// A deadline ensures the second acquire would error (not hang) if it tried
	// to consume a second slot.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	h1, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	h2, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("reuse acquire should not block on a full pool: %v", err)
	}
	if h1 != h2 {
		t.Fatal("affinity reuse must return the same connection")
	}
	if src.acquired != 1 {
		t.Fatalf("reuse must not consume a second slot, got %d acquires", src.acquired)
	}
}

// When several distinct ConnIDs wait on an exhausted single-slot pool, each
// release frees exactly one waiter, so all waiters eventually acquire as slots
// become available one at a time (R9.6).
func TestExhaustion_MultipleWaitersUnblockSequentially(t *testing.T) {
	src := newFakeSource(1)
	p := newAffinityPool(src, true)

	if _, err := p.acquireFor(context.Background(), connID(1)); err != nil {
		t.Fatalf("seed acquire: %v", err)
	}

	type result struct {
		id  core.ConnID
		h   connHandle
		err error
	}
	done := make(chan result, 2)
	for _, port := range []uint16{2, 3} {
		id := connID(port)
		go func(id core.ConnID) {
			h, err := p.acquireFor(context.Background(), id)
			done <- result{id: id, h: h, err: err}
		}(id)
	}

	// Both goroutines should be blocked on the single in-use slot. Release the
	// seed connection: exactly one waiter should unblock.
	time.Sleep(30 * time.Millisecond)
	p.release(connID(1))

	var first result
	select {
	case first = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first blocked acquire to unblock")
	}
	if first.err != nil || first.h == nil {
		t.Fatalf("first waiter should acquire after release, got h=%v err=%v", first.h, first.err)
	}

	// Releasing the just-acquired connection frees the slot for the last waiter.
	p.release(first.id)
	var second result
	select {
	case second = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second blocked acquire to unblock")
	}
	if second.err != nil || second.h == nil {
		t.Fatalf("second waiter should acquire after release, got h=%v err=%v", second.h, second.err)
	}

	if second.id == first.id {
		t.Fatal("expected two distinct waiters to be served")
	}
}
