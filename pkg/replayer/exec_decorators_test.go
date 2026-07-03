package replayer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// --- Pacing math (R7.4, R7.14) -----------------------------------------------

// pacingDelay scales the captured inter-arrival gap by SpeedFactor.
func TestPacingDelay_Math(t *testing.T) {
	const gap = 100 * time.Millisecond
	cases := []struct {
		name   string
		gap    time.Duration
		factor float64
		want   time.Duration
	}{
		{"factor 1.0 reproduces original gap", gap, 1.0, 100 * time.Millisecond},
		{"factor 2.0 halves the gap", gap, 2.0, 50 * time.Millisecond},
		{"factor 0.5 doubles the gap", gap, 0.5, 200 * time.Millisecond},
		{"factor 0 is ASAP (no delay)", gap, 0, 0},
		{"negative factor is ASAP (no delay)", gap, -3, 0},
		{"non-positive gap yields no delay", 0, 1.0, 0},
		{"negative gap (out-of-order lanes) yields no delay", -gap, 1.0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pacingDelay(tc.gap, tc.factor); got != tc.want {
				t.Fatalf("pacingDelay(%v, %v) = %v, want %v", tc.gap, tc.factor, got, tc.want)
			}
		})
	}
}

// fakeSleeper records the delays requested instead of actually sleeping, so the
// pacing decorator's timing decisions are exercised deterministically.
type fakeSleeper struct {
	mu     sync.Mutex
	delays []time.Duration
}

func (f *fakeSleeper) sleep(ctx context.Context, d time.Duration) {
	f.mu.Lock()
	f.delays = append(f.delays, d)
	f.mu.Unlock()
}

func (f *fakeSleeper) recorded() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]time.Duration, len(f.delays))
	copy(out, f.delays)
	return out
}

// pacingExecutor sleeps the scaled inter-arrival gap between consecutive
// statements and never delays before the first one.
func TestPacingExecutor_SleepsScaledGaps(t *testing.T) {
	base := time.Unix(0, 0)
	var executed int32
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		atomic.AddInt32(&executed, 1)
		return nil
	})
	fs := &fakeSleeper{}
	p := newPacingExecutor(next, 2.0, fs.sleep) // 2x: gaps are halved

	c := connID(1)
	ts := []time.Time{
		base,                             // first: no delay
		base.Add(100 * time.Millisecond), // gap 100ms -> sleep 50ms
		base.Add(300 * time.Millisecond), // gap 200ms -> sleep 100ms
	}
	for i, tm := range ts {
		e := ev(c, uint64(i+1), "stmt")
		e.Timestamp = tm
		if err := p.Exec(context.Background(), c, e); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}

	if executed != 3 {
		t.Fatalf("expected 3 executions, got %d", executed)
	}
	got := fs.recorded()
	// The first statement records no positive delay; only the two subsequent
	// gaps produce sleeps.
	want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}
	if len(got) != len(want) {
		t.Fatalf("expected %d sleeps, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sleep[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}

// With SpeedFactor 0 the decorator is not even constructed by buildExecutor;
// constructed directly with factor 0 it must never delay (ASAP).
func TestPacingExecutor_ASAPNeverSleeps(t *testing.T) {
	fs := &fakeSleeper{}
	p := newPacingExecutor(ExecutorFunc(noopExec), 0, fs.sleep)
	c := connID(1)
	for i := 0; i < 5; i++ {
		e := ev(c, uint64(i+1), "stmt")
		e.Timestamp = time.Unix(0, 0).Add(time.Duration(i) * time.Second)
		if err := p.Exec(context.Background(), c, e); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}
	for _, d := range fs.recorded() {
		if d > 0 {
			t.Fatalf("ASAP pacing must not sleep, got delay %v", d)
		}
	}
}

// --- Rate limiting (R7.3) ----------------------------------------------------

// A positive QPS builds a limiter; a non-positive QPS means unlimited (nil).
func TestNewRateLimiter_Construction(t *testing.T) {
	if l := newRateLimiter(0); l != nil {
		t.Fatal("qps=0 must yield no limiter (unlimited)")
	}
	if l := newRateLimiter(-5); l != nil {
		t.Fatal("negative qps must yield no limiter (unlimited)")
	}
	l := newRateLimiter(100)
	if l == nil {
		t.Fatal("positive qps must yield a limiter")
	}
	if l.Limit() != 100 {
		t.Fatalf("expected limit 100, got %v", l.Limit())
	}
	if l.Burst() != 1 {
		t.Fatalf("expected burst 1, got %d", l.Burst())
	}
}

// The token bucket spaces executions so the observed rate does not exceed the
// configured QPS: N events at Q qps take at least (N-1)/Q seconds.
func TestRateLimitExecutor_EnforcesQPS(t *testing.T) {
	const qps = 50.0 // 20ms spacing, burst 1
	const n = 4      // first immediate + 3 spaced gaps => >= 60ms
	rl := &rateLimitExecutor{next: ExecutorFunc(noopExec), limiter: newRateLimiter(qps)}

	c := connID(1)
	start := time.Now()
	for i := 0; i < n; i++ {
		if err := rl.Exec(context.Background(), c, ev(c, uint64(i+1), "stmt")); err != nil {
			t.Fatalf("Exec: %v", err)
		}
	}
	elapsed := time.Since(start)
	min := time.Duration(float64(n-1)/qps*float64(time.Second)) - 5*time.Millisecond
	if elapsed < min {
		t.Fatalf("rate limit not enforced: %d events at %g qps took %v, want >= %v", n, qps, elapsed, min)
	}
}

// A cancelled context surfaces from the limiter wait rather than blocking.
func TestRateLimitExecutor_ContextCancel(t *testing.T) {
	rl := &rateLimitExecutor{next: ExecutorFunc(noopExec), limiter: newRateLimiter(0.001)} // ~1000s spacing
	c := connID(1)
	// First call consumes the single burst token immediately.
	if err := rl.Exec(context.Background(), c, ev(c, 1, "stmt")); err != nil {
		t.Fatalf("first Exec: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := rl.Exec(ctx, c, ev(c, 2, "stmt"))
	if err == nil {
		t.Fatal("expected context error when limiter wait exceeds deadline")
	}
}

// --- Error policy (R7.6, R12.4) ----------------------------------------------

var errTarget = errors.New("target execution failed")

// Skip swallows the failure and continues (returns nil), executing exactly once.
func TestErrorPolicy_Skip(t *testing.T) {
	var calls int32
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		atomic.AddInt32(&calls, 1)
		return errTarget
	})
	ep := newErrorPolicyExecutor(next, Skip, nil)
	c := connID(1)
	if err := ep.Exec(context.Background(), c, ev(c, 1, "stmt")); err != nil {
		t.Fatalf("skip must swallow the error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("skip must execute exactly once, got %d", calls)
	}
}

// Retry re-attempts up to the bound; if a later attempt succeeds it returns nil.
func TestErrorPolicy_RetrySucceedsBeforeBound(t *testing.T) {
	var calls int32
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return errTarget // fail the first two attempts
		}
		return nil // succeed on the third
	})
	ep := newErrorPolicyExecutor(next, Retry, nil)
	c := connID(1)
	if err := ep.Exec(context.Background(), c, ev(c, 1, "stmt")); err != nil {
		t.Fatalf("retry should eventually succeed, got %v", err)
	}
	// initial attempt + 2 retries = 3 total.
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
}

// Retry that never succeeds exhausts its bound then skips (returns nil) so a
// lane is never wedged by a permanently failing statement.
func TestErrorPolicy_RetryExhaustsThenSkips(t *testing.T) {
	var calls int32
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		atomic.AddInt32(&calls, 1)
		return errTarget
	})
	ep := newErrorPolicyExecutor(next, Retry, nil)
	c := connID(1)
	if err := ep.Exec(context.Background(), c, ev(c, 1, "stmt")); err != nil {
		t.Fatalf("exhausted retry must skip (nil), got %v", err)
	}
	// initial attempt + defaultRetryAttempts retries.
	want := int32(1 + defaultRetryAttempts)
	if calls != want {
		t.Fatalf("expected %d attempts, got %d", want, calls)
	}
}

// Abort invokes the abort hook (which stops replay) and propagates the error.
func TestErrorPolicy_AbortSignals(t *testing.T) {
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		return errTarget
	})
	var aborted int32
	ep := newErrorPolicyExecutor(next, Abort, func() { atomic.AddInt32(&aborted, 1) })
	c := connID(1)
	err := ep.Exec(context.Background(), c, ev(c, 1, "stmt"))
	if err == nil {
		t.Fatal("abort must propagate the failure")
	}
	if atomic.LoadInt32(&aborted) != 1 {
		t.Fatalf("abort hook should fire exactly once, got %d", aborted)
	}
}

// A success never triggers the policy regardless of which policy is configured.
func TestErrorPolicy_SuccessPassesThrough(t *testing.T) {
	for _, p := range []ErrorPolicy{Skip, Retry, Abort} {
		var calls int32
		next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
			atomic.AddInt32(&calls, 1)
			return nil
		})
		ep := newErrorPolicyExecutor(next, p, func() { t.Fatalf("abort must not fire on success") })
		c := connID(1)
		if err := ep.Exec(context.Background(), c, ev(c, 1, "stmt")); err != nil {
			t.Fatalf("policy %v: unexpected error %v", p, err)
		}
		if calls != 1 {
			t.Fatalf("policy %v: expected 1 execution, got %d", p, calls)
		}
	}
}

// A failure while the context is already cancelled is a graceful stop, not a
// target failure: it must not be retried or swallowed.
func TestErrorPolicy_ContextDoneShortCircuits(t *testing.T) {
	var calls int32
	next := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		atomic.AddInt32(&calls, 1)
		return errTarget
	})
	ep := newErrorPolicyExecutor(next, Retry, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := connID(1)
	if err := ep.Exec(ctx, c, ev(c, 1, "stmt")); err == nil {
		t.Fatal("a failure under a cancelled context should propagate")
	}
	if calls != 1 {
		t.Fatalf("cancelled context must not trigger retries, got %d calls", calls)
	}
}

// --- Composition (buildExecutor) ---------------------------------------------

// buildExecutor stacks the decorators so a statement is paced, then rate
// limited, then executed under the error policy. Verify the full chain runs and
// the error policy is applied around the base executor.
func TestBuildExecutor_Composition(t *testing.T) {
	var baseCalls int32
	base := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		atomic.AddInt32(&baseCalls, 1)
		return errTarget // base always fails
	})
	fs := &fakeSleeper{}
	cfg := Config{SpeedFactor: 1.0, RateLimitQPS: 1000, ErrorPolicy: Skip}
	ex := buildExecutor(cfg, base, nil, fs.sleep)

	c := connID(1)
	base0 := time.Unix(0, 0)
	for i := 0; i < 3; i++ {
		e := ev(c, uint64(i+1), "stmt")
		e.Timestamp = base0.Add(time.Duration(i) * 100 * time.Millisecond)
		// Skip policy means the chain swallows the base failure.
		if err := ex.Exec(context.Background(), c, e); err != nil {
			t.Fatalf("composed chain should swallow under skip, got %v", err)
		}
	}
	if baseCalls != 3 {
		t.Fatalf("expected base executor hit 3 times, got %d", baseCalls)
	}
	// Pacing was active (factor 1.0): the 2nd and 3rd statements recorded gaps.
	if len(fs.recorded()) != 2 {
		t.Fatalf("expected 2 pacing sleeps, got %v", fs.recorded())
	}
}

// Abort wired through buildExecutor cancels the provided run context.
func TestBuildExecutor_AbortCancelsContext(t *testing.T) {
	base := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
		return errTarget
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := Config{ErrorPolicy: Abort}
	ex := buildExecutor(cfg, base, cancel, realSleep)

	c := connID(1)
	_ = ex.Exec(ctx, c, ev(c, 1, "stmt"))
	select {
	case <-ctx.Done():
		// expected: abort cancelled the run context
	default:
		t.Fatal("abort policy should have cancelled the run context")
	}
}

func noopExec(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error { return nil }
