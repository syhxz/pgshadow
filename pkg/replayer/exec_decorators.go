// Package replayer — pacing, rate limiting, and error policy (task 10.3).
//
// This file adds three Executor decorators that compose around the production
// poolExecutor (task 10.2) without touching the routing/concurrency logic. Each
// decorator wraps a "next" Executor so they can be stacked in any order and so
// task 10.4's Greenplum dialect decorator can slot into the same chain.
//
//   - pacingExecutor     paces inter-statement delay by scaling the captured
//     inter-arrival gap (SpeedFactor): 1.0 reproduces the
//     original timing, 2.0 replays at twice the rate (half
//     the gap), 0 replays as fast as possible (R7.4, R7.14).
//   - rateLimitExecutor  enforces a token-bucket QPS ceiling via x/time/rate
//     when RateLimitQPS > 0; <= 0 means unlimited (R7.3).
//   - errorPolicyExecutor applies skip/retry/abort on a target execution
//     failure so processing continues per policy (R7.6,
//     R12.4).
//
// Production composition (built by NewReplayer, outermost first):
//
//	pacing → rate-limit → error-policy → poolExecutor
//
// so that pacing and rate limiting gate a statement BEFORE it executes, and the
// error policy wraps the execution itself. The clock/sleeper is injected so the
// pacing math is exercised deterministically in tests without real sleeps.
package replayer

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"pgshadow/pkg/core"
	"pgshadow/pkg/replayguard"
)

// defaultRetryAttempts bounds how many times the retry error policy re-attempts
// a failing statement before giving up and skipping it so a permanently failing
// event cannot wedge a lane forever (R7.6).
const defaultRetryAttempts = 3

// sleeper delays for d or returns early if ctx is cancelled. It is injected so
// pacing is deterministic and non-blocking in tests (R7.4).
type sleeper func(ctx context.Context, d time.Duration)

// realSleep is the production sleeper: it waits for d using a timer that is
// abandoned promptly on context cancellation so a graceful stop is not delayed
// by an outstanding pacing delay.
func realSleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// --- Pacing (R7.4, R7.14) ----------------------------------------------------

// pacingExecutor delays before each execution to reproduce the original
// inter-statement timing scaled by SpeedFactor. It tracks the capture timestamp
// of the previously paced statement and sleeps the scaled gap before running
// the next one. Pacing is global across lanes: it shapes the overall replay
// rate relative to capture, which is the timing dimension SpeedFactor controls
// (concurrency is governed separately by the replay mode, R7.14).
type pacingExecutor struct {
	next   Executor
	factor float64
	sleep  sleeper

	mu       sync.Mutex
	havePrev bool
	prev     time.Time // capture timestamp of the previous paced statement
}

// newPacingExecutor wraps next with SpeedFactor pacing. A factor <= 0 means
// "as fast as possible" and the returned executor applies no delay.
func newPacingExecutor(next Executor, factor float64, sleep sleeper) *pacingExecutor {
	if sleep == nil {
		sleep = realSleep
	}
	return &pacingExecutor{next: next, factor: factor, sleep: sleep}
}

// Exec sleeps the scaled inter-arrival gap (if any) then delegates.
func (p *pacingExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	p.mu.Lock()
	var delay time.Duration
	if p.havePrev {
		delay = pacingDelay(ev.Timestamp.Sub(p.prev), p.factor)
	}
	p.havePrev = true
	p.prev = ev.Timestamp
	p.mu.Unlock()

	if delay > 0 {
		p.sleep(ctx, delay)
	}
	return p.next.Exec(ctx, conn, ev)
}

// pacingDelay scales a captured inter-arrival gap by SpeedFactor. A factor of
// 1.0 reproduces the original gap, 2.0 halves it (twice the rate), and a factor
// of 0 (or negative) yields no delay (as fast as possible). Non-positive gaps —
// which arise when statements from different lanes interleave out of capture
// order — contribute no delay (R7.4).
func pacingDelay(gap time.Duration, factor float64) time.Duration {
	if factor <= 0 || gap <= 0 {
		return 0
	}
	return time.Duration(float64(gap) / factor)
}

// --- Rate limiting (R7.3) ----------------------------------------------------

// rateLimitExecutor gates each execution through a token-bucket limiter so no
// more than RateLimitQPS statements run per second across all lanes.
type rateLimitExecutor struct {
	next    Executor
	limiter *rate.Limiter
}

// newRateLimiter builds a token-bucket limiter for qps statements per second,
// or returns nil when qps <= 0 (unlimited, R7.3). A burst of 1 keeps any
// one-second window at or under the configured ceiling.
func newRateLimiter(qps float64) *rate.Limiter {
	if qps <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(qps), 1)
}

// Exec waits for a token (honoring ctx) then delegates. A cancelled context
// surfaces as the limiter's wait error so a graceful stop is not blocked.
func (r *rateLimitExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	if err := r.limiter.Wait(ctx); err != nil {
		return err
	}
	return r.next.Exec(ctx, conn, ev)
}

// --- Error policy (R7.6, R12.4) ----------------------------------------------

// errorPolicyExecutor applies the configured error policy when the wrapped
// Executor fails:
//
//   - Skip:  swallow the error and continue with subsequent events.
//   - Retry: re-attempt up to maxAttempts times, then skip (swallow) so a
//     persistently failing statement cannot wedge its lane.
//   - Abort: invoke onAbort (which cancels the replay context) to stop replay;
//     production is unaffected either way (R12.4).
//
// A context that is already done short-circuits policy application so a
// graceful stop is not mistaken for a target failure.
type errorPolicyExecutor struct {
	next        Executor
	policy      ErrorPolicy
	maxAttempts int
	onAbort     func()   // stops replay on abort; nil is a no-op
	resetExec   Executor // used to issue ROLLBACK after skip; nil = no reset
}

// newErrorPolicyExecutor wraps next with the given error policy. The abort hook
// is called at most once when the policy is Abort and an execution fails.
func newErrorPolicyExecutor(next Executor, policy ErrorPolicy, onAbort func()) *errorPolicyExecutor {
	return &errorPolicyExecutor{
		next:        next,
		policy:      policy,
		maxAttempts: defaultRetryAttempts,
		onAbort:     onAbort,
	}
}

// Exec runs the statement and applies the error policy on failure.
func (e *errorPolicyExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	err := e.next.Exec(ctx, conn, ev)
	if err == nil {
		return nil
	}
	// A cancelled/expired context is a graceful stop, not a target failure:
	// propagate it without applying skip/retry/abort.
	if ctx.Err() != nil {
		return err
	}

	switch e.policy {
	case Retry:
		for attempt := 0; attempt < e.maxAttempts; attempt++ {
			if ctx.Err() != nil {
				return err
			}
			if err = e.next.Exec(ctx, conn, ev); err == nil {
				return nil
			}
		}
		// Retries exhausted: fall through to skip so processing continues.
		// Reset the connection's transaction state so subsequent statements
		// on this affinity connection don't stall on an aborted transaction.
		e.resetTxState(ctx, conn)
		return nil
	case Abort:
		if e.onAbort != nil {
			e.onAbort()
		}
		return err
	default: // Skip (R7.6 default): swallow and continue (R12.4).
		// Reset the connection's transaction state so subsequent statements
		// on this affinity connection don't stall on an aborted transaction.
		// Without this, a failed DDL/DML leaves the connection in "aborted"
		// state, causing all following SQL to fail and locks to be held until
		// the connection is closed.
		e.resetTxState(ctx, conn)
		return nil
	}
}

// resetTxState issues a ROLLBACK on the connection to clear any aborted
// transaction state. This is a best-effort cleanup — if it fails (e.g. ctx
// done), the connection remains dirty but the lane will eventually retire it.
// Uses the resetExec hook if set (production), otherwise falls back to e.next.
func (e *errorPolicyExecutor) resetTxState(ctx context.Context, conn core.ConnID) {
	if e.resetExec == nil {
		return
	}
	rollbackEv := &core.SQLEvent{
		Conn: conn,
		SQL:  "ROLLBACK",
	}
	_ = e.resetExec.Exec(ctx, conn, rollbackEv)
}

// buildExecutor composes the production execution seam around base, layering
// (innermost → outermost) dialect handling, error policy, rate limiting, pacing,
// then safeguard filtering. The result is what a lane worker invokes: safeguard
// drops/rewrites events before they consume pacing delay or rate-limit tokens,
// pacing and rate limiting gate a statement before it runs, the error policy
// wraps the execution, and dialect handling (task 10.4) shapes the statement
// closest to the target (e.g. greenplum drops transaction-control statements
// and runs each statement standalone, R8.5). onAbort stops the replay when the
// error policy is Abort; sleep is injected for deterministic tests (production
// passes realSleep); sg may be nil to skip safeguard filtering.
func buildExecutor(cfg Config, base Executor, onAbort func(), sleep sleeper, sg ...*replayguard.Guard) Executor {
	ex := base
	// Innermost: dialect handling sits closest to the base executor so it is
	// the last decorator a statement passes through before reaching the target
	// (task 10.4). For postgresql this is a pass-through (R8.2).
	ex = newDialectExecutor(ex, cfg.TargetDialect)
	ex = newErrorPolicyExecutor(ex, cfg.ErrorPolicy, onAbort)
	if lim := newRateLimiter(cfg.RateLimitQPS); lim != nil {
		ex = &rateLimitExecutor{next: ex, limiter: lim}
	}
	if cfg.SpeedFactor > 0 {
		ex = newPacingExecutor(ex, cfg.SpeedFactor, sleep)
	}
	// Outermost: safeguard filtering drops blocked events before they consume
	// pacing delay or rate-limit tokens.
	if len(sg) > 0 && sg[0] != nil {
		ex = newReplayguardExecutor(ex, sg[0])
	}
	return ex
}
