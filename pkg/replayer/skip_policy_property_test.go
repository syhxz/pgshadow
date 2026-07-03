package replayer

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"

	"pgshadow/pkg/core"
)

// Property 18 verifies that, under the skip error policy, a target execution
// failure never halts the replay: the Replayer executes every non-failing
// event and keeps processing all subsequent events past each failure. It
// reuses the test doubles declared alongside this package (fakeQueue/
// newFakeQueue, ExecutorFunc, ev, connID, newReplayer, buildExecutor,
// runToCompletion, errTarget).
//
// The error-policy decorator is wired with ErrorPolicy=Skip (via buildExecutor)
// around a mock base Executor that fails for an arbitrary, generated subset of
// the events and records every attempt plus every success. After running a
// generated multi-ConnID stream to completion, the check asserts:
//
//   - every event was attempted exactly once at the base executor (processing
//     reached the end of the stream — no early stop), and
//   - every non-failing event executed successfully, while failing events were
//     attempted but not recorded as successes.
//
// The generator encodes each stream element as a single int in [0, 2*conns):
// the low half selects a succeeding event, the high half a failing event, with
// the ConnID derived by modulo so failures and successes interleave across
// lanes. Skip swallows failures (R7.6 default) so the replay continues without
// affecting the rest of the stream (R12.4).
//
// Feature: pgshadow, Property 18: Under the skip error policy, processing continues past failures
// Validates: Requirements 7.6, 12.4
func TestProperty18SkipPolicyContinuesPastFailures(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100 // >= 100 iterations

	properties := gopter.NewProperties(params)

	// p18 connection pool: a small set of ConnIDs standing in for distinct
	// captured connections so failures and successes interleave across lanes.
	const p18NumConns = 4
	p18Conn := func(idx int) core.ConnID { return connID(uint16(7000 + idx)) }

	// p18StreamGen yields a stream of ints in [0, 2*NumConns). For element v the
	// ConnID is v % NumConns and the event fails iff v >= NumConns. This couples
	// "which ConnID" and "fail vs succeed" into one generated value per event so
	// the failing subset is arbitrary.
	p18StreamGen := gen.SliceOf(gen.IntRange(0, 2*p18NumConns-1)).WithLabel("stream")

	properties.Property("skip policy executes every non-failing event and never stops early", prop.ForAll(
		func(stream []int) bool {
			// Build the interleaved event stream. Each event gets a globally
			// unique SQL id and a strictly increasing per-ConnID Seq (enqueue
			// order). failSet marks which event ids must fail at the target.
			nextSeq := make([]uint64, p18NumConns)
			events := make([]*core.SQLEvent, 0, len(stream))
			failSet := make(map[string]bool)
			wantSuccess := 0
			for i, v := range stream {
				connIdx := v % p18NumConns
				fail := v >= p18NumConns

				id := fmt.Sprintf("e%d", i) // globally unique per event
				seq := nextSeq[connIdx] + 1
				nextSeq[connIdx] = seq
				events = append(events, ev(p18Conn(connIdx), seq, id))
				if fail {
					failSet[id] = true
				} else {
					wantSuccess++
				}
			}

			// Mock base Executor: records every attempt, fails for the generated
			// subset, and records the rest as successes.
			var mu sync.Mutex
			attempts := make(map[string]bool)
			successes := make(map[string]bool)
			base := ExecutorFunc(func(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
				mu.Lock()
				attempts[e.SQL] = true
				mu.Unlock()
				if failSet[e.SQL] {
					return errTarget // simulate a Target_Database failure
				}
				mu.Lock()
				successes[e.SQL] = true
				mu.Unlock()
				return nil
			})

			// Wire the Skip error policy around the failing base. SpeedFactor 0
			// (no pacing) and RateLimitQPS 0 (unlimited) keep the chain free of
			// timing so the test is fast and deterministic; the default dialect
			// is a pass-through.
			cfg := Config{
				Mode:           Bounded,
				Workers:        8,
				MaxConcurrency: 8,
				PoolMaxConns:   8,
				ErrorPolicy:    Skip,
			}
			exec := buildExecutor(cfg, base, nil, realSleep)

			q := newFakeQueue(len(events) + 1)
			r := newReplayer(cfg, q, exec, nil)
			runToCompletion(t, r, q, events)

			mu.Lock()
			defer mu.Unlock()

			// Processing reached the end: every event was attempted exactly
			// once (skip does not retry), so none of the failures stopped the
			// replay short (R12.4).
			if len(attempts) != len(events) {
				return false
			}
			// Every non-failing event executed successfully (R7.6).
			if len(successes) != wantSuccess {
				return false
			}
			for _, e := range events {
				attempted := attempts[e.SQL]
				succeeded := successes[e.SQL]
				if !attempted {
					return false // an event was never processed — early stop
				}
				if failSet[e.SQL] {
					if succeeded {
						return false // a failing event must not be a success
					}
				} else if !succeeded {
					return false // a non-failing event must have executed
				}
			}
			return true
		},
		p18StreamGen,
	))

	properties.TestingRun(t)
}
