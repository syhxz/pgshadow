package queue

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Property 15 verifies that the in-memory ring-buffer backend bounds memory and
// accounts for every overflow. It reuses the test doubles declared in
// ringbuffer_test.go (ev, fakeCollector, collectorPtr).
//
// For any sequence of enqueue/dequeue operations against a ring buffer of any
// configured capacity and any discarding overflow policy (drop_oldest /
// drop_newest), the buffer depth never exceeds the configured capacity at any
// observable point, and the number of overflow-counter increments recorded by
// the collector equals the number of Enqueue calls that reported dropped=true.
//
// Feature: pgshadow, Property 15: The queue bounds memory and accounts for every overflow
// Validates: Requirements 6.3, 6.4, 6.5, 12.2
func TestProperty15QueueBoundingAndOverflowAccounting(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 200 // >= 100 iterations

	properties := gopter.NewProperties(params)

	properties.Property("ring buffer bounds depth and accounts every overflow", prop.ForAll(
		func(capacity int, policyIdx int, ops []bool) bool {
			// The block policy would deadlock a single-threaded driver when the
			// buffer is full, so the generator only selects the two discarding
			// policies, matching the property's overflow-accounting claim.
			policy := DropOldest
			if policyIdx == 1 {
				policy = DropNewest
			}

			fc := &fakeCollector{}
			q, err := New(Config{Type: "ringbuffer", Capacity: capacity, Overflow: policy}, collectorPtr(fc))
			if err != nil {
				return false
			}
			defer q.Close()

			ctx := context.Background()
			var seq uint64
			var dropCount int64

			for _, enqueue := range ops {
				if enqueue {
					if q.Enqueue(ev(seq)) {
						dropCount++
					}
					seq++
				} else if q.Depth() > 0 {
					// Only dequeue when non-empty so the call returns
					// immediately instead of blocking on an empty buffer.
					if _, ok := q.Dequeue(ctx); !ok {
						return false
					}
				}

				// Invariant: depth must never exceed the configured capacity at
				// any observable point in the operation sequence (R6.3, R12.2).
				if q.Depth() > capacity {
					return false
				}
			}

			// Every discarded event must have incremented the overflow counter
			// exactly once (R6.5, R12.2).
			return atomic.LoadInt64(&fc.overflows) == dropCount
		},
		gen.IntRange(1, 64).WithLabel("capacity"),
		gen.IntRange(0, 1).WithLabel("policyIdx"),
		gen.SliceOf(gen.Bool()).WithLabel("ops"),
	))

	properties.TestingRun(t)
}
