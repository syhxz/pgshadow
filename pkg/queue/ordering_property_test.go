package queue

import (
	"context"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Property 16 verifies that the in-memory ring buffer preserves per-ConnID
// ordering. It reuses the test doubles declared in ringbuffer_test.go (connEv).
//
// For any interleaved multi-ConnID stream of SQL_Events enqueued and then fully
// dequeued without overflow, the subsequence of events for each ConnID emerges
// in enqueue (captured) order. The generator builds a stream by interleaving
// ConnIDs drawn from a small pool; each ConnID is assigned a strictly
// increasing Seq in the order it is enqueued. With a capacity large enough to
// hold the whole stream, no event is discarded, so every enqueued event is
// observed on dequeue. The check asserts that, for each ConnID, the dequeued
// Seq subsequence is strictly increasing — i.e. it matches enqueue order.
//
// Feature: pgshadow, Property 16: The queue preserves per-ConnID ordering
// Validates: Requirements 6.6, 6.7
func TestProperty16QueuePreservesPerConnIDOrdering(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100 // >= 100 iterations

	properties := gopter.NewProperties(params)

	// p16 connection pool: a small set of source ports standing in for ConnIDs.
	const p16NumConns = 4
	p16Ports := func(idx int) uint16 { return uint16(5000 + idx) }

	// p16StreamGen produces an interleaved stream of ConnID indices into the
	// pool. Each element selects which ConnID enqueues next; the per-ConnID Seq
	// is derived from the running count for that ConnID (strictly increasing).
	p16StreamGen := gen.SliceOf(gen.IntRange(0, p16NumConns-1)).WithLabel("stream")

	properties.Property("ring buffer preserves per-ConnID enqueue order", prop.ForAll(
		func(stream []int) bool {
			// Capacity large enough to hold the entire stream => no overflow.
			capacity := len(stream) + 1
			q, err := New(Config{Type: "ringbuffer", Capacity: capacity, Overflow: DropOldest}, nil)
			if err != nil {
				return false
			}
			defer q.Close()

			// Assign a strictly increasing per-ConnID Seq in enqueue order and
			// enqueue the interleaved stream.
			nextSeq := make([]uint64, p16NumConns)
			for _, idx := range stream {
				seq := nextSeq[idx]
				nextSeq[idx]++
				if dropped := q.Enqueue(connEv(p16Ports(idx), seq)); dropped {
					// No overflow expected given the capacity choice.
					return false
				}
			}

			// Fully dequeue and verify per-ConnID Seq is strictly increasing.
			ctx := context.Background()
			last := make(map[uint16]int64, p16NumConns)
			for range stream {
				e, ok := q.Dequeue(ctx)
				if !ok {
					return false
				}
				port := e.Conn.SrcPort
				if prev, seen := last[port]; seen && int64(e.Seq) <= prev {
					// Order violated for this ConnID.
					return false
				}
				last[port] = int64(e.Seq)
			}
			return true
		},
		p16StreamGen,
	))

	properties.TestingRun(t)
}
