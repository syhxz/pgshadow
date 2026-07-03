package replayer

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"

	"pgshadow/pkg/core"
)

// Property 17 verifies that the Replayer preserves each ConnID's captured Seq
// order, in every concurrency mode. It reuses the test doubles declared in
// replayer_impl_test.go (fakeQueue/newFakeQueue, recordingExecutor/
// newRecordingExecutor, ExecutorFunc, ev, connID, newReplayer, runToCompletion).
//
// For any concurrency mode (Bounded/Faithful/Serial) and any interleaved
// multi-ConnID stream of SQL_Events, the order in which events sharing a single
// ConnID execute equals their captured Seq order. The generator builds a stream
// by interleaving ConnIDs drawn from a small pool; each ConnID is assigned a
// strictly increasing Seq in the order it is enqueued (so enqueue order == Seq
// order per ConnID). After running the replayer to completion with a recording
// executor, the check asserts that, for each ConnID, the executed Seq
// subsequence is strictly increasing — i.e. it matches captured order.
//
// Feature: pgshadow, Property 17: Replay preserves per-ConnID captured order in every concurrency mode
// Validates: Requirements 7.10, 7.11, 7.12, 7.13
func TestProperty17ReplayPreservesPerConnIDCapturedOrder(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100 // >= 100 iterations

	properties := gopter.NewProperties(params)

	// p17 connection pool: a small set of ConnIDs (by source port) standing in
	// for distinct captured connections.
	const p17NumConns = 4
	p17Conn := func(idx int) core.ConnID { return connID(uint16(6000 + idx)) }
	p17Modes := []ConcurrencyMode{Bounded, Faithful, Serial}

	// p17StreamGen produces an interleaved stream of ConnID indices into the
	// pool. Each element selects which ConnID enqueues next; the per-ConnID Seq
	// is derived from the running count for that ConnID (strictly increasing).
	p17StreamGen := gen.SliceOf(gen.IntRange(0, p17NumConns-1)).WithLabel("stream")
	// p17ModeGen selects one of the three concurrency modes (R7.10/7.11/7.12).
	p17ModeGen := gen.IntRange(0, len(p17Modes)-1).WithLabel("mode")

	properties.Property("replay preserves per-ConnID captured Seq order in every mode", prop.ForAll(
		func(stream []int, modeIdx int) bool {
			mode := p17Modes[modeIdx]

			// Assign a strictly increasing per-ConnID Seq in enqueue order and
			// build the interleaved event stream.
			nextSeq := make([]uint64, p17NumConns)
			events := make([]*core.SQLEvent, 0, len(stream))
			for _, idx := range stream {
				seq := nextSeq[idx] + 1 // Seq is 1-based and strictly increasing
				nextSeq[idx] = seq
				events = append(events, ev(p17Conn(idx), seq, "stmt"))
			}

			exec := newRecordingExecutor()
			q := newFakeQueue(len(events) + 1)
			r := newReplayer(Config{
				Mode:           mode,
				Workers:        8,
				MaxConcurrency: 8,
				PoolMaxConns:   8,
			}, q, exec, nil)

			runToCompletion(t, r, q, events)

			// All enqueued events must have executed exactly once in total.
			if exec.total() != len(events) {
				return false
			}

			// For each ConnID, the executed Seq subsequence must be strictly
			// increasing, matching captured order (R7.13).
			for idx := 0; idx < p17NumConns; idx++ {
				if nextSeq[idx] == 0 {
					continue // this ConnID never appeared in the stream
				}
				got := exec.order(p17Conn(idx))
				if uint64(len(got)) != nextSeq[idx] {
					return false
				}
				for i := 1; i < len(got); i++ {
					if got[i] <= got[i-1] {
						return false
					}
				}
			}
			return true
		},
		p17StreamGen,
		p17ModeGen,
	))

	properties.TestingRun(t)
}
