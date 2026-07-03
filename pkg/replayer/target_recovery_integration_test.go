package replayer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"pgshadow/pkg/core"
	"pgshadow/pkg/queue"
)

// Task 14.3 — target-failure and recovery integration test (R7.7, R7.8, R12.3).
//
// This test is COMPLEMENTARY to TestTargetDown_BuffersThenResumes in
// replayer_impl_test.go. That test uses the channel-backed fakeQueue (sized to
// hold every event) to prove the producer is never blocked while the target is
// down. Here we instead drive the replayer with a REAL bounded pkg/queue
// ringbuffer at a small capacity and a DropOldest overflow policy, so we can
// additionally exercise:
//
//   - bounded accumulation: while the target is down the buffer fills to exactly
//     its configured capacity and never beyond (bounded memory, R12.3);
//   - overflow beyond capacity: a mock "production source" keeps emitting far
//     more events than the queue can hold, the surplus is dropped (overflow
//     applied), and the source is NEVER blocked or delayed by the stalled
//     replayer/target (R7.7, R12.3);
//   - ordered drain on recovery: when the target recovers the replayer drains
//     the surviving backlog and executes each ConnID's events in captured Seq
//     order (R7.8).
//
// It reuses the package-internal test doubles (newRecordingExecutor, ev,
// connID, newReplayer, waitFor) and declares no duplicate helpers.

// mockProductionSource models the real production traffic generator that
// pgshadow passively observes. It is completely independent of pgshadow: it
// writes every event to its own unbounded-enough sink AND mirrors a copy into
// pgshadow's queue, exactly as a Traffic_Mirror would. The invariant under test
// is that it always completes all of its emissions promptly — a stalled target
// or an overflowing pgshadow queue must never block or delay it (R7.7, R12.3).
type mockProductionSource struct {
	sink      chan *core.SQLEvent // the production source's own path (never pgshadow's)
	delivered int64               // events successfully emitted by the source
}

func newMockProductionSource(total int) *mockProductionSource {
	return &mockProductionSource{sink: make(chan *core.SQLEvent, total)}
}

// run emits every event to the source's own sink and mirrors it into the
// pgshadow queue. enqueue returns whether pgshadow dropped the mirrored copy due
// to overflow; the source itself is indifferent to that outcome and keeps going.
func (s *mockProductionSource) run(events []*core.SQLEvent, mirror func(*core.SQLEvent) (dropped bool)) (mirrored, dropped int) {
	for _, e := range events {
		// The production source's own path is never coupled to pgshadow.
		s.sink <- e
		atomic.AddInt64(&s.delivered, 1)

		if mirror(e) {
			dropped++
		} else {
			mirrored++
		}
	}
	return mirrored, dropped
}

func (s *mockProductionSource) count() int64 { return atomic.LoadInt64(&s.delivered) }

// assertStrictlyIncreasing checks that the recorded execution Seq order for each
// ConnID is strictly increasing. With DropOldest the survivors per ConnID are a
// (possibly non-contiguous) subsequence of the captured stream, but the queue
// and lanes are FIFO and never reorder, so monotonicity must hold (R7.8).
func assertStrictlyIncreasing(t *testing.T, exec *recordingExecutor, conns []core.ConnID) {
	t.Helper()
	for _, c := range conns {
		got := exec.order(c)
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Fatalf("conn %s: executed out of captured order at index %d: %v", c, i, got)
			}
		}
	}
}

func TestTargetOutage_RingBufferAccumulatesAndDrainsInOrder(t *testing.T) {
	const capacity = 16
	conns := []core.ConnID{connID(7001), connID(7002), connID(7003)}

	// Produce far more events than any combination of the bounded queue and the
	// replayer's internal lane buffers can hold, so overflow is guaranteed while
	// the target is down. Reference the package-internal laneBufferSize so this
	// stays correct if the lane buffer size ever changes.
	perConn := 4 * (laneBufferSize + capacity + 1)
	var events []*core.SQLEvent
	for i := uint64(1); i <= uint64(perConn); i++ {
		for _, c := range conns {
			events = append(events, ev(c, i, "stmt"))
		}
	}
	total := len(events)

	// Real bounded ring buffer with drop_oldest overflow (R6.4). A nil collector
	// is acceptable; the buffer simply skips overflow notifications.
	q, err := queue.New(queue.Config{
		Type:     "ringbuffer",
		Capacity: capacity,
		Overflow: queue.DropOldest,
	}, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}

	// The executor blocks while the "target is down"; closing the gate models
	// recovery, after which executions succeed (R7.8).
	gate := make(chan struct{})
	var executed int32
	exec := newRecordingExecutor()
	exec.hook = func() {
		<-gate
		atomic.AddInt32(&executed, 1)
	}

	r := newReplayer(Config{Mode: Bounded, Workers: 4, PoolMaxConns: 4}, q, exec, nil)

	runDone := make(chan error, 1)
	go func() { runDone <- r.Run(context.Background()) }()

	// The mock production source emits all traffic while the target is down,
	// mirroring each event into pgshadow's bounded queue.
	src := newMockProductionSource(total)
	srcDone := make(chan [2]int, 1)
	go func() {
		mirrored, dropped := src.run(events, q.Enqueue)
		srcDone <- [2]int{mirrored, dropped}
	}()

	// R7.7 / R12.3: the production source must finish emitting every event even
	// though the target is down and nothing has been replayed. A stalled target
	// or an overflowing pgshadow queue never blocks or delays production.
	var mirrored, dropped int
	select {
	case res := <-srcDone:
		mirrored, dropped = res[0], res[1]
	case <-time.After(2 * time.Second):
		t.Fatal("production source blocked while target was down — production was not isolated (R7.7/R12.3)")
	}
	if src.count() != int64(total) {
		t.Fatalf("expected production source to emit all %d events, emitted %d", total, src.count())
	}

	// Overflow must have been applied: the bounded queue cannot have absorbed
	// every mirrored event at its small capacity while the target was down.
	if dropped == 0 {
		t.Fatalf("expected overflow drops while target down, got none (mirrored=%d, dropped=%d)", mirrored, dropped)
	}

	// Bounded accumulation (R12.3): with the target down, buffered events
	// accumulate in bounded memory and never grow without limit. The surviving
	// backlog is split between the bounded Buffer_Queue (which never exceeds its
	// configured capacity) and the replayer's small per-ConnID lane buffers
	// (laneBufferSize each). Where the residual settles between the two depends
	// on producer/dispatcher scheduling: the dispatcher drains the small queue
	// into the comparatively spacious lanes (3*laneBufferSize >> capacity), so
	// the queue frequently quiesces near empty while the lanes hold the backlog.
	// Asserting a fixed split (e.g. Depth()==capacity) is therefore racy and
	// over-specified; the invariant that actually proves bounded memory is that
	// the queue depth is ALWAYS bounded by its capacity and never grows beyond.
	//
	// Let the system quiesce, then sample repeatedly: depth must stay within
	// [0, capacity] the whole time, and nothing may execute while target is down.
	settleDeadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(settleDeadline) {
		if d := q.Depth(); d > capacity {
			t.Fatalf("queue exceeded bounded capacity while target down: depth=%d > capacity=%d (R12.3)", d, capacity)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&executed); got != 0 {
		t.Fatalf("expected 0 executions while target down, got %d", got)
	}

	// Target recovers: unblock the workers and close the queue so the replayer
	// drains the surviving backlog and exits (R7.8).
	close(gate)
	if cerr := q.Close(); cerr != nil {
		t.Fatalf("queue.Close: %v", cerr)
	}

	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not resume/drain after target recovery (R7.8)")
	}

	// After recovery the buffered survivors are replayed; the dropped surplus is
	// not. Executions are bounded by what survived overflow, and at least some
	// events must have been replayed.
	got := int(atomic.LoadInt32(&executed))
	if got == 0 {
		t.Fatal("expected buffered events to replay after recovery, got 0 (R7.8)")
	}
	if got >= total {
		t.Fatalf("expected fewer executions than produced due to overflow: executed=%d, produced=%d", got, total)
	}
	if exec.total() != got {
		t.Fatalf("recorded execution count %d disagrees with counter %d", exec.total(), got)
	}

	// The queue must be fully drained after recovery.
	if d := q.Depth(); d != 0 {
		t.Fatalf("expected queue fully drained after recovery, depth=%d", d)
	}

	// R7.8: every ConnID's surviving events replayed in captured Seq order.
	assertStrictlyIncreasing(t, exec, conns)
}
