package main

import (
	"context"
	"io"
	"net/netip"
	"testing"
	"time"

	"pgshadow/pkg/config"
	"pgshadow/pkg/core"
	"pgshadow/pkg/queue"
)

// --- shutdown test doubles ---------------------------------------------------

// drainingReplayer is a replayer.Replayer that mimics the real consumer's
// shutdown contract: it dequeues every available event and returns only when
// the queue reports closed-and-drained (Dequeue ok=false). Recorded events let
// a test assert the drain handed off the whole backlog before stopping.
type drainingReplayer struct {
	q        queue.Queue
	executed []*core.SQLEvent
}

func (r *drainingReplayer) Run(ctx context.Context) error {
	for {
		ev, ok := r.q.Dequeue(ctx)
		if !ok {
			return nil
		}
		r.executed = append(r.executed, ev)
	}
}

// blockingReplayer simulates an in-flight worker that is stuck on the
// Target_Database: Run blocks until its context is cancelled, modeling a stall
// that only the drain's bounded deadline (via consumerCancel) can break.
type blockingReplayer struct {
	started chan struct{}
	stopped chan struct{}
}

func newBlockingReplayer() *blockingReplayer {
	return &blockingReplayer{started: make(chan struct{}), stopped: make(chan struct{})}
}

func (r *blockingReplayer) Run(ctx context.Context) error {
	close(r.started)
	<-ctx.Done()
	close(r.stopped)
	return ctx.Err()
}

func shutdownEvent(seq uint64) *core.SQLEvent {
	return &core.SQLEvent{
		Conn: core.ConnID{
			SrcIP:   netip.MustParseAddr("10.0.0.1"),
			SrcPort: 51000,
			DstIP:   netip.MustParseAddr("10.0.0.2"),
			DstPort: 5432,
		},
		SQL: "INSERT INTO t VALUES (1)",
		Seq: seq,
	}
}

// --- tests -------------------------------------------------------------------

// TestDrainDrainsQueueAndClosesInOrder asserts the graceful path (R12.5): on
// shutdown the producer goroutines exit, the buffer queue is fully drained into
// the replayer, the consumer returns, and the pool and capture handle are
// closed. It also confirms the drain nils the resources so the deferred
// components.Close would not double-close them.
func TestDrainDrainsQueueAndClosesInOrder(t *testing.T) {
	q, err := queue.New(queue.Config{Type: "ringbuffer", Capacity: 100}, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		q.Enqueue(shutdownEvent(i))
	}

	capt := &fakeCapture{}
	pool := &fakePool{}
	dr := &drainingReplayer{q: q}
	c := &components{
		cfg:      &config.Config{},
		capture:  capt,
		queue:    q,
		pool:     pool,
		replayer: dr,
	}

	// ctx models the signal context; cancelling it is the shutdown signal.
	ctx, cancel := context.WithCancel(context.Background())
	p, err := startPipeline(ctx, c)
	if err != nil {
		t.Fatalf("startPipeline: %v", err)
	}

	cancel() // SIGINT/SIGTERM equivalent

	done := make(chan struct{})
	go func() {
		p.drainWithin(5*time.Second, io.Discard)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("drain did not complete")
	}

	// The whole backlog reached the replayer before it stopped (queue drained).
	if len(dr.executed) != 3 {
		t.Fatalf("replayer executed %d events, want 3 (queue not fully drained)", len(dr.executed))
	}
	// Consumer returned (queue closed and drained).
	select {
	case <-p.consumerDone:
	default:
		t.Error("consumer did not stop after drain")
	}
	if !capt.closed {
		t.Error("capture handle was not closed during drain")
	}
	if !pool.closed {
		t.Error("connection pool was not closed during drain")
	}
	// Resources were nil'd so the deferred components.Close cannot double-close.
	if c.capture != nil || c.queue != nil || c.pool != nil {
		t.Error("drained resources were not detached from components")
	}
}

// TestDrainDeadlineForcesExit asserts the bounded shutdown deadline (R12.5): if
// an in-flight replay worker stalls, the drain still completes within the
// deadline by forcing the replayer to stop and closing the pool.
func TestDrainDeadlineForcesExit(t *testing.T) {
	q, err := queue.New(queue.Config{Type: "ringbuffer", Capacity: 100}, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	q.Enqueue(shutdownEvent(1)) // an event left undrained by the stalled worker

	capt := &fakeCapture{}
	pool := &fakePool{}
	br := newBlockingReplayer()
	c := &components{
		cfg:      &config.Config{},
		capture:  capt,
		queue:    q,
		pool:     pool,
		replayer: br,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p, err := startPipeline(ctx, c)
	if err != nil {
		t.Fatalf("startPipeline: %v", err)
	}
	<-br.started // ensure the (stalled) worker is running
	cancel()

	const drainTimeout = 50 * time.Millisecond
	start := time.Now()
	done := make(chan struct{})
	go func() {
		p.drainWithin(drainTimeout, io.Discard)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not honor its bounded deadline (stalled)")
	}
	elapsed := time.Since(start)

	// The deadline must have been reached (the worker only stops on force).
	if elapsed < drainTimeout {
		t.Errorf("drain completed in %v, expected to wait at least the %v deadline", elapsed, drainTimeout)
	}
	// The stalled worker was forced to stop, and the pool was still closed.
	select {
	case <-br.stopped:
	default:
		t.Error("stalled replayer was not forced to stop by the deadline")
	}
	if !pool.closed {
		t.Error("connection pool was not closed after the drain deadline")
	}
}
