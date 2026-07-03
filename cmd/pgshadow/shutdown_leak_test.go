package main

import (
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/jackc/pgx/v5/pgxpool"

	"pgshadow/pkg/capture"
	"pgshadow/pkg/config"
	"pgshadow/pkg/core"
	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
)

// This file holds the complementary shutdown unit tests for task 12.5: a
// goroutine-leak check and an explicit drain-ordering assertion. They live
// alongside shutdown_test.go's behavioural drain tests and reuse the existing
// fakes/doubles (fakeCapture, fakePool, drainingReplayer) where possible,
// adding only distinctly-named instrumented doubles that record close ordering.

// --- ordering instrumentation ------------------------------------------------

// orderLog is a concurrency-safe append-only record of lifecycle events. The
// drain closes resources from several goroutines (the consumer returns from its
// own goroutine), so ordering is recorded under a mutex.
type orderLog struct {
	mu     sync.Mutex
	events []string
}

func (l *orderLog) add(event string) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *orderLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// indexOf returns the position of the first occurrence of event, or -1.
func indexOf(events []string, event string) int {
	for i, e := range events {
		if e == event {
			return i
		}
	}
	return -1
}

// orderingCapture is a capture.Source that records the moment it is closed so a
// test can prove capture is stopped before the pool is closed (R12.5).
type orderingCapture struct {
	log    *orderLog
	closed bool
}

func (c *orderingCapture) Packets() <-chan gopacket.Packet { return nil }
func (c *orderingCapture) Stats() (capture.CaptureStats, error) {
	return capture.CaptureStats{}, nil
}
func (c *orderingCapture) Close() error {
	c.closed = true
	c.log.add("capture-close")
	return nil
}

// orderingQueue wraps a real queue and records when it is closed (the drain
// step that hands the consumer the remaining backlog). Dequeue/Enqueue delegate
// to the embedded queue so the ring buffer's real drain-on-close semantics
// drive the consumer.
type orderingQueue struct {
	queue.Queue
	log *orderLog
}

func (q *orderingQueue) Close() error {
	q.log.add("queue-close")
	return q.Queue.Close()
}

// orderingPool is a replayer.Pool that records the moment it is closed.
type orderingPool struct {
	log    *orderLog
	closed bool
}

func (p *orderingPool) AcquireFor(context.Context, core.ConnID) (*pgxpool.Conn, error) {
	return nil, nil
}
func (p *orderingPool) Release(core.ConnID)       {}
func (p *orderingPool) Stats() replayer.PoolStats { return replayer.PoolStats{} }
func (p *orderingPool) Close() {
	p.closed = true
	p.log.add("pool-close")
}

// orderingReplayer drains the queue exactly like drainingReplayer (dequeue
// until the queue reports closed-and-drained) and records when it finally
// stops, so a test can prove the queue is fully drained into the consumer
// before the pool is closed.
type orderingReplayer struct {
	q   queue.Queue
	log *orderLog
}

func (r *orderingReplayer) Run(ctx context.Context) error {
	for {
		if _, ok := r.q.Dequeue(ctx); !ok {
			r.log.add("consumer-stop")
			return nil
		}
	}
}

// --- goroutine settling helpers ----------------------------------------------

// settleGoroutines waits until the live goroutine count is stable for a few
// consecutive samples and returns it. It establishes a baseline that is robust
// to goroutines lingering from earlier tests in the same process.
func settleGoroutines() int {
	prev := runtime.NumGoroutine()
	stable := 0
	for i := 0; i < 200; i++ {
		time.Sleep(5 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == prev {
			if stable++; stable >= 5 {
				return n
			}
			continue
		}
		stable = 0
		prev = n
	}
	return runtime.NumGoroutine()
}

// assertGoroutinesReturnTo polls until the live goroutine count falls back to
// at most baseline (allowing for the scheduler to retire just-finished
// goroutines), failing the test if it has not within a generous deadline.
func assertGoroutinesReturnTo(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var n int
	for {
		runtime.Gosched()
		n = runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak after drain: have %d, baseline %d", n, baseline)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// --- tests -------------------------------------------------------------------

// TestPipelineNoGoroutineLeakAfterDrain proves the graceful drain (R12.5) exits
// every goroutine startPipeline spawns: the producer reader, the
// reassembler/processing loop, the wg-waiter that closes producerDone, and the
// consumer. Goroutine counts must return to the pre-start baseline.
func TestPipelineNoGoroutineLeakAfterDrain(t *testing.T) {
	baseline := settleGoroutines()

	q, err := queue.New(queue.Config{Type: "ringbuffer", Capacity: 100}, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	for i := uint64(1); i <= 5; i++ {
		q.Enqueue(shutdownEvent(i))
	}

	c := &components{
		cfg:      &config.Config{},
		capture:  &fakeCapture{},
		queue:    q,
		pool:     &fakePool{},
		replayer: &drainingReplayer{q: q},
	}

	ctx, cancel := context.WithCancel(context.Background())
	p, err := startPipeline(ctx, c)
	if err != nil {
		t.Fatalf("startPipeline: %v", err)
	}

	// While running, the pipeline must have spawned its goroutines.
	if runtime.NumGoroutine() <= baseline {
		t.Fatalf("expected running pipeline to add goroutines (have %d, baseline %d)",
			runtime.NumGoroutine(), baseline)
	}

	cancel()
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

	// Every pipeline goroutine must have exited: count returns to baseline.
	assertGoroutinesReturnTo(t, baseline)
}

// TestDrainOrderCaptureBeforeQueueBeforePool asserts the precise drain ordering
// mandated by R12.5: capture is stopped, then the queue is drained/closed, then
// (after the consumer has dequeued the whole backlog and stopped) the pool is
// closed — in that order.
func TestDrainOrderCaptureBeforeQueueBeforePool(t *testing.T) {
	log := &orderLog{}

	rb, err := queue.New(queue.Config{Type: "ringbuffer", Capacity: 100}, nil)
	if err != nil {
		t.Fatalf("queue.New: %v", err)
	}
	for i := uint64(1); i <= 3; i++ {
		rb.Enqueue(shutdownEvent(i))
	}
	q := &orderingQueue{Queue: rb, log: log}

	capt := &orderingCapture{log: log}
	pool := &orderingPool{log: log}
	c := &components{
		cfg:      &config.Config{},
		capture:  capt,
		queue:    q,
		pool:     pool,
		replayer: &orderingReplayer{q: q, log: log},
	}

	ctx, cancel := context.WithCancel(context.Background())
	p, err := startPipeline(ctx, c)
	if err != nil {
		t.Fatalf("startPipeline: %v", err)
	}
	cancel()

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

	events := log.snapshot()
	captureAt := indexOf(events, "capture-close")
	queueAt := indexOf(events, "queue-close")
	consumerAt := indexOf(events, "consumer-stop")
	poolAt := indexOf(events, "pool-close")

	if captureAt < 0 || queueAt < 0 || consumerAt < 0 || poolAt < 0 {
		t.Fatalf("missing drain events, got order %v", events)
	}
	// Capture is stopped before the queue is drained (no new events enter).
	if !(captureAt < queueAt) {
		t.Errorf("capture must close before queue drains; order %v", events)
	}
	// The queue is drained into the consumer before the consumer is allowed to
	// stop, and the consumer stops before the pool closes (so no worker is
	// mid-execution against a closing pool).
	if !(queueAt < consumerAt) {
		t.Errorf("queue must close before consumer stops; order %v", events)
	}
	if !(consumerAt < poolAt) {
		t.Errorf("consumer must stop before pool closes; order %v", events)
	}
	if !capt.closed {
		t.Error("capture handle was not closed during drain")
	}
	if !pool.closed {
		t.Error("connection pool was not closed during drain")
	}
}
