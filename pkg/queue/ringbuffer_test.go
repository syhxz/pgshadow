package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/metrics"
)

// fakeCollector is a test double implementing metrics.Collector. It only counts
// overflow notifications; all other methods are no-ops.
type fakeCollector struct {
	overflows int64
}

func (f *fakeCollector) PacketDrops(received, dropped uint64)               {}
func (f *fakeCollector) ParseError()                                        {}
func (f *fakeCollector) Classified(class filter.StmtClass, kept bool)       {}
func (f *fakeCollector) QueueDepth(depth int)                               {}
func (f *fakeCollector) Overflow()                                          { atomic.AddInt64(&f.overflows, 1) }
func (f *fakeCollector) ReplayLag(seconds float64)                          {}
func (f *fakeCollector) ReplayResult(err error)                             {}
func (f *fakeCollector) ExecTime(stmt string, source, target time.Duration) {}
func (f *fakeCollector) Throughput(sourceQPS, targetQPS float64)            {}
func (f *fakeCollector) Serve(port int) error                               { return nil }

func (f *fakeCollector) count() int64 { return atomic.LoadInt64(&f.overflows) }

// collectorPtr wraps a fakeCollector as the *metrics.Collector that New/newRingBuffer expect.
func collectorPtr(f *fakeCollector) *metrics.Collector {
	var c metrics.Collector = f
	return &c
}

func ev(seq uint64) *core.SQLEvent {
	return &core.SQLEvent{Seq: seq}
}

func connEv(srcPort uint16, seq uint64) *core.SQLEvent {
	return &core.SQLEvent{
		Conn: core.ConnID{SrcPort: srcPort},
		Seq:  seq,
	}
}

// --- Construction routing ---

func TestNewRoutesToRingBufferByDefault(t *testing.T) {
	for _, typ := range []string{"", "ringbuffer"} {
		q, err := New(Config{Type: typ, Capacity: 4}, nil)
		if err != nil {
			t.Fatalf("New(type=%q) error: %v", typ, err)
		}
		if q == nil {
			t.Fatalf("New(type=%q) returned nil queue", typ)
		}
		if _, ok := q.(*ringBuffer); !ok {
			t.Fatalf("New(type=%q) returned %T, want *ringBuffer", typ, q)
		}
	}
}

func TestNewDefaultsCapacity(t *testing.T) {
	q, err := New(Config{Type: "ringbuffer", Capacity: 0}, nil)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	rb := q.(*ringBuffer)
	if rb.capacity != DefaultCapacity {
		t.Fatalf("capacity = %d, want default %d", rb.capacity, DefaultCapacity)
	}
}

func TestNewUnimplementedBackends(t *testing.T) {
	// The file and kafka backends are implemented (tasks 7.2, 7.3). A kafka
	// queue with no brokers configured still fails fast at construction.
	if _, err := New(Config{Type: "kafka"}, nil); err == nil {
		t.Fatalf("New(type=kafka) with no brokers expected error, got nil")
	}
}

func TestNewUnknownType(t *testing.T) {
	if _, err := New(Config{Type: "bogus"}, nil); err == nil {
		t.Fatalf("New(type=bogus) expected error, got nil")
	}
}

// --- Basic FIFO behavior ---

func TestEnqueueDequeueFIFO(t *testing.T) {
	q, _ := New(Config{Capacity: 8}, nil)
	for i := uint64(0); i < 5; i++ {
		if dropped := q.Enqueue(ev(i)); dropped {
			t.Fatalf("enqueue %d unexpectedly dropped", i)
		}
	}
	if d := q.Depth(); d != 5 {
		t.Fatalf("Depth = %d, want 5", d)
	}
	for i := uint64(0); i < 5; i++ {
		got, ok := q.Dequeue(context.Background())
		if !ok {
			t.Fatalf("dequeue %d: ok=false", i)
		}
		if got.Seq != i {
			t.Fatalf("dequeue order: got Seq=%d, want %d", got.Seq, i)
		}
	}
	if d := q.Depth(); d != 0 {
		t.Fatalf("Depth after drain = %d, want 0", d)
	}
}

// --- Overflow: drop_oldest (default) ---

func TestOverflowDropOldest(t *testing.T) {
	fc := &fakeCollector{}
	q, _ := New(Config{Capacity: 3, Overflow: DropOldest}, collectorPtr(fc))

	for i := uint64(0); i < 3; i++ {
		q.Enqueue(ev(i)) // fills 0,1,2
	}
	// Enqueue 3 -> evicts 0; enqueue 4 -> evicts 1. Remaining: 2,3,4
	if !q.Enqueue(ev(3)) {
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if !q.Enqueue(ev(4)) {
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if d := q.Depth(); d != 3 {
		t.Fatalf("Depth = %d, want 3 (bounded)", d)
	}
	want := []uint64{2, 3, 4}
	for _, w := range want {
		got, ok := q.Dequeue(context.Background())
		if !ok || got.Seq != w {
			t.Fatalf("drop_oldest order: got (%v,%d), want Seq=%d", ok, got.Seq, w)
		}
	}
	if c := fc.count(); c != 2 {
		t.Fatalf("overflow count = %d, want 2", c)
	}
}

// --- Overflow: drop_newest ---

func TestOverflowDropNewest(t *testing.T) {
	fc := &fakeCollector{}
	q, _ := New(Config{Capacity: 3, Overflow: DropNewest}, collectorPtr(fc))

	for i := uint64(0); i < 3; i++ {
		q.Enqueue(ev(i)) // fills 0,1,2
	}
	// Incoming events are discarded; buffer stays 0,1,2.
	if !q.Enqueue(ev(99)) {
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if !q.Enqueue(ev(100)) {
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if d := q.Depth(); d != 3 {
		t.Fatalf("Depth = %d, want 3", d)
	}
	want := []uint64{0, 1, 2}
	for _, w := range want {
		got, ok := q.Dequeue(context.Background())
		if !ok || got.Seq != w {
			t.Fatalf("drop_newest order: got (%v,%d), want Seq=%d", ok, got.Seq, w)
		}
	}
	if c := fc.count(); c != 2 {
		t.Fatalf("overflow count = %d, want 2", c)
	}
}

// --- Overflow: block ---

func TestOverflowBlockUnblocksOnDequeue(t *testing.T) {
	fc := &fakeCollector{}
	q, _ := New(Config{Capacity: 2, Overflow: Block}, collectorPtr(fc))
	q.Enqueue(ev(0))
	q.Enqueue(ev(1)) // full

	done := make(chan bool, 1)
	go func() {
		dropped := q.Enqueue(ev(2)) // blocks until space
		done <- dropped
	}()

	// Should still be blocked.
	select {
	case <-done:
		t.Fatalf("Block enqueue returned while queue full")
	case <-time.After(50 * time.Millisecond):
	}

	// Make room.
	if got, ok := q.Dequeue(context.Background()); !ok || got.Seq != 0 {
		t.Fatalf("dequeue: got (%v,%d), want Seq=0", ok, got.Seq)
	}

	select {
	case dropped := <-done:
		if dropped {
			t.Fatalf("Block policy must not report dropped=true")
		}
	case <-time.After(time.Second):
		t.Fatalf("Block enqueue did not unblock after dequeue")
	}
	if c := fc.count(); c != 0 {
		t.Fatalf("Block policy must not increment overflow counter, got %d", c)
	}
}

// --- Dequeue blocking / cancellation / close ---

func TestDequeueBlocksUntilEnqueue(t *testing.T) {
	q, _ := New(Config{Capacity: 4}, nil)
	type res struct {
		ev *core.SQLEvent
		ok bool
	}
	ch := make(chan res, 1)
	go func() {
		e, ok := q.Dequeue(context.Background())
		ch <- res{e, ok}
	}()

	select {
	case <-ch:
		t.Fatalf("Dequeue returned on empty queue before enqueue")
	case <-time.After(50 * time.Millisecond):
	}

	q.Enqueue(ev(42))
	select {
	case r := <-ch:
		if !r.ok || r.ev.Seq != 42 {
			t.Fatalf("Dequeue got (%v,%d), want Seq=42", r.ok, r.ev.Seq)
		}
	case <-time.After(time.Second):
		t.Fatalf("Dequeue did not wake after enqueue")
	}
}

func TestDequeueContextCancel(t *testing.T) {
	q, _ := New(Config{Capacity: 4}, nil)
	ctx, cancel := context.WithCancel(context.Background())

	ch := make(chan bool, 1)
	go func() {
		_, ok := q.Dequeue(ctx)
		ch <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case ok := <-ch:
		if ok {
			t.Fatalf("Dequeue returned ok=true after context cancel")
		}
	case <-time.After(time.Second):
		t.Fatalf("Dequeue did not return after context cancel")
	}
}

func TestDequeueClosedAndDrained(t *testing.T) {
	q, _ := New(Config{Capacity: 4}, nil)
	q.Enqueue(ev(1))
	if err := q.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	// Remaining buffered events are still drainable after close.
	if got, ok := q.Dequeue(context.Background()); !ok || got.Seq != 1 {
		t.Fatalf("post-close drain: got (%v,%d), want Seq=1", ok, got.Seq)
	}
	// Now empty + closed -> ok=false.
	if _, ok := q.Dequeue(context.Background()); ok {
		t.Fatalf("Dequeue on closed+empty queue returned ok=true")
	}
	// Enqueue after close is a no-op (not dropped).
	if dropped := q.Enqueue(ev(2)); dropped {
		t.Fatalf("Enqueue after close should not report dropped")
	}
	if d := q.Depth(); d != 0 {
		t.Fatalf("Depth after close enqueue = %d, want 0", d)
	}
}

func TestCloseUnblocksDequeue(t *testing.T) {
	q, _ := New(Config{Capacity: 4}, nil)
	ch := make(chan bool, 1)
	go func() {
		_, ok := q.Dequeue(context.Background())
		ch <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	q.Close()
	select {
	case ok := <-ch:
		if ok {
			t.Fatalf("Dequeue returned ok=true after close")
		}
	case <-time.After(time.Second):
		t.Fatalf("Close did not unblock Dequeue")
	}
}

func TestCloseIdempotent(t *testing.T) {
	q, _ := New(Config{Capacity: 4}, nil)
	if err := q.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- Concurrency: preserve global FIFO (hence per-ConnID) ordering ---

func TestConcurrentPerConnOrdering(t *testing.T) {
	const conns = 4
	const perConn = 500
	// Capacity large enough to avoid overflow so every event is observed.
	q, _ := New(Config{Capacity: conns * perConn, Overflow: Block}, nil)

	var wg sync.WaitGroup
	// One producer per ConnID, each enqueuing in increasing Seq order.
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(port uint16) {
			defer wg.Done()
			for s := uint64(0); s < perConn; s++ {
				q.Enqueue(connEv(port, s))
			}
		}(uint16(1000 + c))
	}

	// Single consumer drains and checks per-ConnID monotonicity.
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		last := map[uint16]int64{}
		total := 0
		for total < conns*perConn {
			e, ok := q.Dequeue(context.Background())
			if !ok {
				t.Errorf("Dequeue returned ok=false before draining all events")
				return
			}
			port := e.Conn.SrcPort
			if prev, seen := last[port]; seen && int64(e.Seq) <= prev {
				t.Errorf("per-ConnID order violated for port %d: seq %d after %d", port, e.Seq, prev)
				return
			}
			last[port] = int64(e.Seq)
			total++
		}
	}()

	wg.Wait()
	select {
	case <-consumed:
	case <-time.After(10 * time.Second):
		t.Fatalf("consumer did not finish draining")
	}
}

// --- Concurrency: bounded + overflow accounting under contention ---

func TestConcurrentBoundedDepth(t *testing.T) {
	fc := &fakeCollector{}
	const capacity = 16
	q, _ := New(Config{Capacity: capacity, Overflow: DropOldest}, collectorPtr(fc))

	var wg sync.WaitGroup
	const producers = 8
	const each = 1000
	var dropped int64
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if q.Enqueue(ev(uint64(i))) {
					atomic.AddInt64(&dropped, 1)
				}
			}
		}()
	}
	wg.Wait()

	if d := q.Depth(); d > capacity {
		t.Fatalf("Depth = %d exceeds capacity %d", d, capacity)
	}
	// Every reported drop must have incremented the overflow counter exactly once.
	if got := fc.count(); got != atomic.LoadInt64(&dropped) {
		t.Fatalf("overflow counter = %d, but %d drops reported", got, dropped)
	}
}
