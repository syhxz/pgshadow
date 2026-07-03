// ringbuffer.go implements the default in-memory ring-buffer Queue backend.
//
// It is a fixed-capacity circular buffer guarded by a mutex and a condition
// variable. The buffer is globally FIFO, which preserves the relative ordering
// of SQL_Events that share the same ConnID (R6.6). At capacity it applies the
// configured overflow policy (R6.4) and increments the overflow counter on each
// discard (R6.5, R12.2). It has no external dependency (R6.8).
package queue

import (
	"context"
	"sync"

	"pgshadow/pkg/core"
	"pgshadow/pkg/metrics"
)

// ringBuffer is a fixed-capacity circular buffer of *core.SQLEvent guarded by a
// mutex + condition variable. head is the index of the oldest event, tail is
// the next write slot, and count is the number of buffered events.
type ringBuffer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	buf      []*core.SQLEvent
	capacity int
	head     int
	tail     int
	count    int
	closed   bool
	overflow OverflowPolicy

	// collector receives overflow notifications. It is a pointer to the
	// Collector interface (per the design's New signature) and may be nil, or
	// point to a nil interface, in which case overflow notifications are
	// silently skipped so the queue can run without a real collector.
	collector *metrics.Collector
}

// newRingBuffer constructs an in-memory ring buffer with the given capacity and
// overflow policy. capacity is assumed to be > 0 (validated by New).
func newRingBuffer(capacity int, overflow OverflowPolicy, m *metrics.Collector) *ringBuffer {
	r := &ringBuffer{
		buf:       make([]*core.SQLEvent, capacity),
		capacity:  capacity,
		overflow:  overflow,
		collector: m,
	}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// push writes ev at the tail and advances the tail. The caller must hold mu and
// must ensure there is free space (count < capacity).
func (r *ringBuffer) push(ev *core.SQLEvent) {
	r.buf[r.tail] = ev
	r.tail = (r.tail + 1) % r.capacity
	r.count++
}

// recordOverflow notifies the metrics collector of a discarded event, if a
// collector is configured. It must be called without holding mu.
func (r *ringBuffer) recordOverflow() {
	if r.collector != nil && *r.collector != nil {
		(*r.collector).Overflow()
	}
}

// Enqueue adds an event, applying the overflow policy when at capacity (R6.4).
// It returns dropped=true when an event was discarded due to overflow (R6.5).
func (r *ringBuffer) Enqueue(ev *core.SQLEvent) (dropped bool) {
	r.mu.Lock()

	if r.closed {
		r.mu.Unlock()
		return false
	}

	// Fast path: free space available.
	if r.count < r.capacity {
		r.push(ev)
		r.cond.Signal() // wake one blocked consumer
		r.mu.Unlock()
		return false
	}

	// At capacity: apply the overflow policy.
	switch r.overflow {
	case DropNewest:
		// Discard the incoming event; the buffer is unchanged.
		r.mu.Unlock()
		r.recordOverflow()
		return true

	case Block:
		// Wait until space is available or the queue is closed. No event is
		// discarded, so the overflow counter is not incremented.
		for r.count == r.capacity && !r.closed {
			r.cond.Wait()
		}
		if r.closed {
			r.mu.Unlock()
			return false
		}
		r.push(ev)
		r.cond.Signal() // wake one blocked consumer
		r.mu.Unlock()
		return false

	default: // DropOldest (default)
		// Evict the oldest event to make room, then append the new one. When
		// the buffer is full, tail == head, so push overwrites the evicted slot.
		r.head = (r.head + 1) % r.capacity
		r.count--
		r.push(ev)
		r.cond.Signal() // wake one blocked consumer
		r.mu.Unlock()
		r.recordOverflow()
		return true
	}
}

// Dequeue blocks until an event is available, the context is cancelled, or the
// queue is closed. It returns ok=false when the queue is closed and drained or
// when the context is cancelled.
func (r *ringBuffer) Dequeue(ctx context.Context) (*core.SQLEvent, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count == 0 && !r.closed && ctx.Err() == nil {
		// Set up a context watcher that wakes the waiter on cancellation. It is
		// torn down via stop before Dequeue returns, so it never leaks.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				r.mu.Lock()
				r.cond.Broadcast()
				r.mu.Unlock()
			case <-stop:
			}
		}()

		for r.count == 0 && !r.closed && ctx.Err() == nil {
			r.cond.Wait()
		}
	}

	if r.count == 0 {
		// Closed and drained, or context cancelled.
		return nil, false
	}

	ev := r.buf[r.head]
	r.buf[r.head] = nil // release the reference for GC
	r.head = (r.head + 1) % r.capacity
	r.count--
	// Wake one blocked enqueuer (Block policy) now that space is free.
	r.cond.Signal()
	return ev, true
}

// Depth returns the number of buffered events. R10.3
func (r *ringBuffer) Depth() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Close marks the queue closed and wakes all blocked producers and consumers.
func (r *ringBuffer) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.cond.Broadcast()
	return nil
}
