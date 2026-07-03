// Package replayer — unbounded lane channel to prevent dispatch deadlock.
//
// When multiple lanes compete for the same database rows, a bounded lane
// channel causes priority inversion: the dispatch goroutine blocks trying to
// send to a full lane whose worker is waiting on a lock held by another lane's
// uncommitted transaction — whose COMMIT is stuck behind the blocked dispatch.
//
// An unboundedLane decouples the dispatch loop from lane worker speed: Enqueue
// never blocks, allowing the dispatcher to always make progress and deliver
// COMMIT events that release cross-lane locks. Memory growth is bounded by the
// upstream backpressure mechanism (Kafka consumer pause) rather than by a
// fixed channel buffer.
package replayer

import (
	"sync"

	"pgshadow/pkg/core"
)

// unboundedLane is a FIFO queue with a non-blocking Enqueue and a blocking
// Dequeue. It replaces the fixed-size chan *core.SQLEvent used per lane to
// eliminate the dispatch deadlock described above.
type unboundedLane struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []*core.SQLEvent
	closed bool
}

// newUnboundedLane creates a new unbounded lane with an initial buffer hint.
func newUnboundedLane(hint int) *unboundedLane {
	l := &unboundedLane{
		buf: make([]*core.SQLEvent, 0, hint),
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Enqueue appends an event to the lane. It never blocks.
func (l *unboundedLane) Enqueue(ev *core.SQLEvent) {
	l.mu.Lock()
	l.buf = append(l.buf, ev)
	l.mu.Unlock()
	l.cond.Signal()
}

// Dequeue removes and returns the next event, blocking until one is available
// or the lane is closed. Returns (nil, false) when closed and drained.
func (l *unboundedLane) Dequeue() (*core.SQLEvent, bool) {
	l.mu.Lock()
	for len(l.buf) == 0 && !l.closed {
		l.cond.Wait()
	}
	if len(l.buf) == 0 && l.closed {
		l.mu.Unlock()
		return nil, false
	}
	ev := l.buf[0]
	l.buf[0] = nil // avoid retaining a reference to a potentially large event
	l.buf = l.buf[1:]

	// Shrink the underlying array when it's over-allocated relative to the
	// current length. After a burst (e.g. target was slow), the buffer may have
	// grown to 100K+ entries; once drained, the backing array would otherwise
	// never be freed. Re-allocate when len < cap/4 and cap > a minimum.
	if cap(l.buf) > 1024 && len(l.buf) < cap(l.buf)/4 {
		shrunk := make([]*core.SQLEvent, len(l.buf))
		copy(shrunk, l.buf)
		l.buf = shrunk
	}

	l.mu.Unlock()
	return ev, true
}

// Close signals that no more events will be enqueued. Workers blocked in
// Dequeue will unblock and drain remaining items, then receive (nil, false).
func (l *unboundedLane) Close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.cond.Broadcast()
}

// Len returns the current number of buffered events (for diagnostics).
func (l *unboundedLane) Len() int {
	l.mu.Lock()
	n := len(l.buf)
	l.mu.Unlock()
	return n
}
