// filequeue.go implements the on-disk file WAL Queue backend (task 7.2).
//
// fileQueue is an append-only write-ahead log of length-prefixed, gob-encoded
// SQL_Events with a read cursor. Records are appended at the write offset and
// consumed from the read cursor, so the log is globally FIFO, which preserves
// the relative ordering of SQL_Events that share the same ConnID (R6.6). It
// spills events to local disk so it has no external dependency (R6.8). The
// number of buffered (undequeued) records is bounded by the configured
// capacity; at capacity it applies the overflow policy (R6.2, R6.4) and
// increments the overflow counter on each discard (R6.5, R12.2).
package queue

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"pgshadow/pkg/core"
	"pgshadow/pkg/metrics"
)

// frameHeaderLen is the size of the big-endian uint32 length prefix that
// precedes every serialized record in the WAL.
const frameHeaderLen = 4

// maxFramePayload is the upper bound on a single record's payload size (64 MiB).
// Any frame whose length prefix exceeds this is treated as corrupt; this
// prevents an OOM allocation if the WAL file is damaged or tampered with.
const maxFramePayload = 64 << 20

// fileQueue is an append-only on-disk WAL with a read cursor. All fields are
// guarded by mu; cond signals waiting producers (Block policy) and consumers.
type fileQueue struct {
	mu   sync.Mutex
	cond *sync.Cond

	f        *os.File
	path     string
	capacity int
	overflow OverflowPolicy
	closed   bool

	writeOff int64 // byte offset where the next record is appended (end of WAL)
	readOff  int64 // byte offset of the next record to read (the cursor)
	count    int   // number of buffered (enqueued - dequeued) records

	releaseErr error // error from closing the WAL file, surfaced by Close

	// writeBuf is a reusable buffer for writeRecord to avoid allocating a new
	// slice for every Enqueue. It is only accessed under mu.
	writeBuf []byte

	// encodeBuf is a reusable buffer for the binary codec to avoid per-Enqueue
	// allocation of the serialized payload. Only accessed under mu.
	encodeBuf []byte

	// collector receives overflow notifications. It may be nil, or point to a
	// nil interface, in which case notifications are silently skipped (same
	// nil-safe pattern as the ring buffer).
	collector *metrics.Collector
}

// newFileQueue constructs a file WAL backend backed by a freshly created temp
// file. If dataDir is non-empty, the WAL is created in that directory;
// otherwise the OS default temp directory is used. capacity is assumed to be
// > 0 (validated by New).
func newFileQueue(capacity int, overflow OverflowPolicy, dataDir string, m *metrics.Collector) (Queue, error) {
	// Ensure the directory exists when a custom path is specified.
	if dataDir != "" {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("queue.New: file backend: create data_dir %q: %w", dataDir, err)
		}
	}
	f, err := os.CreateTemp(dataDir, "pgshadow-wal-*.log")
	if err != nil {
		return nil, fmt.Errorf("queue.New: file backend: create WAL: %w", err)
	}
	return newFileQueueWithFile(f, capacity, overflow, m), nil
}

// newFileQueueWithFile wraps an already-open file as a WAL backend. It is split
// out from newFileQueue so tests can supply a known path.
func newFileQueueWithFile(f *os.File, capacity int, overflow OverflowPolicy, m *metrics.Collector) *fileQueue {
	q := &fileQueue{
		f:         f,
		path:      f.Name(),
		capacity:  capacity,
		overflow:  overflow,
		collector: m,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// recordOverflow notifies the metrics collector of a discarded event, if a
// collector is configured. It must be called without holding mu.
func (q *fileQueue) recordOverflow() {
	if q.collector != nil && *q.collector != nil {
		(*q.collector).Overflow()
	}
}

// writeRecord appends a length-prefixed payload at the write offset and
// advances it. The header and payload are written in a single WriteAt call so
// that a partial write (e.g. due to disk-full) cannot leave a header without a
// payload, which would corrupt the read cursor. The caller must hold mu.
// The internal writeBuf is reused across calls to avoid per-enqueue allocation.
func (q *fileQueue) writeRecord(payload []byte) error {
	need := frameHeaderLen + len(payload)
	if cap(q.writeBuf) < need {
		q.writeBuf = make([]byte, need)
	} else {
		q.writeBuf = q.writeBuf[:need]
	}
	binary.BigEndian.PutUint32(q.writeBuf[:frameHeaderLen], uint32(len(payload)))
	copy(q.writeBuf[frameHeaderLen:], payload)
	if _, err := q.f.WriteAt(q.writeBuf, q.writeOff); err != nil {
		return err
	}
	q.writeOff += int64(need)
	return nil
}

// readLenAt reads the length prefix of the record stored at off. The caller
// must hold mu.
func (q *fileQueue) readLenAt(off int64) (uint32, error) {
	var hdr [frameHeaderLen]byte
	if _, err := q.f.ReadAt(hdr[:], off); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(hdr[:]), nil
}

// dropOldest advances the read cursor past the oldest buffered record without
// surfacing it to a consumer, freeing one capacity slot. The caller must hold
// mu and must ensure count > 0. Returns false if the WAL could not be read.
func (q *fileQueue) dropOldest() bool {
	n, err := q.readLenAt(q.readOff)
	if err != nil {
		return false
	}
	q.readOff += frameHeaderLen + int64(n)
	q.count--
	q.compactIfDrained()
	return true
}

// compactIfDrained resets the cursors and truncates the WAL once it is fully
// drained, so disk usage does not grow without bound during steady-state
// operation. Once the queue is also closed, it releases the backing file
// instead. The caller must hold mu.
func (q *fileQueue) compactIfDrained() {
	if q.count != 0 {
		return
	}
	if q.closed {
		q.releaseLocked()
		return
	}
	q.readOff = 0
	q.writeOff = 0
	// Best-effort truncate; a failure only means the file keeps its old size.
	_ = q.f.Truncate(0)
}

// releaseLocked closes the WAL file and removes the backing temp file. It is
// idempotent and the caller must hold mu. After release, f is nil so any
// further reads fail and Dequeue reports the queue as drained.
func (q *fileQueue) releaseLocked() {
	if q.f == nil {
		return
	}
	q.releaseErr = q.f.Close()
	if q.path != "" {
		_ = os.Remove(q.path)
	}
	q.f = nil
}

// Enqueue serializes ev and appends it to the WAL, applying the overflow policy
// when the buffered-record count is at capacity (R6.4). It returns dropped=true
// when an event was discarded due to overflow (R6.5).
func (q *fileQueue) Enqueue(ev *core.SQLEvent) (dropped bool) {
	q.mu.Lock()

	if q.closed {
		q.mu.Unlock()
		return false
	}

	// Encode under the lock so we can reuse q.encodeBuf without allocation.
	payload, err := encodeBinary(ev, q.encodeBuf)
	if err != nil {
		q.mu.Unlock()
		return true
	}
	q.encodeBuf = payload // retain the backing array for next call

	// Fast path: free capacity available.
	if q.count < q.capacity {
		if err := q.writeRecord(payload); err != nil {
			q.mu.Unlock()
			return true
		}
		q.count++
		q.cond.Broadcast()
		q.mu.Unlock()
		return false
	}

	// At capacity: apply the overflow policy.
	switch q.overflow {
	case DropNewest:
		// Discard the incoming event; the WAL is unchanged.
		q.mu.Unlock()
		q.recordOverflow()
		return true

	case Block:
		// Wait until a slot frees or the queue closes. No event is discarded,
		// so the overflow counter is not incremented.
		for q.count == q.capacity && !q.closed {
			q.cond.Wait()
		}
		if q.closed {
			q.mu.Unlock()
			return false
		}
		if err := q.writeRecord(payload); err != nil {
			q.mu.Unlock()
			return true
		}
		q.count++
		q.cond.Broadcast()
		q.mu.Unlock()
		return false

	default: // DropOldest (default)
		// Evict the oldest buffered record, then append the new one.
		if !q.dropOldest() {
			q.mu.Unlock()
			return true
		}
		if err := q.writeRecord(payload); err != nil {
			q.mu.Unlock()
			return true
		}
		q.count++
		q.cond.Broadcast()
		q.mu.Unlock()
		q.recordOverflow()
		return true
	}
}

// Dequeue blocks until a record is available, the context is cancelled, or the
// queue is closed and drained. It returns ok=false when the queue is closed and
// drained or when the context is cancelled.
func (q *fileQueue) Dequeue(ctx context.Context) (*core.SQLEvent, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count == 0 && !q.closed && ctx.Err() == nil {
		// Watcher wakes the waiter on context cancellation; torn down via stop
		// before Dequeue returns so it never leaks.
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-ctx.Done():
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			case <-stop:
			}
		}()

		for q.count == 0 && !q.closed && ctx.Err() == nil {
			q.cond.Wait()
		}
	}

	if q.count == 0 {
		// Closed and drained, or context cancelled.
		return nil, false
	}

	n, err := q.readLenAt(q.readOff)
	if err != nil {
		return nil, false
	}
	if n > maxFramePayload {
		// Corrupt or tampered frame — refuse to allocate an excessive buffer.
		return nil, false
	}
	payload := make([]byte, n)
	if _, err := q.f.ReadAt(payload, q.readOff+frameHeaderLen); err != nil {
		return nil, false
	}
	ev, err := decodeEvent(payload)
	if err != nil {
		return nil, false
	}

	q.readOff += frameHeaderLen + int64(n)
	q.count--
	q.compactIfDrained()
	// Wake any blocked enqueuers (Block policy) now that a slot is free.
	q.cond.Broadcast()
	return ev, true
}

// Depth returns the number of buffered (undequeued) records. R10.3
func (q *fileQueue) Depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// Close marks the queue closed and wakes all blocked producers and consumers.
// Buffered records remain drainable after Close (mirroring the ring buffer);
// the backing file is closed and the temp WAL removed once the queue is both
// closed and fully drained. If the queue is already empty, resources are
// released immediately.
func (q *fileQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return q.releaseErr
	}
	q.closed = true
	if q.count == 0 {
		q.releaseLocked()
	}
	q.cond.Broadcast()
	return q.releaseErr
}

// encodeEvent serializes a SQL_Event using the compact binary codec (see
// binary_codec.go). The reuse buffer allows the caller to amortize allocation
// across calls when possible.
func encodeEvent(ev *core.SQLEvent) ([]byte, error) {
	return encodeBinary(ev, nil)
}

// decodeEvent reverses encodeEvent using the compact binary codec.
func decodeEvent(payload []byte) (*core.SQLEvent, error) {
	return decodeBinary(payload)
}
