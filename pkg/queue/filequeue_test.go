package queue

import (
	"context"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// newFileQ builds a file-backed queue for tests and registers cleanup so the
// temp WAL is removed afterwards.
func newFileQ(t *testing.T, cfg Config) Queue {
	t.Helper()
	cfg.Type = "file"
	q, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New(type=file) error: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// --- Construction routing ---

func TestNewRoutesToFileQueue(t *testing.T) {
	q, err := New(Config{Type: "file", Capacity: 4}, nil)
	if err != nil {
		t.Fatalf("New(type=file) error: %v", err)
	}
	defer q.Close()
	if _, ok := q.(*fileQueue); !ok {
		t.Fatalf("New(type=file) returned %T, want *fileQueue", q)
	}
}

func TestFileQueueDefaultsCapacity(t *testing.T) {
	q, err := New(Config{Type: "file", Capacity: 0}, nil)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer q.Close()
	fq := q.(*fileQueue)
	if fq.capacity != DefaultCapacity {
		t.Fatalf("capacity = %d, want default %d", fq.capacity, DefaultCapacity)
	}
}

// --- Basic FIFO behavior ---

func TestFileQueueFIFO(t *testing.T) {
	q := newFileQ(t, Config{Capacity: 8})
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

// --- Persistence round-trip: every SQLEvent field survives serialization ---

func TestFileQueuePersistenceRoundTrip(t *testing.T) {
	q := newFileQ(t, Config{Capacity: 4})

	want := &core.SQLEvent{
		Conn: core.ConnID{
			SrcIP:   netip.MustParseAddr("10.1.2.3"),
			SrcPort: 54321,
			DstIP:   netip.MustParseAddr("192.168.0.9"),
			DstPort: 5432,
		},
		SQL:            "INSERT INTO t(a,b) VALUES ($1,$2)",
		Timestamp:      time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC),
		TxID:           987,
		Seq:            42,
		Extended:       true,
		StmtName:       "stmt1",
		Params:         []core.ParamInfo{{OID: 23, Value: []byte("7"), Format: 0}, {OID: 25, Value: []byte("hello"), Format: 1}},
		CopyData:       [][]byte{[]byte("row1\n"), []byte("row2\n")},
		SourceExecTime: 1500 * time.Microsecond,
	}

	if dropped := q.Enqueue(want); dropped {
		t.Fatalf("enqueue unexpectedly dropped")
	}
	got, ok := q.Dequeue(context.Background())
	if !ok {
		t.Fatalf("dequeue: ok=false")
	}

	// time.Time must compare by instant, not struct equality (monotonic/loc).
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("Timestamp = %v, want %v", got.Timestamp, want.Timestamp)
	}
	got.Timestamp = want.Timestamp // normalize before deep compare
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-trip mismatch:\n got = %+v\nwant = %+v", got, want)
	}
}

// --- Overflow: drop_oldest (default) bounds buffered records + accounts drops ---

func TestFileQueueOverflowDropOldest(t *testing.T) {
	fc := &fakeCollector{}
	q, err := New(Config{Type: "file", Capacity: 3, Overflow: DropOldest}, collectorPtr(fc))
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer q.Close()

	for i := uint64(0); i < 3; i++ {
		q.Enqueue(ev(i)) // fills 0,1,2
	}
	if !q.Enqueue(ev(3)) { // evicts 0
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if !q.Enqueue(ev(4)) { // evicts 1
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if d := q.Depth(); d != 3 {
		t.Fatalf("Depth = %d, want 3 (bounded)", d)
	}
	for _, w := range []uint64{2, 3, 4} {
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

func TestFileQueueOverflowDropNewest(t *testing.T) {
	fc := &fakeCollector{}
	q, err := New(Config{Type: "file", Capacity: 3, Overflow: DropNewest}, collectorPtr(fc))
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer q.Close()

	for i := uint64(0); i < 3; i++ {
		q.Enqueue(ev(i)) // fills 0,1,2
	}
	if !q.Enqueue(ev(99)) {
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if !q.Enqueue(ev(100)) {
		t.Fatalf("enqueue at capacity should report dropped=true")
	}
	if d := q.Depth(); d != 3 {
		t.Fatalf("Depth = %d, want 3", d)
	}
	for _, w := range []uint64{0, 1, 2} {
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

func TestFileQueueOverflowBlockUnblocksOnDequeue(t *testing.T) {
	fc := &fakeCollector{}
	q, err := New(Config{Type: "file", Capacity: 2, Overflow: Block}, collectorPtr(fc))
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	defer q.Close()
	q.Enqueue(ev(0))
	q.Enqueue(ev(1)) // full

	done := make(chan bool, 1)
	go func() { done <- q.Enqueue(ev(2)) }()

	select {
	case <-done:
		t.Fatalf("Block enqueue returned while queue full")
	case <-time.After(50 * time.Millisecond):
	}

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

func TestFileQueueDequeueBlocksUntilEnqueue(t *testing.T) {
	q := newFileQ(t, Config{Capacity: 4})
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

func TestFileQueueDequeueContextCancel(t *testing.T) {
	q := newFileQ(t, Config{Capacity: 4})
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

func TestFileQueueClosedAndDrained(t *testing.T) {
	q, err := New(Config{Type: "file", Capacity: 4}, nil)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	q.Enqueue(ev(1))
	if err := q.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	// Buffered records remain drainable after close.
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

func TestFileQueueCloseUnblocksDequeue(t *testing.T) {
	q, err := New(Config{Type: "file", Capacity: 4}, nil)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
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

func TestFileQueueCloseIdempotent(t *testing.T) {
	q, err := New(Config{Type: "file", Capacity: 4}, nil)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// --- Globally FIFO preserves per-ConnID ordering under interleaving ---

func TestFileQueuePerConnOrdering(t *testing.T) {
	const conns = 4
	const perConn = 200
	q := newFileQ(t, Config{Capacity: conns * perConn, Overflow: Block})

	var wg sync.WaitGroup
	for c := 0; c < conns; c++ {
		wg.Add(1)
		go func(port uint16) {
			defer wg.Done()
			for s := uint64(0); s < perConn; s++ {
				q.Enqueue(connEv(port, s))
			}
		}(uint16(1000 + c))
	}

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
