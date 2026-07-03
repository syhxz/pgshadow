package replayer

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// --- Test fake implementing connSource / connHandle without a live DB --------

// fakeConn is a fake leased connection. It can be marked dead to exercise the
// dead-connection discard/replace path (R9.5).
type fakeConn struct {
	id        int
	mu        sync.Mutex
	alive     bool
	released  int
	pingCalls int
	onRelease func() // reclaims a capacity slot in the fake source
}

func (c *fakeConn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pingCalls++
	if !c.alive {
		return fmt.Errorf("conn %d is dead", c.id)
	}
	return nil
}

func (c *fakeConn) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.released++
	// A released connection returns to the fake pool's free slots; see
	// fakeSource.releaseSlot which is wired via the source wrapper.
	if c.onRelease != nil {
		c.onRelease()
	}
}

// kill marks the connection dead so the next Ping fails (R9.5).
func (c *fakeConn) kill() {
	c.mu.Lock()
	c.alive = false
	c.mu.Unlock()
}

// fakeSource is a fake connSource with a bounded number of concurrent
// connections. When all slots are in use, Acquire blocks until a slot frees or
// the context is done, modeling pgxpool exhaustion behavior (R9.6).
type fakeSource struct {
	mu       sync.Mutex
	maxConns int
	inUse    int
	nextID   int
	acquired int
	closed   bool
	free     chan struct{} // signals a slot freed
	failNext bool          // force the next Acquire to error
}

func newFakeSource(maxConns int) *fakeSource {
	return &fakeSource{
		maxConns: maxConns,
		free:     make(chan struct{}, maxConns),
	}
}

func (s *fakeSource) Acquire(ctx context.Context) (connHandle, error) {
	for {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil, errors.New("source closed")
		}
		if s.failNext {
			s.failNext = false
			s.mu.Unlock()
			return nil, errors.New("forced acquire failure")
		}
		if s.inUse < s.maxConns {
			s.inUse++
			s.acquired++
			s.nextID++
			c := &fakeConn{id: s.nextID, alive: true}
			c.onRelease = s.releaseSlot
			s.mu.Unlock()
			return c, nil
		}
		s.mu.Unlock()

		// Exhausted: wait for a freed slot or context cancellation (R9.6).
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.free:
		}
	}
}

func (s *fakeSource) releaseSlot() {
	s.mu.Lock()
	if s.inUse > 0 {
		s.inUse--
	}
	s.mu.Unlock()
	select {
	case s.free <- struct{}{}:
	default:
	}
}

func (s *fakeSource) Stat() PoolStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return PoolStats{
		Total:    int32(s.inUse),
		Acquired: int32(s.inUse),
		Idle:     int32(s.maxConns - s.inUse),
	}
}

func (s *fakeSource) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

func connID(port uint16) core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr("10.0.0.1"),
		SrcPort: port,
		DstIP:   netip.MustParseAddr("10.0.0.2"),
		DstPort: 5432,
	}
}

// --- Tests -------------------------------------------------------------------

// R7.5 / R7.10: the same ConnID maps to the same target connection while the
// lease is held.
func TestAffinity_SameConnID_SameConn(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, true)
	ctx := context.Background()
	id := connID(1000)

	h1, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	h2, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("expected same connection for same ConnID, got %p and %p", h1, h2)
	}
	if src.acquired != 1 {
		t.Fatalf("expected exactly 1 underlying acquire, got %d", src.acquired)
	}
}

// Distinct ConnIDs get distinct connections.
func TestAffinity_DifferentConnID_DifferentConn(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, true)
	ctx := context.Background()

	h1, err := p.acquireFor(ctx, connID(1))
	if err != nil {
		t.Fatalf("acquire a: %v", err)
	}
	h2, err := p.acquireFor(ctx, connID(2))
	if err != nil {
		t.Fatalf("acquire b: %v", err)
	}
	if h1 == h2 {
		t.Fatal("expected distinct connections for distinct ConnIDs")
	}
	if src.acquired != 2 {
		t.Fatalf("expected 2 underlying acquires, got %d", src.acquired)
	}
}

// R7.5: Release frees the affinity mapping, returning the connection to the
// pool; a subsequent acquire for the same ConnID gets a fresh connection.
func TestAffinity_ReleaseFreesAffinity(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, true)
	ctx := context.Background()
	id := connID(7)

	h1, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	c1 := h1.(*fakeConn)

	p.release(id)
	if c1.released != 1 {
		t.Fatalf("expected released conn after release, got released=%d", c1.released)
	}

	h2, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	if h1 == h2 {
		t.Fatal("expected a fresh connection after release, got the same one")
	}
	if src.acquired != 2 {
		t.Fatalf("expected 2 underlying acquires, got %d", src.acquired)
	}
}

// R9.5: a dead leased connection is discarded and replaced with a working one
// for the same ConnID.
func TestAffinity_DeadConnectionReplaced(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, true)
	ctx := context.Background()
	id := connID(42)

	h1, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	c1 := h1.(*fakeConn)
	c1.kill() // connection dies while leased

	h2, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("re-acquire after death: %v", err)
	}
	if h1 == h2 {
		t.Fatal("expected dead connection to be replaced")
	}
	if c1.released != 1 {
		t.Fatalf("expected dead connection to be released, got released=%d", c1.released)
	}
	if err := h2.Ping(ctx); err != nil {
		t.Fatalf("replacement connection should be alive: %v", err)
	}
	if src.acquired != 2 {
		t.Fatalf("expected 2 underlying acquires (original + replacement), got %d", src.acquired)
	}
}

// R9.6: when the pool is exhausted at max, an acquire with a deadline returns a
// context error rather than blocking forever; once a connection is released a
// new acquire succeeds.
func TestExhaustion_BlocksThenErrorsOnDeadline(t *testing.T) {
	src := newFakeSource(1) // only one connection available
	p := newAffinityPool(src, true)

	first, err := p.acquireFor(context.Background(), connID(1))
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = p.acquireFor(ctx, connID(2))
	if err == nil {
		t.Fatal("expected exhaustion to surface a context error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("expected acquire to block until deadline, returned after %v", elapsed)
	}

	// Free the only connection; the waiting ConnID can now acquire it.
	p.release(connID(1))
	_ = first
	got, err := p.acquireFor(context.Background(), connID(2))
	if err != nil {
		t.Fatalf("acquire after release should succeed: %v", err)
	}
	if got == nil {
		t.Fatal("expected a connection after release")
	}
}

// R9.6: exhaustion can also block-until-available. A connection released by
// another goroutine unblocks a waiting acquire.
func TestExhaustion_UnblocksOnRelease(t *testing.T) {
	src := newFakeSource(1)
	p := newAffinityPool(src, true)

	if _, err := p.acquireFor(context.Background(), connID(1)); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	done := make(chan connHandle, 1)
	go func() {
		h, err := p.acquireFor(context.Background(), connID(2))
		if err != nil {
			t.Errorf("blocked acquire failed: %v", err)
			done <- nil
			return
		}
		done <- h
	}()

	// Give the goroutine time to start blocking, then free the slot.
	time.Sleep(20 * time.Millisecond)
	p.release(connID(1))

	select {
	case h := <-done:
		if h == nil {
			t.Fatal("expected a connection once a slot freed")
		}
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after release")
	}
}

// Propagates a non-context acquisition error from the source.
func TestAcquire_SourceErrorPropagates(t *testing.T) {
	src := newFakeSource(10)
	src.failNext = true
	p := newAffinityPool(src, true)

	_, err := p.acquireFor(context.Background(), connID(1))
	if err == nil {
		t.Fatal("expected source acquire error to propagate")
	}
}

// Stats reflect the underlying source usage.
func TestStats_ReflectSource(t *testing.T) {
	src := newFakeSource(4)
	p := newAffinityPool(src, true)

	if _, err := p.acquireFor(context.Background(), connID(1)); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if _, err := p.acquireFor(context.Background(), connID(2)); err != nil {
		t.Fatalf("acquire: %v", err)
	}
	st := p.stats()
	if st.Acquired != 2 {
		t.Fatalf("expected 2 acquired, got %d", st.Acquired)
	}
	if st.Idle != 2 {
		t.Fatalf("expected 2 idle, got %d", st.Idle)
	}
}

// Close releases all outstanding leases and closes the source.
func TestClose_ReleasesOutstandingLeases(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, true)

	h, err := p.acquireFor(context.Background(), connID(1))
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	c := h.(*fakeConn)

	p.close()
	if c.released != 1 {
		t.Fatalf("expected outstanding lease released on close, got %d", c.released)
	}
	if !src.closed {
		t.Fatal("expected source to be closed")
	}
	if _, err := p.acquireFor(context.Background(), connID(2)); err == nil {
		t.Fatal("expected acquire after close to fail")
	}
}

// Without session affinity, each acquisition is independent (distinct conns).
func TestNoAffinity_IndependentConns(t *testing.T) {
	src := newFakeSource(10)
	p := newAffinityPool(src, false)
	ctx := context.Background()
	id := connID(99)

	h1, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	h2, err := p.acquireFor(ctx, id)
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if h1 == h2 {
		t.Fatal("expected independent connections when affinity is disabled")
	}
	if src.acquired != 2 {
		t.Fatalf("expected 2 underlying acquires, got %d", src.acquired)
	}
}
