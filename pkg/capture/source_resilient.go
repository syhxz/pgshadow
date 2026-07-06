// source_resilient.go wraps a Source with automatic reconnection on failure.
//
// When the underlying capture source encounters an error (e.g. NIC bond
// failover invalidates the pcap file descriptor), the resilient wrapper
// transparently reopens the capture interface with exponential backoff.
// This ensures capture survives transient network reconfiguration events
// without manual intervention (Issue #2).
package capture

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/gopacket"
)

// Reconnect backoff constants.
const (
	reconnectInitialDelay = 1 * time.Second
	reconnectMaxDelay     = 16 * time.Second
	reconnectMaxAttempts  = 0 // 0 = unlimited
)

// OpenFunc is the function used to open a capture source. It allows the
// resilient wrapper to reopen the source on failure using the same config.
type OpenFunc func(cfg Config) (Source, error)

// resilientSource wraps a Source with automatic reconnection on read failure.
type resilientSource struct {
	cfg    Config
	openFn OpenFunc

	mu      sync.Mutex
	inner   Source
	closed  bool
	packets chan gopacket.Packet

	// Accumulated stats across reconnections.
	totalReceived uint64
	totalDropped  uint64

	reconnects uint64
}

// NewResilientSource wraps the given openFn-produced Source with automatic
// reconnection. On failure, it attempts to reopen the source with exponential
// backoff. The returned Source's Packets() channel continues delivering packets
// seamlessly across reconnections.
func NewResilientSource(cfg Config, openFn OpenFunc) (Source, error) {
	inner, err := openFn(cfg)
	if err != nil {
		return nil, err
	}

	rs := &resilientSource{
		cfg:     cfg,
		openFn:  openFn,
		inner:   inner,
		packets: make(chan gopacket.Packet, 256),
	}

	go rs.pumpLoop()
	return rs, nil
}

// pumpLoop reads packets from the inner source and forwards them to the
// external channel. When the inner source's channel closes (indicating an
// error or EOF), it attempts to reconnect.
func (rs *resilientSource) pumpLoop() {
	defer close(rs.packets)

	for {
		rs.mu.Lock()
		if rs.closed {
			rs.mu.Unlock()
			return
		}
		inner := rs.inner
		rs.mu.Unlock()

		if inner == nil {
			// Should not happen, but guard against it.
			return
		}

		// Accumulate stats from current source before it potentially dies.
		rs.accumulateStats(inner)

		// Pump packets from the inner source. When Close() is called, it closes
		// the inner source which closes pktCh, breaking this loop.
		pktCh := inner.Packets()
		for pkt := range pktCh {
			rs.packets <- pkt
		}

		// Channel closed — the inner source has died or been closed.
		rs.mu.Lock()
		if rs.closed {
			rs.mu.Unlock()
			return
		}
		rs.mu.Unlock()

		// Accumulate final stats from dying source.
		rs.accumulateStats(inner)
		_ = inner.Close()

		// Attempt reconnection with exponential backoff.
		newInner := rs.reconnect()
		if newInner == nil {
			// Closed during reconnect.
			return
		}

		rs.mu.Lock()
		rs.inner = newInner
		rs.mu.Unlock()
	}
}

// accumulateStats captures stats from the inner source into the accumulated totals.
func (rs *resilientSource) accumulateStats(inner Source) {
	st, err := inner.Stats()
	if err == nil {
		atomic.StoreUint64(&rs.totalReceived, st.Received)
		atomic.StoreUint64(&rs.totalDropped, st.Dropped)
	}
}

// reconnect attempts to reopen the capture source with exponential backoff.
// Returns nil if the resilient source is closed during reconnection.
func (rs *resilientSource) reconnect() Source {
	delay := reconnectInitialDelay
	attempt := 0

	for {
		rs.mu.Lock()
		if rs.closed {
			rs.mu.Unlock()
			return nil
		}
		rs.mu.Unlock()

		attempt++
		atomic.AddUint64(&rs.reconnects, 1)
		fmt.Fprintf(os.Stderr, "pgshadow: WARN capture: source channel closed, attempting reconnect (attempt %d, delay %v)\n", attempt, delay)

		time.Sleep(delay)

		rs.mu.Lock()
		if rs.closed {
			rs.mu.Unlock()
			return nil
		}
		rs.mu.Unlock()

		inner, err := rs.openFn(rs.cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "pgshadow: WARN capture: reconnect attempt %d failed: %v\n", attempt, err)
			// Exponential backoff
			delay *= 2
			if delay > reconnectMaxDelay {
				delay = reconnectMaxDelay
			}
			if reconnectMaxAttempts > 0 && attempt >= reconnectMaxAttempts {
				fmt.Fprintf(os.Stderr, "pgshadow: ERROR capture: max reconnect attempts (%d) exhausted\n", reconnectMaxAttempts)
				return nil
			}
			continue
		}

		fmt.Fprintf(os.Stderr, "pgshadow: INFO capture: reconnected successfully after %d attempt(s)\n", attempt)
		return inner
	}
}

// Packets returns the channel of captured packets, seamlessly spanning reconnections.
func (rs *resilientSource) Packets() <-chan gopacket.Packet {
	return rs.packets
}

// Stats returns cumulative stats across all reconnections.
func (rs *resilientSource) Stats() (CaptureStats, error) {
	rs.mu.Lock()
	inner := rs.inner
	rs.mu.Unlock()

	// Try to get live stats from current inner source.
	if inner != nil {
		if st, err := inner.Stats(); err == nil {
			return CaptureStats{
				Received: st.Received,
				Dropped:  st.Dropped,
			}, nil
		}
	}

	// Fall back to accumulated stats.
	return CaptureStats{
		Received: atomic.LoadUint64(&rs.totalReceived),
		Dropped:  atomic.LoadUint64(&rs.totalDropped),
	}, nil
}

// Close shuts down the resilient source and its inner source.
func (rs *resilientSource) Close() error {
	rs.mu.Lock()
	if rs.closed {
		rs.mu.Unlock()
		return nil
	}
	rs.closed = true
	inner := rs.inner
	rs.inner = nil
	rs.mu.Unlock()

	if inner != nil {
		return inner.Close()
	}
	return nil
}

// Reconnects returns the number of reconnection attempts made (for metrics/testing).
func (rs *resilientSource) Reconnects() uint64 {
	return atomic.LoadUint64(&rs.reconnects)
}
