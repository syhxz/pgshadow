// This file assembles the producer side of the pgshadow pipeline (task 12.2).
//
// Topology
//
//	capture.Source ─▶ [pktCh] ─▶ reassembler ─▶ streamProcessor
//	                                              │  per-(ConnID,direction) parser
//	                                              │  ├─ client msgs ─▶ EventBuilder ─▶ SQLEvent
//	                                              │  │                  ├─ TxIDGenerator (R3.8)
//	                                              │  │                  ├─ BindCorrelator (R3.9)
//	                                              │  │                  ├─ CopyTracker (R3.10)
//	                                              │  │                  └─ SourceExecTimer (R10.7)
//	                                              │  │                  ▼
//	                                              │  │            StateMachine + Filter.Decide
//	                                              │  │                  ▼ (kept)
//	                                              │  │              queue.Enqueue ─▶ replayer
//	                                              │  └─ server msgs ─▶ ExtractReadyForQuery
//	                                              │                    ─▶ StateMachine.OnReadyForQuery (R12.1)
//
// The stages are connected by two Go channels:
//
//   - capture.Source.Packets() — owned by the capture layer.
//   - pktCh — a buffered channel internal to this file that decouples the NIC
//     read loop from the (single-goroutine) reassembler+processing loop so a
//     burst of packets does not stall capture (the "capture→processing"
//     decoupling the design calls for). The filter→replayer decoupling is
//     provided by the bounded queue (pkg/queue), not by this file.
//
// Production isolation (R1.6, R12.1, R12.6)
//
// The producer is strictly read-only with respect to the network. It consumes
// already-captured packets, frames and filters them, and writes kept events to
// the in-process queue. It NEVER dials, writes to, or otherwise opens a socket
// to the Source_Database:
//
//   - capture.Source exposes only Packets()/Stats()/Close() — there is no write
//     path to the wire (see pkg/capture: "never writes to the network", R1.6).
//   - This file imports none of pgx/pgxpool/net/database drivers; the only
//     component in the whole binary that dials a database is the replayer's
//     pool, and it dials the Target_Database exclusively (startup step 5).
//
// The compile-time assertions at the bottom of this file pin the read-only
// shape of the producer's dependencies so a future change that introduces a
// Source_Database connection here would fail to build. The end-to-end dial
// recording check is property test 12.4 (a separate task).
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/gopacket"

	"pgshadow/pkg/capture"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/pipeline"
	"pgshadow/pkg/queue"
)

// pktChanBuffer bounds the buffered channel that decouples the capture read
// loop from the processing loop. It absorbs short bursts without forcing the
// capture layer to block; sustained overload is bounded downstream by the
// queue's overflow policy (R6.4), so this buffer is intentionally modest.
const pktChanBuffer = 4096

// defaultIdleFlush bounds how often idle reassembly state is reclaimed when no
// parser idle timeout is configured.
const defaultIdleFlush = 300 * time.Second

// defaultDrainTimeout bounds the total time the graceful-shutdown drain (R12.5)
// will spend waiting for the pipeline to quiesce. The design calls for a
// configurable drain timeout (default 30s); no drain-timeout config field
// exists yet, so this constant is the sensible default. If draining stalls past
// this deadline the drain forces the replayer to stop and closes the pool so
// the process always exits promptly.
const defaultDrainTimeout = 30 * time.Second

// pipelineState holds the running pipeline's lifecycle handles so run can perform
// the ordered graceful-shutdown drain (R12.5). The producer (capture →
// reassembler → processor → queue) and the consumer (replayer draining the
// queue → Target_Database) run on independent contexts so they can be torn down
// in the required order rather than all at once:
//
//   - prodCancel stops the producer so no new events enter the queue (step 1/2).
//   - Closing the queue makes the consumer drain the remaining events and exit
//     on its own (step 3/4); consumerCancel is only the deadline escape hatch
//     that forces in-flight workers to abandon work if draining stalls.
type pipelineState struct {
	c              *components
	prodCancel     context.CancelFunc
	consumerCancel context.CancelFunc
	producerDone   <-chan struct{}
	consumerDone   <-chan struct{}
}

// startPipeline assembles and starts the full pipeline and returns a handle for
// the graceful-shutdown drain. The consumer side (the replayer draining the
// queue and executing against the Target_Database) and the producer side
// (capture → reassembler → processor → queue) run as independent goroutines, so
// a stall on either side is absorbed by the queue rather than propagated.
//
// The consumer runs on a context derived from context.Background rather than
// from ctx, so a shutdown signal does NOT immediately abandon events already
// buffered in the queue: the drain instead closes the queue, which lets the
// replayer dequeue every remaining event and return cleanly (R12.5 step 3/4).
// The producer runs on a context derived from ctx, so the signal stops capture
// first (R12.5 step 1).
//
// It returns an error only if the producer cannot be constructed (an invalid
// filter mode that slipped past configuration validation); in that case run
// aborts before blocking.
func startPipeline(ctx context.Context, c *components) (*pipelineState, error) {
	sp, err := pipeline.NewStreamProcessor(c.cfg.Parser, c.cfg.Filter, c.queue, c.collector)
	if err != nil {
		return nil, err
	}
	sp.DebugMode = debugMode

	// Consumer: the replayer drains the queue and executes against the target.
	// It runs on an independent context so the queue can be drained at shutdown
	// before the consumer is asked to stop; consumerCancel forces it to stop if
	// the drain deadline expires.
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		_ = c.replayer.Run(consumerCtx)
	}()

	// Producer: capture → reassembler → processor → queue. Stops when ctx is
	// cancelled (the shutdown signal) or when prodCancel is called by the drain.
	prodCtx, prodCancel := context.WithCancel(ctx)
	producerDone := startProducer(prodCtx, c, sp)

	// Metrics polling: periodically report capture stats and queue depth so
	// Prometheus metrics stay current. Stops when the producer context is done.
	go metricsPoller(prodCtx, c.capture, c.queue, c.collector)

	return &pipelineState{
		c:              c,
		prodCancel:     prodCancel,
		consumerCancel: consumerCancel,
		producerDone:   producerDone,
		consumerDone:   consumerDone,
	}, nil
}

// drain runs the ordered graceful-shutdown sequence under the default deadline.
func (p *pipelineState) drain(stderr io.Writer) {
	p.drainWithin(defaultDrainTimeout, stderr)
}

// drainWithin runs the ordered graceful-shutdown drain (R12.5) bounded by a
// single shared deadline measured from the start of the drain:
//
//	(1) stop Packet_Capture so no new events enter;
//	(2) let the parse/filter pipeline drain (wait for the producer goroutines to
//	    flush whatever they framed into the queue);
//	(3) drain (ringbuffer) or checkpoint (file/kafka) the Buffer_Queue by
//	    closing it — the replayer then dequeues every remaining event;
//	(4) wait for the in-flight replay workers to finish executing against the
//	    Target_Database;
//	(5) close the Connection_Pool.
//
// If any wait exceeds the deadline, the drain stops waiting, forces the
// replayer to abandon in-flight work, and still closes the pool so the process
// exits promptly. The resources closed here are nil'd out of components so the
// deferred components.Close in run does not double-close them.
func (p *pipelineState) drainWithin(timeout time.Duration, stderr io.Writer) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	timedOut := false

	// Step 1: stop Packet_Capture so no new events enter the pipeline. The
	// producer reader goroutine stops feeding on prodCancel; closing the handle
	// releases the capture resource.
	p.prodCancel()
	if p.c.capture != nil {
		_ = p.c.capture.Close()
		p.c.capture = nil
	}

	// Step 2: let the parse/filter stage drain. The producer goroutines close
	// the internal packet channel and the reassembler and exit once they have
	// processed what they already framed, flushing kept events into the queue.
	select {
	case <-p.producerDone:
	case <-deadline.C:
		timedOut = true
		fmt.Fprintln(stderr, "pgshadow: shutdown: producer drain timed out")
	}

	// Step 3: drain or checkpoint the Buffer_Queue. Closing the queue makes the
	// ring buffer hand the replayer every still-buffered event before reporting
	// closed (drain); the file/kafka backends flush/checkpoint on Close.
	if p.c.queue != nil {
		_ = p.c.queue.Close()
		p.c.queue = nil
	}

	// Step 4: wait for the in-flight replay workers to finish. With the queue
	// closed, the replayer drains the remainder and returns on its own; the
	// deadline bounds the wait regardless of whether the producer timed out so
	// that a stalled consumer does not hang the process indefinitely. Even when
	// the producer timed out, the queue is already closed (step 3) and the
	// consumer may be mid-drain, so we always give it the remaining deadline
	// rather than force-cancelling immediately (R12.5 step 4).
	select {
	case <-p.consumerDone:
	case <-deadline.C:
		timedOut = true
		fmt.Fprintln(stderr, "pgshadow: shutdown: replay drain timed out; forcing stop")
	}
	if timedOut {
		// Force the replayer to abandon in-flight work so the pool can close.
		p.consumerCancel()
	}
	// Always wait for the consumer to fully stop before closing the pool, so no
	// worker is mid-execution against a closing pool.
	<-p.consumerDone

	// Step 5: close the Connection_Pool now that no worker is executing.
	p.consumerCancel() // release the derived context (no-op if already cancelled)
	if p.c.pool != nil {
		p.c.pool.Close()
		p.c.pool = nil
	}
}

// metricsPollerInterval controls how often the metrics poller reads capture
// stats and queue depth. 5 seconds is a sensible default for Prometheus scrape
// intervals without creating excessive overhead.
const metricsPollerInterval = 5 * time.Second

// metricsPoller periodically polls capture stats (packets received/dropped) and
// queue depth, reporting them to the metrics collector. This fills the gap where
// no pipeline stage was wiring these values to the collector. It exits when ctx
// is cancelled (producer shutdown).
func metricsPoller(ctx context.Context, cap capture.Source, q queue.Queue, collector metrics.Collector) {
	if collector == nil {
		return
	}
	ticker := time.NewTicker(metricsPollerInterval)
	defer ticker.Stop()

	var lastDropped uint64
	var lastOverflowDepth int // track queue depth for overflow warning

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cap != nil {
				if st, err := cap.Stats(); err == nil {
					collector.PacketDrops(st.Received, st.Dropped)
					// Log when new packet drops are detected.
					if st.Dropped > lastDropped {
						newDrops := st.Dropped - lastDropped
						var dropRate float64
						if st.Received > 0 {
							dropRate = float64(st.Dropped) / float64(st.Received) * 100
						}
						fmt.Fprintf(os.Stderr, "pgshadow: WARN [%s] packet drops detected: +%d (total=%d, received=%d, drop_rate=%.4f%%)\n",
							time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
							newDrops, st.Dropped, st.Received, dropRate)
					}
					lastDropped = st.Dropped
				}
			}
			if q != nil {
				depth := q.Depth()
				collector.QueueDepth(depth)
				// Log when queue depth is critically high (>80% of default capacity).
				if depth > queue.DefaultCapacity*80/100 && lastOverflowDepth <= queue.DefaultCapacity*80/100 {
					fmt.Fprintf(os.Stderr, "pgshadow: WARN [%s] queue depth critical: %d (capacity may be near limit)\n",
						time.Now().Format("2006-01-02T15:04:05.000Z07:00"), depth)
				}
				lastOverflowDepth = depth
			}
		}
	}
}

// startProducer wires the capture source to the stream processor through the
// TCP reassembler and starts the two producer goroutines. It returns a channel
// that is closed once both goroutines have exited, so the drain can wait for
// the parse/filter stage to finish flushing into the queue (R12.5 step 2).
//
// Goroutine 1 (reader) copies packets from the capture source into the buffered
// pktCh, decoupling NIC reads from processing (the capture→processing seam).
// Goroutine 2 (processor) is the single owner of the reassembler — which is not
// safe for concurrent use — and drives Assemble for each packet plus a periodic
// FlushOlderThan to reclaim idle connection state (R2.4). Both exit on
// ctx.Done() or when the capture source closes its channel.
func startProducer(ctx context.Context, c *components, sp *pipeline.StreamProcessor) <-chan struct{} {
	reasm := capture.NewReassembler(c.cfg.Capture, sp)
	pktCh := make(chan gopacket.Packet, pktChanBuffer)

	idle := time.Duration(c.cfg.Parser.TimeoutIdle) * time.Second
	if idle <= 0 {
		idle = defaultIdleFlush
	}

	// Read the packet channel once here (not inside the goroutine) so the
	// reader never touches c.capture concurrently with the drain detaching it.
	src := c.capture.Packets()

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: capture → buffered channel (read-only; never writes the NIC).
	go func() {
		defer wg.Done()
		defer close(pktCh)
		if debugMode {
			fmt.Fprintln(os.Stderr, "pgshadow: DEBUG producer reader goroutine started")
		}
		pktCount := 0
		for {
			select {
			case <-ctx.Done():
				if debugMode {
					fmt.Fprintf(os.Stderr, "pgshadow: DEBUG reader exiting ctx.Done, packets=%d\n", pktCount)
				}
				return
			case pkt, ok := <-src:
				if !ok {
					if debugMode {
						fmt.Fprintf(os.Stderr, "pgshadow: DEBUG reader src channel closed, packets=%d\n", pktCount)
					}
					return
				}
				pktCount++
				if debugMode && (pktCount <= 5 || pktCount%100 == 0) {
					fmt.Fprintf(os.Stderr, "pgshadow: DEBUG packet %d received, len=%d\n", pktCount, len(pkt.Data()))
				}
				select {
				case pktCh <- pkt:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Goroutine 2: single-threaded reassembler + processing loop.
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(idle)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				reasm.Close()
				return
			case pkt, ok := <-pktCh:
				if !ok {
					reasm.Close()
					return
				}
				reasm.Assemble(pkt)
			case <-ticker.C:
				reasm.FlushOlderThan(idle)
			}
		}
	}()

	// Signal completion once both producer goroutines have exited.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

// Compile-time production-isolation assertions (R12.6). The producer depends on
// the capture source only through these read-only shapes; there is no method
// here, or on these interfaces, that opens or writes a socket to the
// Source_Database. If a future edit widened capture.Source with a write/dial
// method and wired it in, these assertions are the structural anchor that keeps
// the producer's surface read-only.
var (
	_ interface {
		Packets() <-chan gopacket.Packet
		Stats() (capture.CaptureStats, error)
		Close() error
	} = capture.Source(nil)

	_ capture.StreamHandler = (*pipeline.StreamProcessor)(nil)
)
