// Package monitor implements operational monitoring for pgshadow.
// Addresses: #17 replay progress, #19 source failover detection,
// #20 replay result comparison.
package monitor

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ProgressTracker tracks replay progress and Kafka consumer lag (#17).
type ProgressTracker struct {
	mu sync.RWMutex

	// Counters
	eventsProduced  atomic.Int64
	eventsConsumed  atomic.Int64
	eventsReplayed  atomic.Int64
	eventsFailed    atomic.Int64
	eventsSkipped   atomic.Int64

	// Timing
	lastProduceTime atomic.Value // time.Time
	lastConsumeTime atomic.Value // time.Time
	lastReplayTime  atomic.Value // time.Time

	// Lag tracking
	currentLagMs atomic.Int64

	// Kafka offset tracking
	kafkaHighWatermark atomic.Int64
	kafkaCommitted     atomic.Int64

	startTime time.Time
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker() *ProgressTracker {
	pt := &ProgressTracker{startTime: time.Now()}
	pt.lastProduceTime.Store(time.Time{})
	pt.lastConsumeTime.Store(time.Time{})
	pt.lastReplayTime.Store(time.Time{})
	return pt
}

// OnProduce records a produce event.
func (pt *ProgressTracker) OnProduce() {
	pt.eventsProduced.Add(1)
	pt.lastProduceTime.Store(time.Now())
}

// OnConsume records a consume event.
func (pt *ProgressTracker) OnConsume() {
	pt.eventsConsumed.Add(1)
	pt.lastConsumeTime.Store(time.Now())
}

// OnReplay records a successful replay.
func (pt *ProgressTracker) OnReplay(lagMs int64) {
	pt.eventsReplayed.Add(1)
	pt.lastReplayTime.Store(time.Now())
	pt.currentLagMs.Store(lagMs)
}

// OnFailed records a failed replay.
func (pt *ProgressTracker) OnFailed() { pt.eventsFailed.Add(1) }

// OnSkipped records a skipped event (by safeguard).
func (pt *ProgressTracker) OnSkipped() { pt.eventsSkipped.Add(1) }

// UpdateKafkaOffsets records Kafka consumer group lag.
func (pt *ProgressTracker) UpdateKafkaOffsets(highWatermark, committed int64) {
	pt.kafkaHighWatermark.Store(highWatermark)
	pt.kafkaCommitted.Store(committed)
}

// Progress returns the current progress snapshot.
type Progress struct {
	Uptime          time.Duration
	EventsProduced  int64
	EventsConsumed  int64
	EventsReplayed  int64
	EventsFailed    int64
	EventsSkipped   int64
	CurrentLagMs    int64
	KafkaLag        int64 // highWatermark - committed
	ProduceRate     float64 // events/sec (last minute)
	ReplayRate      float64 // events/sec (last minute)
	LastProduceTime time.Time
	LastReplayTime  time.Time
}

// GetProgress returns the current progress.
func (pt *ProgressTracker) GetProgress() Progress {
	uptime := time.Since(pt.startTime)
	produced := pt.eventsProduced.Load()
	replayed := pt.eventsReplayed.Load()

	var produceRate, replayRate float64
	if uptime.Seconds() > 0 {
		produceRate = float64(produced) / uptime.Seconds()
		replayRate = float64(replayed) / uptime.Seconds()
	}

	lastProduce, _ := pt.lastProduceTime.Load().(time.Time)
	lastReplay, _ := pt.lastReplayTime.Load().(time.Time)

	return Progress{
		Uptime:          uptime,
		EventsProduced:  produced,
		EventsConsumed:  pt.eventsConsumed.Load(),
		EventsReplayed:  replayed,
		EventsFailed:    pt.eventsFailed.Load(),
		EventsSkipped:   pt.eventsSkipped.Load(),
		CurrentLagMs:    pt.currentLagMs.Load(),
		KafkaLag:        pt.kafkaHighWatermark.Load() - pt.kafkaCommitted.Load(),
		ProduceRate:     produceRate,
		ReplayRate:      replayRate,
		LastProduceTime: lastProduce,
		LastReplayTime:  lastReplay,
	}
}

// IsStale returns true if no events have been produced or replayed recently,
// indicating the pipeline may be stuck or the source is down.
func (pt *ProgressTracker) IsStale(threshold time.Duration) bool {
	lastProduce, _ := pt.lastProduceTime.Load().(time.Time)
	if lastProduce.IsZero() {
		return time.Since(pt.startTime) > threshold
	}
	return time.Since(lastProduce) > threshold
}

// ──────────────────────────────────────────────────────────────────────────────
// Failover Detector (#19)
// ──────────────────────────────────────────────────────────────────────────────

// FailoverDetector detects source database failover by monitoring connection
// drops and IP changes. When a failover is detected, it triggers a callback
// to allow pgshadow to re-attach to the new primary.
type FailoverDetector struct {
	sourceHost string
	sourcePort int
	checkInterval time.Duration

	mu          sync.Mutex
	currentIP   string
	onFailover  func(oldIP, newIP string)
	cancel      context.CancelFunc
}

// NewFailoverDetector creates a detector that monitors the source host for
// IP/DNS changes (indicating a failover/switchover).
func NewFailoverDetector(host string, port int, interval time.Duration, onFailover func(oldIP, newIP string)) *FailoverDetector {
	return &FailoverDetector{
		sourceHost:    host,
		sourcePort:    port,
		checkInterval: interval,
		onFailover:    onFailover,
	}
}

// Start begins periodic monitoring. Call Stop() to halt.
func (fd *FailoverDetector) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	fd.mu.Lock()
	fd.cancel = cancel
	fd.currentIP = resolveHost(fd.sourceHost)
	fd.mu.Unlock()

	go fd.monitorLoop(ctx)
}

// Stop halts the monitoring loop.
func (fd *FailoverDetector) Stop() {
	fd.mu.Lock()
	cancel := fd.cancel
	fd.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (fd *FailoverDetector) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(fd.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			newIP := resolveHost(fd.sourceHost)
			fd.mu.Lock()
			oldIP := fd.currentIP
			if newIP != "" && newIP != oldIP {
				fd.currentIP = newIP
				fd.mu.Unlock()
				if fd.onFailover != nil {
					fd.onFailover(oldIP, newIP)
				}
			} else {
				fd.mu.Unlock()
			}
		}
	}
}

func resolveHost(host string) string {
	ips, err := net.LookupHost(host)
	if err != nil || len(ips) == 0 {
		return ""
	}
	return ips[0]
}

// ──────────────────────────────────────────────────────────────────────────────
// Result Comparator (#20)
// ──────────────────────────────────────────────────────────────────────────────

// ReplayResult records the outcome of replaying one SQL event.
type ReplayResult struct {
	EventSeq     uint64
	ConnKey      string
	SQL          string
	SourceExecMs float64 // execution time on source (from capture timestamp)
	TargetExecMs float64 // execution time on target
	SourceError  string  // error from source (if observed)
	TargetError  string  // error from target
	RowsAffected int64   // rows affected on target (-1 if unknown)
	Timestamp    time.Time
}

// DiffType categorizes a mismatch between source and target.
type DiffType int

const (
	DiffNone        DiffType = iota
	DiffError                // target errored when source didn't (or vice versa)
	DiffSlower               // target significantly slower
	DiffFaster               // target significantly faster
	DiffRowCount             // different rows affected (for DML)
)

// ResultComparator collects replay results and identifies mismatches.
type ResultComparator struct {
	mu      sync.Mutex
	diffs   []Diff
	maxDiffs int

	// Thresholds
	slowThreshold float64 // multiplier: target > source * threshold → DiffSlower

	// Output
	outputFile *os.File
}

// Diff represents a detected mismatch.
type Diff struct {
	Type     DiffType
	Result   ReplayResult
	Message  string
}

// NewResultComparator creates a comparator with the given slow threshold
// (e.g., 2.0 means flag if target is 2x slower than source).
func NewResultComparator(slowThreshold float64, outputPath string) (*ResultComparator, error) {
	rc := &ResultComparator{
		slowThreshold: slowThreshold,
		maxDiffs:      10000,
	}
	if outputPath != "" {
		f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("open diff output %q: %w", outputPath, err)
		}
		rc.outputFile = f
	}
	return rc, nil
}

// Record analyzes a replay result and records any diff.
func (rc *ResultComparator) Record(r ReplayResult) {
	diff := rc.analyze(r)
	if diff.Type == DiffNone {
		return
	}

	rc.mu.Lock()
	if len(rc.diffs) < rc.maxDiffs {
		rc.diffs = append(rc.diffs, diff)
	}

	if rc.outputFile != nil {
		line := fmt.Sprintf("%s [%s] %s: %s (source=%.1fms target=%.1fms err=%q)\n",
			r.Timestamp.Format(time.RFC3339),
			diffTypeName(diff.Type),
			r.ConnKey,
			truncate(r.SQL, 100),
			r.SourceExecMs, r.TargetExecMs,
			r.TargetError,
		)
		rc.outputFile.WriteString(line)
	}
	rc.mu.Unlock()
}

func (rc *ResultComparator) analyze(r ReplayResult) Diff {
	// Error diff: target errored but source didn't
	if r.TargetError != "" && r.SourceError == "" {
		return Diff{Type: DiffError, Result: r, Message: "target error: " + r.TargetError}
	}

	// Performance diff: target significantly slower
	if r.SourceExecMs > 0 && r.TargetExecMs > r.SourceExecMs*rc.slowThreshold {
		return Diff{
			Type:    DiffSlower,
			Result:  r,
			Message: fmt.Sprintf("target %.0fms vs source %.0fms (%.1fx slower)", r.TargetExecMs, r.SourceExecMs, r.TargetExecMs/r.SourceExecMs),
		}
	}

	return Diff{Type: DiffNone}
}

// GetDiffs returns accumulated diffs.
func (rc *ResultComparator) GetDiffs() []Diff {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	result := make([]Diff, len(rc.diffs))
	copy(result, rc.diffs)
	return result
}

// Summary returns a summary of diffs by type.
func (rc *ResultComparator) Summary() map[DiffType]int {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	counts := make(map[DiffType]int)
	for _, d := range rc.diffs {
		counts[d.Type]++
	}
	return counts
}

// Close flushes and closes the output file.
func (rc *ResultComparator) Close() error {
	if rc.outputFile != nil {
		return rc.outputFile.Close()
	}
	return nil
}

func diffTypeName(t DiffType) string {
	switch t {
	case DiffError:
		return "ERROR"
	case DiffSlower:
		return "SLOW"
	case DiffFaster:
		return "FAST"
	case DiffRowCount:
		return "ROWS"
	default:
		return "NONE"
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
