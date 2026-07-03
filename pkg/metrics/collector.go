// collector.go implements the Prometheus-backed Collector (task 11.1, R10).
//
// It registers a catalog of counters, gauges and histograms on a private
// registry, serves them on /metrics (default port 9090, R10.1), and maintains
// derived alert-condition gauges for the five thresholds called out by the
// design: replay lag > 60s (R10.2), queue depth > 100000 (R10.3), replay error
// rate > 1% (R10.4), target p99 > 2x source p99 (R10.5), and packet drop rate
// > 0.1% (R10.6). It also records source-vs-target execution time (R10.7) and
// source/target QPS throughput (R10.9).
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pgshadow/pkg/filter"
)

// DefaultPort is the metrics port applied when none is configured (R10.1).
const DefaultPort = 9090

// Alert thresholds (R10.2-R10.6). Exposed as constants so the boundary tests
// (task 11.3) and downstream alerting rules can reference the same values.
const (
	ThresholdLagSeconds = 60.0    // R10.2 replay lag > 60s
	ThresholdQueueDepth = 100000  // R10.3 queue depth > 100000
	ThresholdErrorRate  = 0.01    // R10.4 replay error rate > 1%
	ThresholdP99Ratio   = 2.0     // R10.5 target p99 > 2x source p99
	ThresholdDropRate   = 0.001   // R10.6 packet drop rate > 0.1%
	execSampleWindow    = 100_000 // bounded reservoir for p99 estimation
)

// execBuckets are the histogram buckets (seconds) for source/target execution
// time. They span sub-millisecond to multi-second queries.
var execBuckets = []float64{
	0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
	0.25, 0.5, 1, 2.5, 5, 10,
}

// collector is the concrete Prometheus Collector implementation.
type collector struct {
	reg *prometheus.Registry

	// Counters
	packetsReceived prometheus.Counter
	packetsDropped  prometheus.Counter
	parseErrors     prometheus.Counter
	classified      *prometheus.CounterVec // labels: class, kept
	overflow        prometheus.Counter
	replayTotal     prometheus.Counter
	replayErrors    prometheus.Counter

	// Gauges
	queueDepth prometheus.Gauge
	replayLag  prometheus.Gauge
	errorRate  prometheus.Gauge
	dropRate   prometheus.Gauge
	sourceQPS  prometheus.Gauge
	targetQPS  prometheus.Gauge
	sourceP99  prometheus.Gauge
	targetP99  prometheus.Gauge

	// Alert-condition gauges (1 = firing, 0 = clear). R10.2-R10.6
	alertLag       prometheus.Gauge
	alertDepth     prometheus.Gauge
	alertErrorRate prometheus.Gauge
	alertP99       prometheus.Gauge
	alertDropRate  prometheus.Gauge

	// Execution-time histograms keyed by side (source/target). R10.5, R10.7
	execTime *prometheus.HistogramVec

	// internal state guarded by mu for derived computations
	mu           sync.Mutex
	totReplays   uint64
	totErrors    uint64
	execCount    uint64     // counts ExecTime calls to amortize quantile refresh
	lastReceived uint64     // last reported cumulative received total
	lastDropped  uint64     // last reported cumulative dropped total
	sourceTimes  *reservoir // bounded samples for source p99 (R10.5)
	targetTimes  *reservoir // bounded samples for target p99 (R10.5)
}

// New constructs a Prometheus-backed Collector with a private registry so that
// multiple instances (e.g. across tests) never collide on the global default
// registry. The returned value satisfies the Collector interface.
func New(cfg Config) Collector {
	c := &collector{
		reg:         prometheus.NewRegistry(),
		sourceTimes: newReservoir(execSampleWindow),
		targetTimes: newReservoir(execSampleWindow),
	}

	c.packetsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgshadow_packets_received_total",
		Help: "Total packets received by the capture layer (R10.6).",
	})
	c.packetsDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgshadow_packets_dropped_total",
		Help: "Total packets dropped by the capture layer (R1.7, R10.6).",
	})
	c.parseErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgshadow_parse_errors_total",
		Help: "Total protocol parse errors (R3.6).",
	})
	c.classified = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgshadow_classified_total",
		Help: "Statements classified by class and keep/drop decision (R5).",
	}, []string{"class", "kept"})
	c.overflow = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgshadow_queue_overflow_total",
		Help: "Total events discarded due to queue overflow (R6.5).",
	})
	c.replayTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgshadow_replay_total",
		Help: "Total replay attempts against the target (R10.4).",
	})
	c.replayErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pgshadow_replay_errors_total",
		Help: "Total failed replay attempts against the target (R10.4).",
	})

	c.queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_queue_depth",
		Help: "Current buffer queue depth (R10.3).",
	})
	c.replayLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_replay_lag_seconds",
		Help: "Current replay lag in seconds (R10.2).",
	})
	c.errorRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_replay_error_rate",
		Help: "Replay error rate as a fraction in [0,1] (R10.4).",
	})
	c.dropRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_packet_drop_rate",
		Help: "Capture packet drop rate as a fraction in [0,1] (R10.6).",
	})
	c.sourceQPS = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_source_qps",
		Help: "Observed source-database QPS throughput (R10.9).",
	})
	c.targetQPS = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_target_qps",
		Help: "Observed target-database QPS throughput (R10.9).",
	})
	c.sourceP99 = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_source_exec_p99_seconds",
		Help: "Estimated source-database execution-time 99th percentile (R10.5, R10.7).",
	})
	c.targetP99 = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_target_exec_p99_seconds",
		Help: "Estimated target-database execution-time 99th percentile (R10.5).",
	})

	c.alertLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_alert_replay_lag",
		Help: "Alert: 1 when replay lag exceeds 60s, else 0 (R10.2).",
	})
	c.alertDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_alert_queue_depth",
		Help: "Alert: 1 when queue depth exceeds 100000, else 0 (R10.3).",
	})
	c.alertErrorRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_alert_error_rate",
		Help: "Alert: 1 when replay error rate exceeds 1%, else 0 (R10.4).",
	})
	c.alertP99 = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_alert_target_p99",
		Help: "Alert: 1 when target p99 exceeds 2x source p99, else 0 (R10.5).",
	})
	c.alertDropRate = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgshadow_alert_packet_drop_rate",
		Help: "Alert: 1 when packet drop rate exceeds 0.1%, else 0 (R10.6).",
	})

	c.execTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgshadow_exec_time_seconds",
		Help:    "Execution time per statement, by side (source|target) (R10.5, R10.7).",
		Buckets: execBuckets,
	}, []string{"side"})

	c.reg.MustRegister(
		c.packetsReceived, c.packetsDropped, c.parseErrors, c.classified,
		c.overflow, c.replayTotal, c.replayErrors,
		c.queueDepth, c.replayLag, c.errorRate, c.dropRate,
		c.sourceQPS, c.targetQPS, c.sourceP99, c.targetP99,
		c.alertLag, c.alertDepth, c.alertErrorRate, c.alertP99, c.alertDropRate,
		c.execTime,
	)
	return c
}

// PacketDrops records cumulative received/dropped packet counts and updates the
// drop-rate gauge and its alert condition (R1.7, R10.6). received and dropped
// are cumulative totals from the capture layer's Stats().
func (c *collector) PacketDrops(received, dropped uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Counters are monotonic; advance them by the delta over the last reported
	// cumulative totals. Resets (totals going backwards) are treated as a new
	// baseline without decrementing the counter.
	if received >= c.lastReceived {
		c.packetsReceived.Add(float64(received - c.lastReceived))
	}
	c.lastReceived = received
	if dropped >= c.lastDropped {
		c.packetsDropped.Add(float64(dropped - c.lastDropped))
	}
	c.lastDropped = dropped

	var rate float64
	if received > 0 {
		rate = float64(dropped) / float64(received)
	}
	c.dropRate.Set(rate)
	c.alertDropRate.Set(boolToFloat(rate > ThresholdDropRate))
}

// ParseError increments the protocol parse-error counter (R3.6).
func (c *collector) ParseError() { c.parseErrors.Inc() }

// Classified records a keep/drop classification decision by class (R5).
func (c *collector) Classified(class filter.StmtClass, kept bool) {
	c.classified.WithLabelValues(className(class), boolToLabel(kept)).Inc()
}

// QueueDepth records the current queue depth and updates its alert (R10.3).
func (c *collector) QueueDepth(depth int) {
	c.queueDepth.Set(float64(depth))
	c.alertDepth.Set(boolToFloat(depth > ThresholdQueueDepth))
}

// Overflow increments the queue-overflow counter (R6.5).
func (c *collector) Overflow() { c.overflow.Inc() }

// ReplayLag records current replay lag and updates its alert (R10.2).
func (c *collector) ReplayLag(seconds float64) {
	c.replayLag.Set(seconds)
	c.alertLag.Set(boolToFloat(seconds > ThresholdLagSeconds))
}

// ReplayResult records a replay attempt outcome and updates the error-rate
// gauge and its alert (R10.4). A nil err denotes success. Both the Prometheus
// counter and the internal total are incremented under the same lock to keep
// them consistent across concurrent callers.
func (c *collector) ReplayResult(err error) {
	c.mu.Lock()
	c.replayTotal.Inc()
	c.totReplays++
	if err != nil {
		c.totErrors++
		c.replayErrors.Inc()
	}
	rate := 0.0
	if c.totReplays > 0 {
		rate = float64(c.totErrors) / float64(c.totReplays)
	}
	c.mu.Unlock()

	c.errorRate.Set(rate)
	c.alertErrorRate.Set(boolToFloat(rate > ThresholdErrorRate))
}

// p99RefreshInterval controls how often the expensive quantile computation is
// performed. Computing exact quantiles via sort on every statement is O(n log n)
// and allocates; batching it amortizes the cost to negligible levels.
const p99RefreshInterval = 1000

// ExecTime records the derived source and measured target execution times for a
// statement, updates the per-side histograms, and periodically refreshes the p99
// comparison alert (R10.5, R10.7). The quantile computation is amortized: it
// runs every p99RefreshInterval calls rather than on every statement, avoiding
// the O(n log n) sort and 800KB allocation on the hot path. A non-positive
// duration for a side is skipped (e.g. no source timing available outside
// bidirectional capture).
func (c *collector) ExecTime(stmt string, source, target time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if source > 0 {
		s := source.Seconds()
		c.execTime.WithLabelValues("source").Observe(s)
		c.sourceTimes.add(s)
	}
	if target > 0 {
		t := target.Seconds()
		c.execTime.WithLabelValues("target").Observe(t)
		c.targetTimes.add(t)
	}

	c.execCount++
	if c.execCount == 1 || c.execCount%p99RefreshInterval == 0 {
		srcP99 := c.sourceTimes.quantile(0.99)
		tgtP99 := c.targetTimes.quantile(0.99)
		c.sourceP99.Set(srcP99)
		c.targetP99.Set(tgtP99)
		c.alertP99.Set(boolToFloat(srcP99 > 0 && tgtP99 > ThresholdP99Ratio*srcP99))
	}
}

// Throughput records the observed source and target QPS for comparison (R10.9).
func (c *collector) Throughput(sourceQPS, targetQPS float64) {
	c.sourceQPS.Set(sourceQPS)
	c.targetQPS.Set(targetQPS)
}

// healthState holds the current health state of the application.
type healthState struct {
	mu         sync.RWMutex
	ready      bool
	lastError  string
}

// SetReady sets the ready state of the application.
func (h *healthState) SetReady(ready bool, errMsg string) {
	h.mu.Lock()
	h.ready = ready
	h.lastError = errMsg
	h.mu.Unlock()
}

// IsReady returns the current ready state.
func (h *healthState) IsReady() (bool, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.ready, h.lastError
}

// globalHealth holds the global health state for the collector.
// This is used for the /health and /ready endpoints.
var globalHealth healthState

// SetHealthStatus sets the global health status for the /health and /ready endpoints.
// ready should be true when all components are initialized and the application
// is ready to process traffic.
func SetHealthStatus(ready bool, errMsg string) {
	globalHealth.SetReady(ready, errMsg)
}

// Serve starts an HTTP server exposing the Prometheus catalog at /metrics, 
// plus health check endpoints at /health and /ready. A non-positive port 
// defaults to 9090 (R10.1). It blocks until the server stops.
//
// Endpoints:
//   - /metrics   - Prometheus metrics (default port 9090)
//   - /health    - Liveness probe: returns 200 if the service is running
//   - /ready     - Readiness probe: returns 200 if the service is ready to process traffic
func (c *collector) Serve(port int) error {
	if port <= 0 {
		port = DefaultPort
	}
	mux := http.NewServeMux()
	
	// Prometheus metrics endpoint
	mux.Handle("/metrics", c.handler())
	
	// Health check endpoints
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/ready", readyHandler)
	
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	return srv.ListenAndServe()
}

// healthHandler handles the /health liveness probe.
// Returns 200 OK when the service is running.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// readyHandler handles the /ready readiness probe.
// Returns 200 OK when the service is ready to process traffic.
// Returns 503 Service Unavailable when not ready.
func readyHandler(w http.ResponseWriter, r *http.Request) {
	ready, errMsg := globalHealth.IsReady()
	if ready {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		if errMsg != "" {
			w.Write([]byte(errMsg))
		} else {
			w.Write([]byte("Not ready"))
		}
	}
}

// handler returns the /metrics HTTP handler bound to this collector's registry.
// It is unexported so tests in this package can mount it on httptest servers.
func (c *collector) handler() http.Handler {
	return promhttp.HandlerFor(c.reg, promhttp.HandlerOpts{})
}

// --- helpers -----------------------------------------------------------------

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func boolToLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// className maps a filter.StmtClass to a stable metric label without requiring
// the filter package to expose a String method.
func className(class filter.StmtClass) string {
	switch class {
	case filter.ClassPlainSelect:
		return "plain_select"
	case filter.ClassProcSelect:
		return "proc_select"
	case filter.ClassCall:
		return "call"
	case filter.ClassDML:
		return "dml"
	case filter.ClassDDL:
		return "ddl"
	case filter.ClassBegin:
		return "begin"
	case filter.ClassCommitRollback:
		return "commit_rollback"
	case filter.ClassUtility:
		return "utility"
	case filter.ClassCopyFrom:
		return "copy_from"
	default:
		return "unknown"
	}
}

// reservoir is a bounded ring of recent samples used to estimate quantiles for
// the p99 alert comparison (R10.5) without unbounded memory growth.
type reservoir struct {
	buf  []float64
	size int
	next int
	full bool
}

func newReservoir(size int) *reservoir {
	if size <= 0 {
		size = 1
	}
	return &reservoir{buf: make([]float64, 0, size), size: size}
}

func (r *reservoir) add(v float64) {
	if !r.full {
		r.buf = append(r.buf, v)
		if len(r.buf) == r.size {
			r.full = true
			r.next = 0
		}
		return
	}
	r.buf[r.next] = v
	r.next = (r.next + 1) % r.size
}

// quantile returns the requested quantile (q in [0,1]) of the current samples
// using nearest-rank, or 0 when there are no samples.
func (r *reservoir) quantile(q float64) float64 {
	n := len(r.buf)
	if n == 0 {
		return 0
	}
	cp := make([]float64, n)
	copy(cp, r.buf)
	sort.Float64s(cp)
	if q <= 0 {
		return cp[0]
	}
	if q >= 1 {
		return cp[n-1]
	}
	idx := int(q*float64(n-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return cp[idx]
}
