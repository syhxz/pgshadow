// Command pgshadow is the entry point for the passive capture-and-replay
// pipeline. It performs ordered fail-fast startup, wires the pipeline stages
// via channels, and runs a graceful shutdown drain (R12). It never opens a
// connection to the Source_Database (R12.6).
//
// Startup ordering (task 12.1, R1.8/R1.9/R6.10/R8.6/R11.9/R11.10):
//
//	load+validate config        (config.Load — R8.6, R11.9, R11.10)
//	  → init metrics            (collector + error exporter)
//	  → open capture handle     (capture.Open — validates interface R1.8 + BPF R1.9)
//	  → connect queue           (queue.New — validates Kafka reachability R6.10)
//	  → connect target pool     (replayer.NewPool)
//	  → only then start the pipeline goroutines
//
// Any startup error aborts the remaining steps, tears down whatever was already
// opened, and causes main() to print the descriptive error and exit non-zero.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pgshadow/pkg/capture"
	"pgshadow/pkg/config"
	"pgshadow/internal/logger"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
	"pgshadow/pkg/replayguard"
)

// defaultConfigPath is the configuration file used when --config is omitted.
const defaultConfigPath = "config.yaml"

// debugMode enables verbose pipeline debug logging. Set via --debug flag.
var debugMode bool

// deps holds the constructors used during startup. Production wires them to the
// real package constructors via productionDeps; tests inject fakes so the
// ordered fail-fast sequence can be exercised without a live NIC or database.
type deps struct {
	loadConfig       func(path string) (*config.Config, error)
	newMetrics       func(metrics.Config) metrics.Collector
	newErrorExporter func(metrics.Config) (metrics.ErrorExporter, error)
	openCapture      func(capture.Config) (capture.Source, error)
	newQueue         func(queue.Config, *metrics.Collector) (queue.Queue, error)
	newPool          func(context.Context, replayer.Config) (replayer.Pool, error)
	newReplayer      func(replayer.Config, queue.Queue, replayer.Pool, metrics.Collector, *replayguard.Guard) replayer.Replayer
}

// productionDeps wires deps to the real package constructors.
func productionDeps() deps {
	return deps{
		loadConfig:       config.Load,
		newMetrics:       metrics.New,
		newErrorExporter: metrics.NewErrorExporter,
		openCapture:      capture.Open,
		newQueue:         queue.New,
		newPool: func(ctx context.Context, cfg replayer.Config) (replayer.Pool, error) {
			// NewPool returns the concrete *ConnPool; surface it as the Pool
			// interface so the pool step is injectable for tests.
			return replayer.NewPool(ctx, cfg)
		},
		newReplayer: func(cfg replayer.Config, q queue.Queue, pool replayer.Pool, coll metrics.Collector, sg *replayguard.Guard) replayer.Replayer {
			var hook func(error)
			if coll != nil {
				hook = func(err error) { coll.ReplayResult(err) }
			}
			return replayer.NewReplayerWithHook(cfg, q, pool, hook, sg)
		},
	}
}

// components holds everything assembled during a successful startup. The fields
// are owned by the returned value and released by Close (in reverse open order)
// on shutdown or on a partial-startup failure.
type components struct {
	cfg         *config.Config
	collector   metrics.Collector
	errorExport metrics.ErrorExporter
	capture     capture.Source
	queue       queue.Queue
	pool        replayer.Pool
	replayer    replayer.Replayer
}

// Close releases the components in reverse order of acquisition. It tolerates
// nil fields so it can be used to unwind a partially-completed startup.
func (c *components) Close() {
	if c == nil {
		return
	}
	if c.pool != nil {
		c.pool.Close()
	}
	if c.queue != nil {
		_ = c.queue.Close()
	}
	if c.capture != nil {
		_ = c.capture.Close()
	}
	if c.errorExport != nil {
		_ = c.errorExport.Close()
	}
}

// startup performs the ordered, fail-fast startup sequence. Each step that
// fails returns a descriptive error that names the offending resource and, on
// failure, any already-opened resources are torn down before returning so a
// partial startup never leaks a capture handle, queue, or pool.
//
// The steps are intentionally ordered cheapest-and-safest first: configuration
// and metrics cannot touch external systems, capture validates the interface
// and BPF filter (R1.8, R1.9), the queue validates Kafka reachability (R6.10),
// and only the final pool step dials the Target_Database.
func startup(ctx context.Context, path string, d deps) (*components, error) {
	// Initialize logging from environment variables
	logger.InitFromEnv()
	logger.Info("pgshadow starting up...")

	// Step 1: load + validate configuration. config.Load surfaces missing
	// required parameters (R11.9), invalid value ranges / cross-field
	// constraints (R11.10), and the invalid target dialect (R8.6).
	cfg, err := d.loadConfig(path)
	if err != nil {
		logger.Errorf("startup: configuration: %v", err)
		return nil, fmt.Errorf("startup: configuration: %w", err)
	}

	c := &components{cfg: cfg}

	// Step 2: initialize metrics (collector + optional error exporter). These
	// are local and cannot fail on external systems, but a bad error-log path
	// is surfaced here before any capture/queue/pool resources are opened.
	c.collector = d.newMetrics(cfg.Metrics)
	errExport, err := d.newErrorExporter(cfg.Metrics)
	if err != nil {
		c.Close()
		logger.Errorf("startup: metrics: %v", err)
		return nil, fmt.Errorf("startup: metrics: %w", err)
	}
	c.errorExport = errExport

	// Step 3: open the capture handle. capture.Open validates the interface
	// (R1.8) and the BPF filter (R1.9) and fails fast naming the offender.
	logger.Info("opening capture handle...")
	src, err := d.openCapture(cfg.Capture)
	if err != nil {
		c.Close()
		logger.Errorf("startup: capture: %v", err)
		return nil, fmt.Errorf("startup: capture: %w", err)
	}
	c.capture = src
	logger.Infof("capture opened: interface=%s mode=%s", cfg.Capture.Interface, cfg.Capture.Mode)

	// Step 4: connect the queue. For the Kafka backend this validates broker
	// reachability and fails fast naming the unreachable brokers (R6.10).
	logger.Info("connecting to queue...")
	coll := c.collector
	q, err := d.newQueue(cfg.Queue, &coll)
	if err != nil {
		c.Close()
		logger.Errorf("startup: queue: %v", err)
		return nil, fmt.Errorf("startup: queue: %w", err)
	}
	c.queue = q
	logger.Infof("queue connected: type=%s", cfg.Queue.Type)

	// Step 5: connect the Target_Database connection pool. This is the only
	// startup step that dials an external database; it never touches the
	// Source_Database (R12.6).
	logger.Info("connecting to target database pool...")
	pool, err := d.newPool(ctx, cfg.Replayer)
	if err != nil {
		c.Close()
		logger.Errorf("startup: target pool: %v", err)
		return nil, fmt.Errorf("startup: target pool: %w", err)
	}
	c.pool = pool
	logger.Infof("target pool connected: host=%s database=%s", cfg.Replayer.TargetHost, cfg.Replayer.TargetDatabase)

	// Step 6: build the replayer. Only after every prerequisite is open do we
	// construct the consumer that drives the pipeline goroutines.
	// The safeguard is created from config and injected into the replayer's
	// execution chain to filter/rewrite events before they hit the target DB.
	sg := replayguard.New(convertReplayGuardConfig(cfg.ReplayGuard))
	c.replayer = d.newReplayer(cfg.Replayer, q, pool, c.collector, sg)

	// Mark the service as ready for the /ready endpoint
	metrics.SetHealthStatus(true, "")
	logger.Info("pgshadow startup complete, ready to process traffic")

	return c, nil
}

// run performs startup, exposes the metrics endpoint, starts the pipeline, and
// blocks until ctx is cancelled. It returns the first startup error (if any) so
// main can map it to a non-zero exit code.
//
// stderr receives operator-facing status lines; it is a parameter so tests can
// capture output without touching the process streams.
func run(ctx context.Context, args []string, stderr io.Writer, d deps) error {
	fs := flag.NewFlagSet("pgshadow", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", defaultConfigPath, "path to the pgshadow YAML configuration file")
	debugFlag := fs.Bool("debug", false, "enable verbose debug logging for pipeline diagnostics")
	if err := fs.Parse(args); err != nil {
		return err
	}
	debugMode = *debugFlag

	// Set initial health status to not ready
	metrics.SetHealthStatus(false, "starting up")

	c, err := startup(ctx, *configPath, d)
	if err != nil {
		// Mark as not ready on startup failure
		metrics.SetHealthStatus(false, err.Error())
		return err
	}
	defer func() {
		// Mark as not ready during shutdown
		metrics.SetHealthStatus(false, "shutting down")
		c.Close()
	}()

	// Expose the Prometheus /metrics endpoint when metrics are enabled (R10.1).
	// Serving runs in the background and is not part of the fail-fast sequence;
	// a bind failure is logged rather than aborting an otherwise-healthy run.
	if c.cfg.Metrics.Enabled {
		port := c.cfg.Metrics.Port
		go func() {
			if serveErr := c.collector.Serve(port); serveErr != nil {
				logger.Warnf("metrics endpoint stopped: %v", serveErr)
			}
		}()
	}

	// ── Pipeline (task 12.2) ─────────────────────────────────────────────────
	// Startup ordering and fail-fast are complete here: capture handle, queue,
	// and target pool are all open and the replayer is built. startPipeline (in
	// pipeline.go) wires the stages together —
	//
	//	capture.Source ─▶ reassembler ─▶ parser ─▶ state-machine/filter ─▶ queue
	//	                                                                      │
	//	                                                          replayer ◀──┘
	//
	// routing server→client ReadyForQuery into the transaction state machine
	// (R12.1) and never opening a socket to the Source_Database (R12.6). It
	// returns a handle so the graceful-shutdown drain (task 12.3) can tear the
	// goroutines down in order, and an error only if the producer cannot be
	// constructed.
	p, err := startPipeline(ctx, c)
	if err != nil {
		return err
	}

	// Block until a shutdown signal cancels ctx (SIGINT/SIGTERM, installed in
	// main via signal.NotifyContext), then run the ordered drain (R12.5) before
	// the deferred components.Close releases whatever the drain did not. The
	// drain stops capture, drains/checkpoints the queue, waits for in-flight
	// replay workers, and closes the pool — all under a bounded deadline.
	<-ctx.Done()
	p.drain(stderr)
	return nil
}

func main() {
	// Install graceful-shutdown signal handling: SIGINT/SIGTERM cancel ctx,
	// which run observes to start the ordered drain sequence (R12.5).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stderr, productionDeps()); err != nil {
		fmt.Fprintf(os.Stderr, "pgshadow: %v\n", err)
		os.Exit(1)
	}
}

// convertReplayGuardConfig bridges the config-layer SafeguardConfig (which uses
// string durations for YAML compatibility) to the safeguard package's typed
// SafeguardConfig (which uses time.Duration).
func convertReplayGuardConfig(c config.ReplayGuardConfig) replayguard.Config {
	var txDur time.Duration
	if c.MaxTransactionDuration != "" {
		txDur, _ = time.ParseDuration(c.MaxTransactionDuration)
	}

	var masks []replayguard.MaskRule
	for _, m := range c.MaskPatterns {
		masks = append(masks, replayguard.MaskRule{Pattern: m.Pattern, Replacement: m.Replacement})
	}

	return replayguard.Config{
		ExcludeDDL:             c.ExcludeDDL,
		RewriteTimeFunctions:   c.RewriteTimeFunctions,
		SkipNonDeterministic:   c.SkipNonDeterministic,
		SkipExternalDeps:       c.SkipExternalDeps,
		MaxReplayLagSeconds:    c.MaxReplayLagSeconds,
		MaxQueueDepth:          c.MaxQueueDepth,
		MaxTransactionDuration: txDur,
		MaxStatementsPerTx:     c.MaxStatementsPerTx,
		ReplayPercentage:       c.ReplayPercentage,
		IncludeTables:          c.IncludeTables,
		ExcludeTables:          c.ExcludeTables,
		IncludeUsers:           c.IncludeUsers,
		ReadOnly:               c.ReadOnly,
		MaskPatterns:           masks,
	}
}
