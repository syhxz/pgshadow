// Package metrics implements Module ⑥ — Monitoring. It exposes Prometheus
// metrics, evaluates alert conditions, and compares source-vs-target behavior
// (R10).
//
// This file declares the package interfaces and Config. The Collector
// implementation lives in collector.go (task 11.1); the ErrorExporter
// implementation is task 11.2.
package metrics

import (
	"time"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
)

// Collector observes all pipeline stages and exposes Prometheus metrics.
type Collector interface {
	PacketDrops(received, dropped uint64)               // R1.7, R10.6
	ParseError()                                        // R3.6
	Classified(class filter.StmtClass, kept bool)       // R5
	QueueDepth(depth int)                               // R10.3
	Overflow()                                          // R6.5
	ReplayLag(seconds float64)                          // R10.2
	ReplayResult(err error)                             // R10.4
	ExecTime(stmt string, source, target time.Duration) // R10.5, R10.7
	Throughput(sourceQPS, targetQPS float64)            // R10.9
	Serve(port int) error                               // /metrics; default 9090 R10.1
}

// ErrorExporter writes failed SQL records to disk (R10.8).
type ErrorExporter interface {
	ExportError(ev *core.SQLEvent, execErr error) error
	Close() error
}

// Config configures the metrics collector.
type Config struct {
	Enabled      bool   `yaml:"enabled"`
	Port         int    `yaml:"port"`          // default 9090 R10.1
	ExportErrors bool   `yaml:"export_errors"` // R10.8
	ErrorLog     string `yaml:"error_log"`     // R10.8
}
