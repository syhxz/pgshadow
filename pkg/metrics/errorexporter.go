// errorexporter.go implements the ErrorExporter (task 11.2, R10.8).
//
// When error export is enabled, ExportError appends a human-readable, replayable
// record of each failed statement (timestamp, ConnID, error reason, and the SQL
// text) to the configured error-log file. The file is opened append-only and
// every write is guarded by a mutex so concurrent replay workers can export
// errors safely. When export is disabled the exporter is a no-op.
package metrics

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"pgshadow/pkg/core"
)

// errorExporter is the concrete ErrorExporter implementation. A zero-value
// (enabled=false, file=nil) exporter is a valid no-op.
type errorExporter struct {
	enabled bool

	mu   sync.Mutex
	file *os.File
}

// NewErrorExporter constructs an ErrorExporter from the metrics config.
//
// Export is active only when cfg.ExportErrors is true and cfg.ErrorLog names a
// path; in that case the error-log file is created if absent and opened
// append-only. Otherwise the returned exporter is a no-op whose ExportError and
// Close calls succeed without touching the filesystem (R10.8).
func NewErrorExporter(cfg Config) (ErrorExporter, error) {
	if !cfg.ExportErrors || cfg.ErrorLog == "" {
		return &errorExporter{enabled: false}, nil
	}
	f, err := os.OpenFile(cfg.ErrorLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("metrics: cannot open error log %q: %w", cfg.ErrorLog, err)
	}
	return &errorExporter{enabled: true, file: f}, nil
}

// ExportError appends a record for a failed statement to the error log (R10.8).
// The record captures the capture timestamp, source ConnID, the failure reason,
// and the statement text. It is a no-op when export is disabled or ev is nil.
// Concurrent calls are serialized so records never interleave.
func (e *errorExporter) ExportError(ev *core.SQLEvent, execErr error) error {
	if e == nil || !e.enabled || ev == nil {
		return nil
	}

	reason := "unknown"
	if execErr != nil {
		reason = execErr.Error()
	}

	record := formatErrorRecord(ev, reason)

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.file == nil {
		return nil
	}
	if _, err := e.file.WriteString(record); err != nil {
		return fmt.Errorf("metrics: write error log: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying error-log file. It is safe to call on
// a disabled exporter and idempotent on repeated calls.
func (e *errorExporter) Close() error {
	if e == nil || !e.enabled {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.file == nil {
		return nil
	}
	err := e.file.Close()
	e.file = nil
	return err
}

// formatErrorRecord renders one error entry. The format is a SQL comment header
// carrying the timestamp, ConnID and failure reason, followed by the statement
// terminated with a semicolon and newline so the file stays roughly replayable.
// The reason is collapsed onto a single line so each record is line-delimited.
func formatErrorRecord(ev *core.SQLEvent, reason string) string {
	reason = strings.ReplaceAll(reason, "\n", " ")
	reason = strings.ReplaceAll(reason, "\r", " ")

	sql := strings.TrimRight(ev.SQL, "\n")

	var b strings.Builder
	b.WriteString("-- ")
	b.WriteString(ev.Timestamp.UTC().Format(time.RFC3339Nano))
	b.WriteString(" conn=")
	b.WriteString(ev.Conn.Key())
	b.WriteString(" error: ")
	b.WriteString(reason)
	b.WriteByte('\n')
	b.WriteString(sql)
	b.WriteString(";\n")
	return b.String()
}
