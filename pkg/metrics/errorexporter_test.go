package metrics

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

func sampleEvent(sql string) *core.SQLEvent {
	return &core.SQLEvent{
		Conn: core.ConnID{
			SrcIP:   netip.MustParseAddr("10.0.0.1"),
			SrcPort: 54321,
			DstIP:   netip.MustParseAddr("10.0.0.2"),
			DstPort: 5432,
		},
		SQL:       sql,
		Timestamp: time.Date(2024, 1, 2, 15, 4, 5, 0, time.UTC),
	}
}

// ExportError writes a record with timestamp, ConnID, reason, and SQL (R10.8).
func TestExportError_WritesRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")

	exp, err := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	if err != nil {
		t.Fatalf("NewErrorExporter: %v", err)
	}

	ev := sampleEvent("INSERT INTO t VALUES (1)")
	if err := exp.ExportError(ev, errors.New("duplicate key value")); err != nil {
		t.Fatalf("ExportError: %v", err)
	}
	if err := exp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	got := string(data)

	for _, want := range []string{
		"2024-01-02T15:04:05Z",
		"conn=" + ev.Conn.Key(),
		"error: duplicate key value",
		"INSERT INTO t VALUES (1);",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("record missing %q; full output:\n%s", want, got)
		}
	}
}

// A nil execErr is recorded with a placeholder reason rather than panicking.
func TestExportError_NilError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")
	exp, err := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	if err != nil {
		t.Fatalf("NewErrorExporter: %v", err)
	}
	defer exp.Close()

	if err := exp.ExportError(sampleEvent("SELECT 1"), nil); err != nil {
		t.Fatalf("ExportError: %v", err)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "error: unknown") {
		t.Errorf("expected placeholder reason, got:\n%s", data)
	}
}

// Multi-line error reasons are collapsed so each record stays line-delimited.
func TestExportError_CollapsesMultilineReason(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")
	exp, _ := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	defer exp.Close()

	_ = exp.ExportError(sampleEvent("SELECT 1"), errors.New("line one\nline two\r\nline three"))
	data, _ := os.ReadFile(path)
	header := strings.SplitN(string(data), "\n", 2)[0]
	// The header line must contain no embedded CR/LF so each record stays
	// line-delimited, and all three reason fragments must survive.
	if strings.ContainsAny(header, "\r\n") {
		t.Errorf("header still contains CR/LF: %q", header)
	}
	for _, frag := range []string{"line one", "line two", "line three"} {
		if !strings.Contains(header, frag) {
			t.Errorf("reason fragment %q missing from header: %q", frag, header)
		}
	}
}

// When export is disabled, ExportError is a no-op and writes no file (R10.8).
func TestExportError_DisabledIsNoOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")

	exp, err := NewErrorExporter(Config{ExportErrors: false, ErrorLog: path})
	if err != nil {
		t.Fatalf("NewErrorExporter: %v", err)
	}
	if err := exp.ExportError(sampleEvent("DELETE FROM t"), errors.New("boom")); err != nil {
		t.Fatalf("ExportError (disabled): %v", err)
	}
	if err := exp.Close(); err != nil {
		t.Fatalf("Close (disabled): %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("disabled exporter must not create the error log; stat err=%v", err)
	}
}

// An empty error-log path disables export even when ExportErrors is true.
func TestExportError_EmptyPathIsNoOp(t *testing.T) {
	exp, err := NewErrorExporter(Config{ExportErrors: true, ErrorLog: ""})
	if err != nil {
		t.Fatalf("NewErrorExporter: %v", err)
	}
	if err := exp.ExportError(sampleEvent("SELECT 1"), errors.New("x")); err != nil {
		t.Fatalf("ExportError: %v", err)
	}
	if err := exp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// ExportError tolerates a nil event.
func TestExportError_NilEvent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")
	exp, _ := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	defer exp.Close()
	if err := exp.ExportError(nil, errors.New("x")); err != nil {
		t.Fatalf("ExportError(nil): %v", err)
	}
}

// Concurrent ExportError calls append every record without interleaving or loss.
func TestExportError_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")
	exp, err := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	if err != nil {
		t.Fatalf("NewErrorExporter: %v", err)
	}

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			ev := sampleEvent(fmt.Sprintf("SELECT %d", i))
			if err := exp.ExportError(ev, fmt.Errorf("err-%d", i)); err != nil {
				t.Errorf("ExportError: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if err := exp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	// Each record is exactly two lines (header + SQL).
	if len(lines) != 2*n {
		t.Fatalf("expected %d lines for %d records, got %d", 2*n, n, len(lines))
	}
	// Every record's SQL must appear exactly once (no loss, no interleave).
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("SELECT %d;", i)
		count := strings.Count(string(data), want+"\n")
		if count != 1 {
			t.Errorf("SQL %q appeared %d times, want 1", want, count)
		}
	}
}

// Close is idempotent and safe on a disabled exporter.
func TestClose_Idempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")
	exp, _ := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	if err := exp.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := exp.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// Records are appended across exporter instances pointing at the same file.
func TestExportError_AppendsAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "errors.sql")

	exp1, _ := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	_ = exp1.ExportError(sampleEvent("SELECT 1"), errors.New("a"))
	_ = exp1.Close()

	exp2, _ := NewErrorExporter(Config{ExportErrors: true, ErrorLog: path})
	_ = exp2.ExportError(sampleEvent("SELECT 2"), errors.New("b"))
	_ = exp2.Close()

	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "SELECT 1;") || !strings.Contains(string(data), "SELECT 2;") {
		t.Errorf("append-only across instances failed; output:\n%s", data)
	}
}
