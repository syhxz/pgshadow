package replayguard

import (
	"strings"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

func TestCurrentTime_NotDoubleReplace(t *testing.T) {
	// Ensure current_timestamp is replaced once, and current_time is replaced once,
	// without current_time regex matching inside already-replaced current_timestamp.
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	sg := New(Config{RewriteTimeFunctions: true})

	ev := &core.SQLEvent{
		SQL:       "SELECT current_timestamp, current_time",
		Timestamp: ts,
	}
	decision := sg.Apply(ev)
	if decision != Rewrite {
		t.Fatalf("expected Rewrite, got %d", decision)
	}

	t.Logf("Rewritten: %s", ev.SQL)

	// current_timestamp → ::timestamptz, current_time → ::timetz
	tsLiteral := "'2026-07-01 10:00:00.000000+00:00'::timestamptz"
	timeLiteral := "'10:00:00.000000+00:00'::timetz"
	if count := strings.Count(ev.SQL, tsLiteral); count != 1 {
		t.Errorf("expected timestamptz literal to appear exactly 1 time, got %d", count)
	}
	if count := strings.Count(ev.SQL, timeLiteral); count != 1 {
		t.Errorf("expected timetz literal to appear exactly 1 time, got %d", count)
	}
}
