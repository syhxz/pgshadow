package replayguard

import (
	"strings"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// =============================================================================
// Issue #6: SERIAL/IDENTITY sequence ID conflicts — safeguard.rewrite_sequences
// The shadow DB has independent sequences. nextval()/currval()/lastval()
// should execute normally on the shadow DB without rewriting.
// The RewriteSequences option is reserved for future hardcoded-ID handling.
// =============================================================================

func TestRewriteSequences_SequenceFunctionsPassThrough(t *testing.T) {
	// Even with RewriteSequences enabled, sequence function calls should
	// pass through unchanged — the shadow DB's own sequences handle them.
	s := New(Config{RewriteSequences: true})

	tests := []struct {
		name string
		sql  string
	}{
		{"nextval in INSERT", "INSERT INTO orders (id, name) VALUES (nextval('orders_id_seq'), 'test')"},
		{"currval reference", "INSERT INTO items (order_id) VALUES (currval('orders_id_seq'))"},
		{"lastval", "SELECT lastval()"},
		{"nextval in SELECT", "SELECT nextval('my_seq')"},
		{"setval", "SELECT setval('orders_id_seq', 10000)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: time.Now()}
			got := s.Apply(ev)
			if got == Block {
				t.Errorf("sequence SQL should not be blocked: %s", tt.sql)
			}
			if ev.SQL != tt.sql {
				t.Errorf("SQL should not be modified.\n  got:  %s\n  want: %s", ev.SQL, tt.sql)
			}
		})
	}
}

func TestRewriteSequences_Disabled(t *testing.T) {
	// When rewrite_sequences is false, sequence functions also pass through.
	s := New(Config{RewriteSequences: false})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO orders (id, name) VALUES (nextval('orders_id_seq'), 'test')",
		Timestamp: time.Now(),
	}
	got := s.Apply(ev)
	if got != Allow {
		t.Errorf("with RewriteSequences=false, expected Allow, got %d", got)
	}
	if ev.SQL != "INSERT INTO orders (id, name) VALUES (nextval('orders_id_seq'), 'test')" {
		t.Error("SQL should not be modified when RewriteSequences is false")
	}
}

// =============================================================================
// Issue #7: now()/current_timestamp time inconsistency — safeguard.rewrite_time_functions
// =============================================================================

func TestRewriteTimeFunctions_AllVariants(t *testing.T) {
	ts := time.Date(2026, 7, 1, 15, 30, 45, 123456000, time.FixedZone("CST", 8*3600))
	s := New(Config{RewriteTimeFunctions: true})

	tests := []struct {
		name string
		sql  string
	}{
		{"now()", "INSERT INTO logs (ts) VALUES (now())"},
		{"NOW() uppercase", "INSERT INTO logs (ts) VALUES (NOW())"},
		{"now() with spaces", "INSERT INTO logs (ts) VALUES (now(  ))"},
		{"current_timestamp", "INSERT INTO logs (ts) VALUES (current_timestamp)"},
		{"CURRENT_TIMESTAMP", "INSERT INTO logs (ts) VALUES (CURRENT_TIMESTAMP)"},
		{"current_date", "SELECT current_date"},
		{"current_time", "SELECT current_time"},
		{"clock_timestamp()", "INSERT INTO logs (ts) VALUES (clock_timestamp())"},
		{"statement_timestamp()", "INSERT INTO logs (ts) VALUES (statement_timestamp())"},
		{"transaction_timestamp()", "INSERT INTO logs (ts) VALUES (transaction_timestamp())"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: ts}
			decision := s.Apply(ev)
			if decision != Rewrite {
				t.Fatalf("expected Rewrite for %q, got %d", tt.sql, decision)
			}
			// Verify original function call is gone
			originalLower := strings.ToLower(tt.sql)
			rewrittenLower := strings.ToLower(ev.SQL)
			for _, fn := range []string{"now(", "current_timestamp", "current_date", "current_time", "clock_timestamp(", "statement_timestamp(", "transaction_timestamp("} {
				if strings.Contains(originalLower, fn) && strings.Contains(rewrittenLower, fn) {
					t.Errorf("time function %q still present in rewritten SQL: %s", fn, ev.SQL)
				}
			}
			// Verify timestamp is embedded: for current_time the output is
			// a time literal (no date component), so check for the timezone
			// cast suffix instead of the year.
			if tt.name == "current_time" {
				if !strings.Contains(ev.SQL, "::timetz") {
					t.Errorf("expected ::timetz cast in rewritten SQL, got: %s", ev.SQL)
				}
			} else if !strings.Contains(ev.SQL, "2026") {
				t.Errorf("expected captured timestamp in rewritten SQL, got: %s", ev.SQL)
			}
			t.Logf("Rewritten: %s", ev.SQL)
		})
	}
}

func TestRewriteTimeFunctions_MultipleOccurrences(t *testing.T) {
	// SQL with multiple time function calls should have ALL replaced
	ts := time.Date(2026, 3, 15, 9, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO audit (created_at, updated_at) VALUES (now(), current_timestamp)",
		Timestamp: ts,
	}
	decision := s.Apply(ev)
	if decision != Rewrite {
		t.Fatalf("expected Rewrite, got %d", decision)
	}

	lower := strings.ToLower(ev.SQL)
	if strings.Contains(lower, "now(") {
		t.Error("now() still present after rewrite")
	}
	if strings.Contains(lower, "current_timestamp") {
		t.Error("current_timestamp still present after rewrite")
	}
	// Count occurrences of the timestamp literal
	count := strings.Count(ev.SQL, "2026-03-15")
	if count != 2 {
		t.Errorf("expected 2 timestamp replacements, got %d in: %s", count, ev.SQL)
	}
	t.Logf("Rewritten: %s", ev.SQL)
}

func TestRewriteTimeFunctions_NoTimestamp(t *testing.T) {
	// If the event has a zero Timestamp, rewriting should NOT occur
	// (to avoid injecting an invalid timestamp)
	s := New(Config{RewriteTimeFunctions: true})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO logs (ts) VALUES (now())",
		Timestamp: time.Time{}, // zero value
	}
	decision := s.Apply(ev)
	if decision == Rewrite {
		t.Error("should not rewrite when Timestamp is zero")
	}
}

func TestRewriteTimeFunctions_PreservesNonTimeSQL(t *testing.T) {
	// SQL without time functions should pass through unchanged
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	sqls := []string{
		"SELECT * FROM orders WHERE id = 1",
		"INSERT INTO foo (name) VALUES ('bar')",
		"UPDATE users SET active = true WHERE email = 'test@example.com'",
		"DELETE FROM sessions WHERE expired = true",
	}

	for _, sql := range sqls {
		ev := &core.SQLEvent{SQL: sql, Timestamp: ts}
		decision := s.Apply(ev)
		if decision != Allow {
			t.Errorf("expected Allow for %q, got %d", sql, decision)
		}
		if ev.SQL != sql {
			t.Errorf("SQL should not be modified: got %q", ev.SQL)
		}
	}
}

func TestRewriteTimeFunctions_InWhereClause(t *testing.T) {
	// Time functions in WHERE clauses should also be rewritten
	ts := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	ev := &core.SQLEvent{
		SQL:       "SELECT * FROM events WHERE created_at > now() - interval '1 hour'",
		Timestamp: ts,
	}
	decision := s.Apply(ev)
	if decision != Rewrite {
		t.Fatalf("expected Rewrite, got %d", decision)
	}
	if strings.Contains(strings.ToLower(ev.SQL), "now(") {
		t.Error("now() still present in WHERE clause")
	}
	t.Logf("Rewritten: %s", ev.SQL)
}

func TestRewriteTimeFunctions_Disabled(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: false})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO logs (ts) VALUES (now())",
		Timestamp: ts,
	}
	decision := s.Apply(ev)
	if decision != Allow {
		t.Errorf("with RewriteTimeFunctions=false, expected Allow, got %d", decision)
	}
	if ev.SQL != "INSERT INTO logs (ts) VALUES (now())" {
		t.Error("SQL should not be modified when RewriteTimeFunctions is disabled")
	}
}

func TestRewriteTimeFunctions_InsideStringLiteral(t *testing.T) {
	// Time function names inside string literals should NOT be rewritten
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	tests := []struct {
		name string
		sql  string
	}{
		{"now() in string", "INSERT INTO logs (msg) VALUES ('we called now() to get time')"},
		{"current_timestamp in string", "INSERT INTO docs (note) VALUES ('use current_timestamp for defaults')"},
		{"mixed: string + comment", "INSERT INTO logs (msg, note) VALUES ('now() is fast', 'current_date works')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: ts}
			decision := s.Apply(ev)
			if decision != Allow {
				t.Errorf("should not rewrite time functions inside string literals, got decision=%d\n  original: %s\n  modified: %s", decision, tt.sql, ev.SQL)
			}
			if ev.SQL != tt.sql {
				t.Errorf("SQL was modified when it shouldn't be:\n  original: %s\n  modified: %s", tt.sql, ev.SQL)
			}
		})
	}
}

func TestRewriteTimeFunctions_MixedRealAndStringLiteral(t *testing.T) {
	// If both a real now() and a string-literal 'now()' appear,
	// only the real one should be rewritten.
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO logs (ts, msg) VALUES (now(), 'called now() at startup')",
		Timestamp: ts,
	}
	decision := s.Apply(ev)
	if decision != Rewrite {
		t.Fatalf("expected Rewrite, got %d", decision)
	}
	// The real now() should be replaced
	if strings.Contains(ev.SQL, "VALUES (now()") {
		t.Error("real now() was not rewritten")
	}
	// The string literal should remain
	if !strings.Contains(ev.SQL, "'called now() at startup'") {
		t.Errorf("string literal was corrupted: %s", ev.SQL)
	}
	t.Logf("Rewritten: %s", ev.SQL)
}

// =============================================================================
// Issue #8: random()/gen_random_uuid() non-reproducible — always replayed as-is
// Non-deterministic functions are always replayed as-is. Skipping them would
// cause missing rows and cascading failures on the shadow DB.
// =============================================================================

func TestNonDeterministic_AlwaysReplayed(t *testing.T) {
	// Even with SkipNonDeterministic enabled, these should pass through
	s := New(Config{SkipNonDeterministic: true})

	tests := []struct {
		name string
		sql  string
	}{
		{"random()", "SELECT random()"},
		{"RANDOM() uppercase", "SELECT RANDOM()"},
		{"gen_random_uuid()", "INSERT INTO users (id) VALUES (gen_random_uuid())"},
		{"GEN_RANDOM_UUID() uppercase", "INSERT INTO users (id) VALUES (GEN_RANDOM_UUID())"},
		{"uuid_generate_v4()", "INSERT INTO users (id) VALUES (uuid_generate_v4())"},
		{"setseed", "SELECT setseed(0.5)"},
		{"random in expression", "INSERT INTO lottery (ticket) VALUES (floor(random() * 1000000))"},
		{"gen_random_uuid in subquery", "INSERT INTO orders SELECT gen_random_uuid(), amount FROM cart"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: time.Now()}
			got := s.Apply(ev)
			if got == Block {
				t.Errorf("non-deterministic SQL should NOT be blocked: %s", tt.sql)
			}
			if ev.SQL != tt.sql {
				t.Errorf("SQL should not be modified.\n  got:  %s\n  want: %s", ev.SQL, tt.sql)
			}
		})
	}
}

func TestNonDeterministic_SQLUnmodified(t *testing.T) {
	// Verify the SQL passes through completely unchanged
	s := New(Config{SkipNonDeterministic: true})

	sqls := []string{
		"INSERT INTO users (id, name) VALUES (gen_random_uuid(), 'test')",
		"UPDATE scores SET val = random() WHERE id = 1",
		"SELECT uuid_generate_v4()",
	}

	for _, sql := range sqls {
		ev := &core.SQLEvent{SQL: sql, Timestamp: time.Now()}
		s.Apply(ev)
		if ev.SQL != sql {
			t.Errorf("SQL modified unexpectedly: %s -> %s", sql, ev.SQL)
		}
	}
}

// =============================================================================
// Combined safeguards: verify interactions between #6, #7, #8
// =============================================================================

func TestCombined_TimeFunctionsAndNonDeterministic(t *testing.T) {
	// Both enabled: non-deterministic SQL with time functions should be
	// rewritten (time part) and allowed through (not blocked).
	s := New(Config{
		RewriteTimeFunctions: true,
		SkipNonDeterministic: true,
	})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO users (id, created_at) VALUES (gen_random_uuid(), now())",
		Timestamp: time.Now(),
	}
	decision := s.Apply(ev)
	if decision == Block {
		t.Error("non-deterministic SQL should never be blocked")
	}
	// now() should be rewritten, gen_random_uuid() left as-is
	if decision != Rewrite {
		t.Errorf("expected Rewrite (for time function), got %d", decision)
	}
	if !strings.Contains(ev.SQL, "gen_random_uuid()") {
		t.Error("gen_random_uuid() should be preserved in SQL")
	}
	if strings.Contains(strings.ToLower(ev.SQL), "now(") {
		t.Error("now() should have been rewritten")
	}
}

func TestCombined_AllSafeguardsEnabled(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{
		ExcludeDDL:           true,
		RewriteSequences:     true,
		RewriteTimeFunctions: true,
		SkipNonDeterministic: true,
		SkipExternalDeps:     true,
	})

	tests := []struct {
		name string
		sql  string
		want Decision
	}{
		{"DDL blocked", "CREATE TABLE foo (id serial)", Block},
		{"random allowed", "SELECT random()", Allow},
		{"uuid allowed", "INSERT INTO t VALUES (gen_random_uuid())", Allow},
		{"dblink blocked", "SELECT dblink('host=x', 'SELECT 1')", Block},
		{"now rewritten", "INSERT INTO logs (ts) VALUES (now())", Rewrite},
		{"plain allowed", "SELECT * FROM orders", Allow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: ts}
			got := s.Apply(ev)
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}
