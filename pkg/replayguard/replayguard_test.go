package replayguard

import (
	"strings"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

func TestDDLFilter(t *testing.T) {
	s := New(Config{ExcludeDDL: true})

	tests := []struct {
		sql  string
		want Decision
	}{
		{"CREATE TABLE foo (id int)", Block},
		{"ALTER TABLE foo ADD COLUMN bar text", Block},
		{"DROP TABLE foo", Block},
		{"TRUNCATE foo", Block},
		{"GRANT SELECT ON foo TO bar", Block},
		{"INSERT INTO foo VALUES (1)", Allow},
		{"SELECT * FROM foo", Allow},
		{"UPDATE foo SET bar = 1", Allow},
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: tt.sql}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("DDL filter %q: got %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestTimeFunctionRewrite(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	ev := &core.SQLEvent{
		SQL:       "INSERT INTO logs (created_at) VALUES (now())",
		Timestamp: ts,
	}
	decision := s.Apply(ev)
	if decision != Rewrite {
		t.Fatalf("expected Rewrite, got %d", decision)
	}
	if ev.SQL == "INSERT INTO logs (created_at) VALUES (now())" {
		t.Fatal("SQL was not rewritten")
	}
	t.Logf("Rewritten SQL: %s", ev.SQL)
}

func TestTimeFunctionRewrite_TypeCorrectness(t *testing.T) {
	ts := time.Date(2026, 7, 1, 10, 30, 45, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	tests := []struct {
		name     string
		sql      string
		contains string
	}{
		{"current_date uses ::date", "SELECT current_date", "'2026-07-01'::date"},
		{"current_time uses ::timetz", "SELECT current_time", "'10:30:45.000000+00:00'::timetz"},
		{"now() uses ::timestamptz", "SELECT now()", "'2026-07-01 10:30:45.000000+00:00'::timestamptz"},
		{"current_timestamp uses ::timestamptz", "SELECT current_timestamp", "'2026-07-01 10:30:45.000000+00:00'::timestamptz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: ts}
			decision := s.Apply(ev)
			if decision != Rewrite {
				t.Fatalf("expected Rewrite, got %d", decision)
			}
			if !strings.Contains(ev.SQL, tt.contains) {
				t.Errorf("expected %q in SQL, got: %s", tt.contains, ev.SQL)
			}
		})
	}
}

func TestNonDeterministicSkip(t *testing.T) {
	// Non-deterministic functions are always replayed as-is, never skipped.
	// Skipping would cause missing rows and data inconsistency on shadow DB.
	s := New(Config{SkipNonDeterministic: true})

	tests := []struct {
		sql  string
		want Decision
	}{
		{"SELECT random()", Allow},
		{"INSERT INTO foo VALUES (gen_random_uuid())", Allow},
		{"SELECT 1", Allow},
		{"INSERT INTO foo VALUES (1)", Allow},
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: tt.sql}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("NonDet %q: got %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestExternalDepsSkip(t *testing.T) {
	s := New(Config{SkipExternalDeps: true})

	tests := []struct {
		sql  string
		want Decision
	}{
		{"SELECT dblink('host=remote', 'SELECT 1')", Block},
		{"SELECT pg_notify('chan', 'msg')", Block},
		{"NOTIFY my_channel", Block},
		{"SELECT * FROM local_table", Allow},
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: tt.sql}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("ExtDep %q: got %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestSelectiveReplay(t *testing.T) {
	s := New(Config{
		IncludeTables: []string{"orders", "users"},
	})

	tests := []struct {
		sql  string
		want Decision
	}{
		{"INSERT INTO orders (id) VALUES (1)", Allow},
		{"SELECT * FROM users WHERE id = 1", Allow},
		{"INSERT INTO audit_log (msg) VALUES ('x')", Block},
		{"DELETE FROM sessions WHERE expired", Block},
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: tt.sql}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("Selective %q: got %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestReadOnlyMode(t *testing.T) {
	s := New(Config{ReadOnly: true})

	tests := []struct {
		sql  string
		want Decision
	}{
		{"SELECT * FROM foo", Allow},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", Allow},
		{"INSERT INTO foo VALUES (1)", Block},
		{"UPDATE foo SET bar = 1", Block},
		{"DELETE FROM foo", Block},
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: tt.sql}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("ReadOnly %q: got %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestBackpressure(t *testing.T) {
	s := New(Config{
		MaxQueueDepth:       10000,
		MaxReplayLagSeconds: 30,
	})

	// Normal state
	if s.CheckBackpressure(5000, 10) {
		t.Error("should not pause under threshold")
	}

	// Queue overflow
	if !s.CheckBackpressure(15000, 10) {
		t.Error("should pause when queue depth exceeds max")
	}

	// Lag overflow
	if !s.CheckBackpressure(5000, 60) {
		t.Error("should pause when replay lag exceeds max")
	}

	// Recovered
	if s.CheckBackpressure(5000, 10) {
		t.Error("should resume when both metrics recover")
	}
}

func TestTransactionLimits(t *testing.T) {
	s := New(Config{
		MaxTransactionDuration: 30 * time.Second,
		MaxStatementsPerTx:     1000,
	})

	// Within limits
	if s.CheckTransaction(time.Now(), 100) != Allow {
		t.Error("should allow within limits")
	}

	// Exceeded statement count
	if s.CheckTransaction(time.Now(), 1500) != Block {
		t.Error("should block when statement count exceeded")
	}

	// Exceeded duration
	past := time.Now().Add(-60 * time.Second)
	if s.CheckTransaction(past, 100) != Block {
		t.Error("should block when duration exceeded")
	}
}

func TestDataMasking(t *testing.T) {
	s := New(Config{
		MaskPatterns: []MaskRule{
			{Pattern: `'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}'`, Replacement: "'****-****-****-****'"},
			{Pattern: `'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'`, Replacement: "'***@***.***'"},
		},
	})

	ev := &core.SQLEvent{
		SQL: "INSERT INTO payments (card, email) VALUES ('1234-5678-9012-3456', 'user@example.com')",
	}
	decision := s.Apply(ev)
	if decision != Rewrite {
		t.Fatalf("expected Rewrite, got %d", decision)
	}
	if ev.SQL == "INSERT INTO payments (card, email) VALUES ('1234-5678-9012-3456', 'user@example.com')" {
		t.Fatal("SQL was not masked")
	}
	t.Logf("Masked SQL: %s", ev.SQL)
}

func TestPercentageReplay(t *testing.T) {
	s := New(Config{ReplayPercentage: 50})

	allowed := 0
	total := 200
	for i := 0; i < total; i++ {
		ev := &core.SQLEvent{SQL: "INSERT INTO foo VALUES (1)"}
		if s.Apply(ev) == Allow {
			allowed++
		}
	}
	// Should be roughly 50% ± some tolerance
	if allowed < 80 || allowed > 120 {
		t.Errorf("expected ~100 allowed out of 200, got %d", allowed)
	}
}

func TestIncludeUsers(t *testing.T) {
	s := New(Config{
		IncludeUsers: []string{"app_user", "Admin"},
	})

	tests := []struct {
		user string
		want Decision
	}{
		{"app_user", Allow},
		{"APP_USER", Allow},    // case-insensitive
		{"Admin", Allow},
		{"admin", Allow},       // case-insensitive
		{"other_user", Block},
		{"", Block},            // empty user blocked when filter is set
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: "SELECT 1", User: tt.user}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("IncludeUsers user=%q: got %d, want %d", tt.user, got, tt.want)
		}
	}
}

func TestIncludeUsersEmpty(t *testing.T) {
	// When IncludeUsers is empty, all users are allowed (no filtering)
	s := New(Config{})

	ev := &core.SQLEvent{SQL: "SELECT 1", User: "any_user"}
	if got := s.Apply(ev); got != Allow {
		t.Errorf("empty IncludeUsers should allow all, got %d", got)
	}
}
