package replayguard

import (
	"strings"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

func TestSetTimezonePassThrough(t *testing.T) {
	s := New(Config{
		ExcludeDDL:           true,
		SkipExternalDeps:     true,
		RewriteTimeFunctions: true,
	})
	ev := &core.SQLEvent{SQL: "SET timezone = 'Asia/Shanghai'", Timestamp: time.Now()}
	got := s.Apply(ev)
	if got == Block {
		t.Errorf("SET timezone should not be blocked, got Block")
	}
}

func TestSessionCommandsPassTableFilter(t *testing.T) {
	// Session-state commands must pass through even when IncludeTables is set,
	// otherwise the shadow DB's session state diverges from production (#7).
	s := New(Config{
		IncludeTables: []string{"orders"},
	})

	tests := []struct {
		sql  string
		want Decision
	}{
		{"SET timezone = 'Asia/Shanghai'", Allow},
		{"SET search_path TO myschema, public", Allow},
		{"SET statement_timeout = '5s'", Allow},
		{"RESET ALL", Allow},
		{"DISCARD ALL", Allow},
		// Regular queries still filtered
		{"INSERT INTO audit (msg) VALUES ('x')", Block},
		{"INSERT INTO orders (id) VALUES (1)", Allow},
	}
	for _, tt := range tests {
		ev := &core.SQLEvent{SQL: tt.sql}
		if got := s.Apply(ev); got != tt.want {
			t.Errorf("SessionCommand %q: got %d, want %d", tt.sql, got, tt.want)
		}
	}
}

func TestDollarQuotedStringNotRewritten(t *testing.T) {
	// Time functions inside dollar-quoted strings (e.g. function bodies)
	// should NOT be rewritten (#5).
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	s := New(Config{RewriteTimeFunctions: true})

	tests := []struct {
		name string
		sql  string
		want Decision
	}{
		{
			"now() inside $$...$$",
			"SELECT $$ INSERT INTO logs VALUES (now()) $$",
			Allow, // no rewrite because now() is inside dollar-quotes
		},
		{
			"now() inside $fn$...$fn$",
			"SELECT $fn$ now() $fn$",
			Allow,
		},
		{
			"now() outside dollar-quote",
			"SELECT now(), $$ literal $$",
			Rewrite,
		},
		{
			"now() both inside and outside",
			"SELECT now(), $$ now() $$",
			Rewrite, // only the outer one is rewritten
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := &core.SQLEvent{SQL: tt.sql, Timestamp: ts}
			got := s.Apply(ev)
			if got != tt.want {
				t.Errorf("got decision %d, want %d. SQL: %s", got, tt.want, ev.SQL)
			}
			if tt.want == Allow {
				// SQL should be unchanged
				if ev.SQL != tt.sql {
					t.Errorf("SQL was modified when it should not have been: %s", ev.SQL)
				}
			}
			if tt.want == Rewrite {
				// The dollar-quoted content should be preserved
				if strings.Contains(tt.sql, "$$") {
					// Check the dollar-quoted part is unchanged
					if !strings.Contains(ev.SQL, "$$") {
						t.Errorf("dollar-quote markers removed from: %s", ev.SQL)
					}
				}
			}
		})
	}
}

func TestStripStringLiterals_DollarQuote(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			"simple dollar-quote",
			"SELECT $$ hello world $$, 1",
			"SELECT $$$$, 1",
		},
		{
			"tagged dollar-quote",
			"SELECT $fn$ now() $fn$, 1",
			"SELECT $fn$$fn$, 1",
		},
		{
			"mixed single and dollar-quote",
			"SELECT 'text', $$ body $$",
			"SELECT '', $$$$",
		},
		{
			"no dollar-quote",
			"SELECT 1 + $1",
			"SELECT 1 + $1", // $1 is a parameter reference, not a dollar-quote
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripStringLiterals(tt.sql)
			if got != tt.want {
				t.Errorf("stripStringLiterals(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}
