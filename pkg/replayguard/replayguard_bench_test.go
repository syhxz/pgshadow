package replayguard

import (
	"fmt"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// BenchmarkApply_Allow measures the hot path: a simple DML that passes all checks.
func BenchmarkApply_Allow(b *testing.B) {
	sg := New(Config{
		ExcludeDDL:           true,
		RewriteTimeFunctions: true,
		SkipExternalDeps:     true,
	})
	ev := &core.SQLEvent{
		SQL:       "INSERT INTO orders (name, amount) VALUES ('test', 99.99)",
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev.SQL = "INSERT INTO orders (name, amount) VALUES ('test', 99.99)"
		sg.Apply(ev)
	}
}

// BenchmarkApply_Rewrite measures the time function rewriting path.
func BenchmarkApply_Rewrite(b *testing.B) {
	sg := New(Config{
		ExcludeDDL:           true,
		RewriteTimeFunctions: true,
		SkipExternalDeps:     true,
	})
	ts := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &core.SQLEvent{
			SQL:       "INSERT INTO audit (action, ts) VALUES ('login', now())",
			Timestamp: ts,
		}
		sg.Apply(ev)
	}
}

// BenchmarkApply_RewriteComplex measures rewriting with string literals present.
func BenchmarkApply_RewriteComplex(b *testing.B) {
	sg := New(Config{
		ExcludeDDL:           true,
		RewriteTimeFunctions: true,
		SkipExternalDeps:     true,
	})
	ts := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &core.SQLEvent{
			SQL:       "INSERT INTO logs (ts, msg) VALUES (now(), 'called now() at startup with current_timestamp info')",
			Timestamp: ts,
		}
		sg.Apply(ev)
	}
}

// BenchmarkApply_Block_DDL measures the fast-exit DDL block path.
func BenchmarkApply_Block_DDL(b *testing.B) {
	sg := New(Config{ExcludeDDL: true})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &core.SQLEvent{SQL: "CREATE TABLE foo (id int)"}
		sg.Apply(ev)
	}
}

// BenchmarkApply_TableFilter measures the table include filter.
func BenchmarkApply_TableFilter(b *testing.B) {
	sg := New(Config{
		IncludeTables: []string{"orders", "users", "products", "inventory"},
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &core.SQLEvent{SQL: "INSERT INTO orders (id) VALUES (1)"}
		sg.Apply(ev)
	}
}

// BenchmarkApply_AllSafeguards measures with all safeguards enabled.
func BenchmarkApply_AllSafeguards(b *testing.B) {
	sg := New(Config{
		ExcludeDDL:           true,
		RewriteTimeFunctions: true,
		SkipExternalDeps:     true,
		IncludeTables:        []string{"orders", "users", "audit"},
		MaskPatterns: []MaskRule{
			{Pattern: `'[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}'`, Replacement: "'****'"},
		},
	})
	ts := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &core.SQLEvent{
			SQL:       "INSERT INTO orders (name, amount) VALUES ('test', 99.99)",
			Timestamp: ts,
		}
		sg.Apply(ev)
	}
}

// BenchmarkStripStringLiterals measures the string literal stripping overhead.
func BenchmarkStripStringLiterals(b *testing.B) {
	sql := "INSERT INTO logs (ts, msg, detail) VALUES (now(), 'called now() at startup', 'current_timestamp is useful for auditing purposes')"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stripStringLiterals(sql)
	}
}

// BenchmarkReplaceOutsideStrings measures the string-aware replacement.
func BenchmarkReplaceOutsideStrings(b *testing.B) {
	sg := New(Config{RewriteTimeFunctions: true})
	sql := "INSERT INTO logs (ts, msg) VALUES (now(), 'we use now() for timestamps')"
	replacement := "'2026-07-01 10:00:00.000000+00:00'::timestamptz"
	pat := sg.timePatterns[0] // now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		replaceOutsideStrings(sql, pat, replacement)
	}
}

// BenchmarkApply_LongSQL measures performance on a realistic long SQL statement.
func BenchmarkApply_LongSQL(b *testing.B) {
	sg := New(Config{
		ExcludeDDL:           true,
		RewriteTimeFunctions: true,
		SkipExternalDeps:     true,
	})
	// Generate a long INSERT with many values
	sql := "INSERT INTO events (user_id, action, created_at, metadata) VALUES "
	for i := 0; i < 100; i++ {
		if i > 0 {
			sql += ", "
		}
		sql += fmt.Sprintf("(%d, 'action_%d', now(), '{\"key\": \"value_%d\"}')", i, i, i)
	}
	ts := time.Now()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := &core.SQLEvent{SQL: sql, Timestamp: ts}
		sg.Apply(ev)
	}
}
