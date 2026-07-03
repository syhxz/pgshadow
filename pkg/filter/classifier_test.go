package filter

import "testing"

func className(c StmtClass) string {
	switch c {
	case ClassUnknown:
		return "Unknown"
	case ClassPlainSelect:
		return "PlainSelect"
	case ClassProcSelect:
		return "ProcSelect"
	case ClassCall:
		return "Call"
	case ClassDML:
		return "DML"
	case ClassDDL:
		return "DDL"
	case ClassBegin:
		return "Begin"
	case ClassCommitRollback:
		return "CommitRollback"
	case ClassUtility:
		return "Utility"
	case ClassCopyFrom:
		return "CopyFrom"
	default:
		return "?"
	}
}

func TestClassify_LeadingKeyword(t *testing.T) {
	c := NewClassifier(nil)
	cases := []struct {
		sql  string
		want StmtClass
	}{
		// CALL
		{"CALL my_proc()", ClassCall},
		{"call foo(1, 2)", ClassCall},
		// DML
		{"INSERT INTO t VALUES (1)", ClassDML},
		{"insert into t (a) values (1)", ClassDML},
		{"UPDATE t SET a = 1 WHERE id = 2", ClassDML},
		{"DELETE FROM t WHERE id = 1", ClassDML},
		// DDL
		{"CREATE TABLE t (id int)", ClassDDL},
		{"ALTER TABLE t ADD COLUMN c int", ClassDDL},
		{"DROP TABLE t", ClassDDL},
		{"create index idx on t (a)", ClassDDL},
		// transaction control
		{"BEGIN", ClassBegin},
		{"begin transaction", ClassBegin},
		{"START TRANSACTION", ClassBegin},
		{"COMMIT", ClassCommitRollback},
		{"commit", ClassCommitRollback},
		{"ROLLBACK", ClassCommitRollback},
		{"END", ClassCommitRollback},
		// utility
		{"SET search_path = public", ClassUtility},
		{"RESET ALL", ClassUtility},
		{"DISCARD ALL", ClassUtility},
		// COPY
		{"COPY t FROM STDIN", ClassCopyFrom},
		{"copy t (a, b) from stdin with csv", ClassCopyFrom},
		{"COPY t TO STDOUT", ClassUnknown},
		{"COPY (SELECT * FROM t) TO STDOUT", ClassUnknown},
		// unknown / empty
		{"", ClassUnknown},
		{"   ", ClassUnknown},
		{"VACUUM", ClassUnknown},
		{"-- just a comment\n", ClassUnknown},
		{"/* block */", ClassUnknown},
	}
	for _, tc := range cases {
		got := c.Classify(tc.sql)
		if got != tc.want {
			t.Errorf("Classify(%q) = %s, want %s", tc.sql, className(got), className(tc.want))
		}
	}
}

func TestClassify_SelectHeuristic(t *testing.T) {
	c := NewClassifier(nil)
	cases := []struct {
		sql  string
		want StmtClass
	}{
		// Plain SELECTs (no UDF invocation).
		{"SELECT col FROM tbl", ClassPlainSelect},
		{"SELECT * FROM tbl WHERE id = 1", ClassPlainSelect},
		{"SELECT 1", ClassPlainSelect},
		// Built-in / aggregate functions are NOT proc invocations.
		{"SELECT now()", ClassPlainSelect},
		{"SELECT NOW()", ClassPlainSelect},
		{"SELECT count(*) FROM t", ClassPlainSelect},
		{"SELECT a, count(*) FROM t WHERE id IN (1,2,3) GROUP BY a", ClassPlainSelect},
		{"SELECT * FROM generate_series(1, 10)", ClassPlainSelect},
		{"SELECT coalesce(a, 0), row_number() OVER () FROM t", ClassPlainSelect},
		{"SELECT (SELECT max(x) FROM t2) FROM t1", ClassPlainSelect},
		// Subquery / IN / VALUES parentheses are not calls.
		{"SELECT * FROM (SELECT 1) s", ClassPlainSelect},
		{"SELECT 'hello(world)' AS lit", ClassPlainSelect},
		{"SELECT cast(a AS numeric(10,2)) FROM t", ClassPlainSelect},
		// WITH that resolves to a plain SELECT.
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", ClassPlainSelect},
		{"with recursive r as (select 1) select * from r", ClassPlainSelect},

		// Procedure-invoking SELECTs (user-defined function/proc).
		{"SELECT my_func(1)", ClassProcSelect},
		{"select my_func(1, 2)", ClassProcSelect},
		{"SELECT * FROM my_proc(1, 2)", ClassProcSelect},
		{"SELECT schema.do_work()", ClassProcSelect},
		{"SELECT do_it ()", ClassProcSelect}, // whitespace before paren
		{"SELECT batch_job() FROM dual", ClassProcSelect},
		// WITH that resolves to a proc-invoking SELECT.
		{"WITH x AS (SELECT do_batch()) SELECT * FROM x", ClassProcSelect},
		// case-insensitive keyword + UDF
		{"  \n\t sElEcT Run_Batch(42)", ClassProcSelect},
	}
	for _, tc := range cases {
		got := c.Classify(tc.sql)
		if got != tc.want {
			t.Errorf("Classify(%q) = %s, want %s", tc.sql, className(got), className(tc.want))
		}
	}
}

func TestClassify_WithDML(t *testing.T) {
	c := NewClassifier(nil)
	cases := []struct {
		sql  string
		want StmtClass
	}{
		{"WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x", ClassDML},
		{"WITH d AS (DELETE FROM a RETURNING *) INSERT INTO b SELECT * FROM d", ClassDML},
	}
	for _, tc := range cases {
		got := c.Classify(tc.sql)
		if got != tc.want {
			t.Errorf("Classify(%q) = %s, want %s", tc.sql, className(got), className(tc.want))
		}
	}
}

// TestClassify_WithDataModifyingCTE documents that a WITH whose data-modifying
// statement is nested in a CTE while the OUTER (primary) command is SELECT is
// classified by its primary command (SELECT). R5.3's heuristic addresses
// function/procedure invocation, not data-modifying CTEs.
func TestClassify_WithDataModifyingCTE(t *testing.T) {
	c := NewClassifier(nil)
	sql := "WITH u AS (UPDATE a SET x = 1 RETURNING *) SELECT * FROM u"
	if got := c.Classify(sql); got != ClassPlainSelect {
		t.Errorf("Classify(%q) = %s, want PlainSelect (outer command is SELECT)", sql, className(got))
	}
}

func TestIsProcInvokingSelect(t *testing.T) {
	c := NewClassifier(nil)
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT f()", true},
		{"SELECT * FROM proc(1)", true},
		{"SELECT schema.do_work(1)", true},
		{"SELECT now()", false},
		{"SELECT count(*) FROM t", false},
		{"SELECT * FROM generate_series(1,10)", false},
		{"SELECT col FROM tbl", false},
		{"WITH c AS (SELECT 1) SELECT * FROM cte", false},
		// Non-SELECT statements are never proc-invoking SELECTs.
		{"INSERT INTO t SELECT f()", false},
		{"CALL f()", false},
		{"UPDATE t SET a = f()", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := c.IsProcInvokingSelect(tc.sql); got != tc.want {
			t.Errorf("IsProcInvokingSelect(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

func TestClassify_ConfigurableBuiltins(t *testing.T) {
	// Extra builtins are additive to the baseline set: "myfunc" becomes
	// excluded (Plain), while defaults like now() remain excluded, and an
	// unlisted user function is still a proc invocation.
	c := NewClassifier([]string{"myfunc"})

	if got := c.Classify("SELECT myfunc(1)"); got != ClassPlainSelect {
		t.Errorf("Classify(SELECT myfunc(1)) with extra builtins = %s, want PlainSelect", className(got))
	}
	if got := c.Classify("SELECT now()"); got != ClassPlainSelect {
		t.Errorf("Classify(SELECT now()) = %s, want PlainSelect (baseline builtin)", className(got))
	}
	if got := c.Classify("SELECT other_fn()"); got != ClassProcSelect {
		t.Errorf("Classify(SELECT other_fn()) = %s, want ProcSelect", className(got))
	}
}

func TestClassify_CommentsAndDollarQuotes(t *testing.T) {
	c := NewClassifier(nil)
	cases := []struct {
		sql  string
		want StmtClass
	}{
		{"/* leading */ SELECT count(*) FROM t", ClassPlainSelect},
		{"-- line comment\nSELECT do_thing()", ClassProcSelect},
		{"SELECT $$text with ( paren $$ AS x", ClassPlainSelect},
		{"SELECT $tag$ body(x) $tag$ AS x", ClassPlainSelect},
	}
	for _, tc := range cases {
		got := c.Classify(tc.sql)
		if got != tc.want {
			t.Errorf("Classify(%q) = %s, want %s", tc.sql, className(got), className(tc.want))
		}
	}
}
