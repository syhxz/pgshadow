package filter

import (
	"regexp"
	"testing"

	"pgshadow/pkg/core"
)

// mustRegexp wraps a compiled pattern in the YAML Regexp type for tests.
func mustRegexp(t *testing.T, pat string) Regexp {
	t.Helper()
	re, err := regexp.Compile(pat)
	if err != nil {
		t.Fatalf("compile %q: %v", pat, err)
	}
	return Regexp{Regexp: re}
}

func actionName(a Action) string {
	switch a {
	case Keep:
		return "Keep"
	case Drop:
		return "Drop"
	default:
		return "?"
	}
}

// decideSQL is a small helper that builds an event and runs Decide.
func decideSQL(t *testing.T, f Filter, sql string, state core.TxStatus) (Action, StmtClass) {
	t.Helper()
	return f.Decide(&core.SQLEvent{SQL: sql}, state)
}

func mustFilter(t *testing.T, cfg Config) Filter {
	t.Helper()
	f, err := NewFilter(cfg)
	if err != nil {
		t.Fatalf("NewFilter(%+v): %v", cfg, err)
	}
	return f
}

// TestResolveRules_InvalidMode verifies an unsupported mode is rejected.
func TestResolveRules_InvalidMode(t *testing.T) {
	if _, err := NewFilter(Config{Mode: "bogus"}); err == nil {
		t.Fatal("NewFilter with invalid mode: expected error, got nil")
	}
}

// TestDecide_DefaultMatrix exercises the default (empty-mode) rule matrix
// across transaction states for every statement class. It is the core
// decision-matrix table per Requirement 5.
func TestDecide_DefaultMatrix(t *testing.T) {
	f := mustFilter(t, Config{}) // empty mode == custom with no overrides

	cases := []struct {
		name    string
		sql     string
		inTx    Action // expected action when TxInTx
		outTx   Action // expected action when TxIdle / TxFailed
		wantCls StmtClass
	}{
		// Plain_SELECT: keep in-tx (R5.7), drop out-of-tx (R5.6).
		{"plain select", "SELECT a FROM t", Keep, Drop, ClassPlainSelect},
		// Procedure_Invoking_SELECT: keep regardless (R5.4).
		{"proc select", "SELECT do_batch(1)", Keep, Keep, ClassProcSelect},
		// CALL: keep regardless (R5.5).
		{"call", "CALL run_job()", Keep, Keep, ClassCall},
		// DML: keep regardless (R5.8).
		{"insert", "INSERT INTO t VALUES (1)", Keep, Keep, ClassDML},
		{"update", "UPDATE t SET a=1", Keep, Keep, ClassDML},
		{"delete", "DELETE FROM t", Keep, Keep, ClassDML},
		// DDL: keep regardless (R5.9).
		{"create", "CREATE TABLE t (id int)", Keep, Keep, ClassDDL},
		{"alter", "ALTER TABLE t ADD c int", Keep, Keep, ClassDDL},
		{"drop", "DROP TABLE t", Keep, Keep, ClassDDL},
		// BEGIN: keep (R5.10).
		{"begin", "BEGIN", Keep, Keep, ClassBegin},
		// COMMIT/ROLLBACK: keep (R5.11).
		{"commit", "COMMIT", Keep, Keep, ClassCommitRollback},
		{"rollback", "ROLLBACK", Keep, Keep, ClassCommitRollback},
		// COPY ... FROM: keep (R5.14).
		{"copy from", "COPY t FROM STDIN", Keep, Keep, ClassCopyFrom},
		// Utility: default keep (R5.13).
		{"set", "SET search_path = public", Keep, Keep, ClassUtility},
		// Indeterminate: always drop (R5.17).
		{"unknown", "VACUUM", Drop, Drop, ClassUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotIn, clsIn := decideSQL(t, f, tc.sql, core.TxInTx)
			if gotIn != tc.inTx {
				t.Errorf("in-tx Decide(%q) = %s, want %s", tc.sql, actionName(gotIn), actionName(tc.inTx))
			}
			if clsIn != tc.wantCls {
				t.Errorf("class(%q) = %s, want %s", tc.sql, className(clsIn), className(tc.wantCls))
			}
			gotIdle, _ := decideSQL(t, f, tc.sql, core.TxIdle)
			if gotIdle != tc.outTx {
				t.Errorf("idle Decide(%q) = %s, want %s", tc.sql, actionName(gotIdle), actionName(tc.outTx))
			}
			// Failed transaction is routed identically to Idle (out-of-tx).
			gotFailed, _ := decideSQL(t, f, tc.sql, core.TxFailed)
			if gotFailed != tc.outTx {
				t.Errorf("failed Decide(%q) = %s, want %s", tc.sql, actionName(gotFailed), actionName(tc.outTx))
			}
		})
	}
}

// TestDecide_Precedence_ExcludeWins verifies exclude patterns drop a statement
// that the matrix would otherwise keep, including always-keep classes (R5.16
// has the highest precedence in the decision flow).
func TestDecide_Precedence_ExcludeWins(t *testing.T) {
	f := mustFilter(t, Config{
		ExcludePatterns: []Regexp{mustRegexp(t, `(?i)pg_catalog`)},
	})
	// INSERT is always-keep, but the exclude pattern matches → Drop.
	if got, _ := decideSQL(t, f, "INSERT INTO pg_catalog.x VALUES (1)", core.TxInTx); got != Drop {
		t.Errorf("excluded INSERT = %s, want Drop", actionName(got))
	}
	// A non-matching INSERT is still kept.
	if got, _ := decideSQL(t, f, "INSERT INTO app.x VALUES (1)", core.TxInTx); got != Keep {
		t.Errorf("non-excluded INSERT = %s, want Keep", actionName(got))
	}
}

// TestDecide_IncludePattern_OutranksOutOfTxDrop verifies an include pattern
// keeps a Plain_SELECT out of transaction that would otherwise be dropped
// (R5.12 outranks R5.6).
func TestDecide_IncludePattern_OutranksOutOfTxDrop(t *testing.T) {
	f := mustFilter(t, Config{
		IncludePatterns: []Regexp{mustRegexp(t, `(?i)from\s+critical_view`)},
	})
	sql := "SELECT * FROM critical_view"
	if got, cls := decideSQL(t, f, sql, core.TxIdle); got != Keep || cls != ClassPlainSelect {
		t.Errorf("included plain SELECT out-of-tx = (%s,%s), want (Keep,PlainSelect)", actionName(got), className(cls))
	}
	// A plain SELECT not matching the include pattern is dropped out-of-tx.
	if got, _ := decideSQL(t, f, "SELECT * FROM other", core.TxIdle); got != Drop {
		t.Errorf("non-included plain SELECT out-of-tx = %s, want Drop", actionName(got))
	}
}

// TestDecide_ReplayableProcedure_OutranksOutOfTxDrop verifies a SELECT that
// invokes a configured replayable procedure is kept out of transaction (R5.12).
func TestDecide_ReplayableProcedure_OutranksOutOfTxDrop(t *testing.T) {
	f := mustFilter(t, Config{
		ReplayableProcedures: []string{"refresh_cache", "schema.do_work"},
	})
	cases := []struct {
		sql  string
		want Action
	}{
		{"SELECT refresh_cache()", Keep},         // bare name
		{"SELECT schema.do_work(1)", Keep},       // qualified name
		{"SELECT do_work(1)", Keep},              // last-segment match
		{"SELECT other_fn()", Keep},              // proc-invoking select keeps via matrix (R5.4)
		{"SELECT col FROM t", Drop},              // plain select, not replayable, out-of-tx
		{"SELECT '/* refresh_cache() */'", Drop}, // literal mention is not an invocation
	}
	for _, tc := range cases {
		if got, _ := decideSQL(t, f, tc.sql, core.TxIdle); got != tc.want {
			t.Errorf("Decide(%q) out-of-tx = %s, want %s", tc.sql, actionName(got), actionName(tc.want))
		}
	}
}

// TestDecide_PlainSelectConfigurable verifies the Plain_SELECT in/out actions
// are configurable via custom rules (R5.6, R5.7, R11.4).
func TestDecide_PlainSelectConfigurable(t *testing.T) {
	// Invert the defaults: drop in-tx, keep out-of-tx.
	f := mustFilter(t, Config{
		Mode: "custom",
		Rules: map[StmtClass]Rule{
			ClassPlainSelect: {InTransaction: Drop, OutTransaction: Keep},
		},
	})
	if got, _ := decideSQL(t, f, "SELECT 1", core.TxInTx); got != Drop {
		t.Errorf("plain SELECT in-tx (custom) = %s, want Drop", actionName(got))
	}
	if got, _ := decideSQL(t, f, "SELECT 1", core.TxIdle); got != Keep {
		t.Errorf("plain SELECT out-of-tx (custom) = %s, want Keep", actionName(got))
	}
	// Always-keep classes are unaffected by the override.
	if got, _ := decideSQL(t, f, "INSERT INTO t VALUES (1)", core.TxIdle); got != Keep {
		t.Errorf("INSERT (custom) = %s, want Keep", actionName(got))
	}
}

// TestDecide_UtilityConfigurable verifies SET/DISCARD/RESET keep-or-drop is
// configurable (R5.13).
func TestDecide_UtilityConfigurable(t *testing.T) {
	fDrop := mustFilter(t, Config{
		Mode:  "custom",
		Rules: map[StmtClass]Rule{ClassUtility: {InTransaction: Drop, OutTransaction: Drop}},
	})
	for _, sql := range []string{"SET search_path = x", "RESET ALL", "DISCARD ALL"} {
		if got, _ := decideSQL(t, fDrop, sql, core.TxIdle); got != Drop {
			t.Errorf("utility %q with drop-rule = %s, want Drop", sql, actionName(got))
		}
	}
	// Default keeps utilities.
	fKeep := mustFilter(t, Config{})
	if got, _ := decideSQL(t, fKeep, "SET x = 1", core.TxIdle); got != Keep {
		t.Errorf("utility default = %s, want Keep", actionName(got))
	}
}

// TestDecide_ModePresets verifies each mode preset's keep/drop behavior
// (R5.15). State is varied where it matters.
func TestDecide_ModePresets(t *testing.T) {
	type expect struct {
		sql   string
		state core.TxStatus
		want  Action
	}
	tests := []struct {
		mode    string
		expects []expect
	}{
		{
			mode: "write_only",
			expects: []expect{
				{"INSERT INTO t VALUES (1)", core.TxIdle, Keep},
				{"CREATE TABLE t (id int)", core.TxIdle, Keep},
				{"CALL p()", core.TxIdle, Keep},
				{"SELECT a FROM t", core.TxInTx, Drop}, // plain select dropped even in-tx
				{"SELECT a FROM t", core.TxIdle, Drop},
				{"SELECT do_work()", core.TxIdle, Keep}, // proc-invoking still kept
			},
		},
		{
			mode: "all",
			expects: []expect{
				{"SELECT a FROM t", core.TxIdle, Keep}, // plain select kept out-of-tx in 'all'
				{"INSERT INTO t VALUES (1)", core.TxIdle, Keep},
				{"CREATE TABLE t (id int)", core.TxIdle, Keep},
				{"SET x = 1", core.TxIdle, Keep},
			},
		},
		{
			mode: "ddl_only",
			expects: []expect{
				{"CREATE TABLE t (id int)", core.TxIdle, Keep},
				{"ALTER TABLE t ADD c int", core.TxInTx, Keep},
				{"INSERT INTO t VALUES (1)", core.TxIdle, Drop},
				{"SELECT a FROM t", core.TxInTx, Drop},
				{"CALL p()", core.TxIdle, Drop},
			},
		},
		{
			mode: "dml_only",
			expects: []expect{
				{"INSERT INTO t VALUES (1)", core.TxIdle, Keep},
				{"UPDATE t SET a=1", core.TxInTx, Keep},
				{"DELETE FROM t", core.TxIdle, Keep},
				{"CREATE TABLE t (id int)", core.TxIdle, Drop},
				{"SELECT a FROM t", core.TxInTx, Drop},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			f := mustFilter(t, Config{Mode: tt.mode})
			for _, e := range tt.expects {
				if got, _ := decideSQL(t, f, e.sql, e.state); got != e.want {
					t.Errorf("[%s] Decide(%q, %v) = %s, want %s",
						tt.mode, e.sql, e.state, actionName(got), actionName(e.want))
				}
			}
		})
	}
}

// TestDecide_ModePresets_ExcludeStillApplies verifies that even in a preset
// mode the exclude pattern keeps its highest precedence (R5.16).
func TestDecide_ModePresets_ExcludeStillApplies(t *testing.T) {
	f := mustFilter(t, Config{
		Mode:            "dml_only",
		ExcludePatterns: []Regexp{mustRegexp(t, `(?i)temp_`)},
	})
	if got, _ := decideSQL(t, f, "INSERT INTO temp_x VALUES (1)", core.TxIdle); got != Drop {
		t.Errorf("excluded INSERT in dml_only = %s, want Drop", actionName(got))
	}
	if got, _ := decideSQL(t, f, "INSERT INTO real_x VALUES (1)", core.TxIdle); got != Keep {
		t.Errorf("non-excluded INSERT in dml_only = %s, want Keep", actionName(got))
	}
}

// TestDecide_MetricsHook verifies the classification hook is invoked once per
// Decide with the resolved class and kept flag, and that indeterminate
// statements are recorded as dropped (R5.17, metrics decoupling seam).
func TestDecide_MetricsHook(t *testing.T) {
	type rec struct {
		class StmtClass
		kept  bool
	}
	var recs []rec
	f, err := NewFilterWithHook(Config{}, func(class StmtClass, kept bool) {
		recs = append(recs, rec{class, kept})
	})
	if err != nil {
		t.Fatalf("NewFilterWithHook: %v", err)
	}

	decideSQL(t, f, "INSERT INTO t VALUES (1)", core.TxIdle) // kept DML
	decideSQL(t, f, "SELECT a FROM t", core.TxIdle)          // dropped plain select out-of-tx
	decideSQL(t, f, "VACUUM", core.TxIdle)                   // indeterminate → drop + record

	want := []rec{
		{ClassDML, true},
		{ClassPlainSelect, false},
		{ClassUnknown, false},
	}
	if len(recs) != len(want) {
		t.Fatalf("hook called %d times, want %d (%+v)", len(recs), len(want), recs)
	}
	for i := range want {
		if recs[i] != want[i] {
			t.Errorf("hook[%d] = {%s,%v}, want {%s,%v}",
				i, className(recs[i].class), recs[i].kept,
				className(want[i].class), want[i].kept)
		}
	}
}

// TestDecide_NilHookSafe verifies Decide does not panic without a hook.
func TestDecide_NilHookSafe(t *testing.T) {
	f := mustFilter(t, Config{})
	if got, _ := decideSQL(t, f, "INSERT INTO t VALUES (1)", core.TxIdle); got != Keep {
		t.Errorf("nil-hook Decide = %s, want Keep", actionName(got))
	}
}

// TestDecide_IndeterminateAlwaysDropped covers a few non-classifiable inputs to
// confirm the indeterminate drop (R5.17) regardless of transaction state.
func TestDecide_IndeterminateAlwaysDropped(t *testing.T) {
	f := mustFilter(t, Config{Mode: "all"}) // 'all' keeps everything classifiable
	for _, sql := range []string{"VACUUM", "ANALYZE t", "", "   ", "EXPLAIN SELECT 1"} {
		for _, st := range []core.TxStatus{core.TxIdle, core.TxInTx, core.TxFailed} {
			if got, cls := decideSQL(t, f, sql, st); got != Drop || cls != ClassUnknown {
				t.Errorf("Decide(%q, %v) = (%s,%s), want (Drop,Unknown)",
					sql, st, actionName(got), className(cls))
			}
		}
	}
}
