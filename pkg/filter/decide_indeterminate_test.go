package filter

import (
	"testing"

	"pgshadow/pkg/core"
)

// This file complements decide_test.go's TestDecide_IndeterminateAlwaysDropped
// (task 5.13, R5.17). Where that test asserts the structural drop of
// indeterminate statements, the tests here focus on the *metrics-hook
// recording* of indeterminate classifications and exercise a broader set of
// unknown-class inputs across every transaction state. Together they assert
// that statements classified as ClassUnknown are dropped AND that the
// indeterminate-classification event is observed by the metrics hook
// (recorded as kept=false) for each input and state.

// indeterminateInputs is a broad set of statements that the classifier resolves
// to ClassUnknown: maintenance/utility commands that are not on the keep matrix
// (VACUUM, ANALYZE, EXPLAIN, TRUNCATE, GRANT, REINDEX, COMMENT, COPY ... TO),
// empty / whitespace-only input, and random gibberish.
var indeterminateInputs = []string{
	"VACUUM",
	"VACUUM FULL t",
	"ANALYZE t",
	"EXPLAIN SELECT 1",
	"EXPLAIN ANALYZE SELECT 1",
	"TRUNCATE t",
	"GRANT SELECT ON t TO bob",
	"REVOKE SELECT ON t FROM bob",
	"REINDEX TABLE t",
	"COMMENT ON TABLE t IS 'x'",
	"CLUSTER t",
	"LISTEN chan",
	"NOTIFY chan",
	"COPY t TO STDOUT", // COPY ... TO is a read, not ClassCopyFrom (R5.14)
	"",
	"   ",
	"\t\n  ",
	"asdf qwer zxcv",
	"-- just a comment",
	"/* block comment only */",
	"42",
	"???",
}

// allTxStates is every transaction state the decision flow distinguishes for
// out-of-tx routing; Idle and Failed are routed identically to out-of-tx.
var allTxStates = []core.TxStatus{core.TxIdle, core.TxInTx, core.TxFailed}

// TestDecide_IndeterminateRecordedByHook asserts that for every indeterminate
// input, in every transaction state, Decide drops the statement, resolves the
// class to ClassUnknown, and the metrics hook records exactly one
// (ClassUnknown, kept=false) event for that decision (R5.17). A fresh
// counting hook is used per (input, state) so the recorded event is asserted in
// isolation.
func TestDecide_IndeterminateRecordedByHook(t *testing.T) {
	for _, sql := range indeterminateInputs {
		for _, st := range allTxStates {
			t.Run(className(ClassUnknown)+"/"+stateName(st)+"/"+sqlLabel(sql), func(t *testing.T) {
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

				got, cls := decideSQL(t, f, sql, st)
				if got != Drop {
					t.Errorf("Decide(%q, %s) action = %s, want Drop", sql, stateName(st), actionName(got))
				}
				if cls != ClassUnknown {
					t.Errorf("Decide(%q, %s) class = %s, want %s", sql, stateName(st), className(cls), className(ClassUnknown))
				}
				if len(recs) != 1 {
					t.Fatalf("hook called %d times for %q (%s), want 1: %+v", len(recs), sql, stateName(st), recs)
				}
				if recs[0].class != ClassUnknown || recs[0].kept {
					t.Errorf("hook recorded {%s,%v} for %q (%s), want {%s,false}",
						className(recs[0].class), recs[0].kept, sql, stateName(st), className(ClassUnknown))
				}
			})
		}
	}
}

// TestDecide_IndeterminateHookCountsDrops verifies the aggregate behavior a
// metrics counter would observe: feeding all indeterminate inputs across all
// states through a single filter records one (ClassUnknown, kept=false) event
// per decision and never a kept event (R5.17).
func TestDecide_IndeterminateHookCountsDrops(t *testing.T) {
	var unknownDropped, total, kept int
	f, err := NewFilterWithHook(Config{Mode: "all"}, func(class StmtClass, k bool) {
		total++
		if k {
			kept++
		}
		if class == ClassUnknown && !k {
			unknownDropped++
		}
	})
	if err != nil {
		t.Fatalf("NewFilterWithHook: %v", err)
	}

	wantDecisions := len(indeterminateInputs) * len(allTxStates)
	for _, sql := range indeterminateInputs {
		for _, st := range allTxStates {
			if got, cls := decideSQL(t, f, sql, st); got != Drop || cls != ClassUnknown {
				t.Errorf("Decide(%q, %s) = (%s,%s), want (Drop,Unknown)",
					sql, stateName(st), actionName(got), className(cls))
			}
		}
	}

	if total != wantDecisions {
		t.Errorf("hook recorded %d events, want %d", total, wantDecisions)
	}
	if unknownDropped != wantDecisions {
		t.Errorf("indeterminate-dropped events = %d, want %d", unknownDropped, wantDecisions)
	}
	if kept != 0 {
		t.Errorf("kept events = %d, want 0 (indeterminate statements must never be kept)", kept)
	}
}

// stateName renders a TxStatus for subtest names and diagnostics.
func stateName(s core.TxStatus) string {
	switch s {
	case core.TxIdle:
		return "Idle"
	case core.TxInTx:
		return "InTx"
	case core.TxFailed:
		return "Failed"
	default:
		return "?"
	}
}

// sqlLabel produces a compact, stable subtest label for an input SQL string,
// keeping empty/whitespace cases legible.
func sqlLabel(sql string) string {
	switch sql {
	case "":
		return "empty"
	case "   ", "\t\n  ":
		return "whitespace"
	}
	if len(sql) > 16 {
		return sql[:16]
	}
	return sql
}
