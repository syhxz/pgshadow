package filter

import (
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p10AlwaysKeep pairs an always-keep SQL statement with the StmtClass it is
// expected to classify into. The always-keep set is every class that the
// default rule matrix keeps in AND out of a transaction:
// Procedure_Invoking_SELECT (R5.4), CALL (R5.5), DML (R5.8), DDL (R5.9),
// BEGIN (R5.10), COMMIT/ROLLBACK (R5.11) and COPY ... FROM (R5.14). Plain_SELECT
// is intentionally excluded because it drops out-of-transaction, and so is the
// indeterminate class.
type p10AlwaysKeep struct {
	sql   string
	class StmtClass
}

// p10Cases is the pool of always-keep statements, with at least one example per
// always-keep class so the generator covers the full set under Requirement 5.
var p10Cases = []p10AlwaysKeep{
	// Procedure_Invoking_SELECT (R5.4)
	{"SELECT do_batch(1)", ClassProcSelect},
	{"SELECT * FROM run_report()", ClassProcSelect},
	// CALL (R5.5)
	{"CALL run_job()", ClassCall},
	{"CALL schema.process(42)", ClassCall},
	// DML: INSERT/UPDATE/DELETE (R5.8)
	{"INSERT INTO t VALUES (1)", ClassDML},
	{"UPDATE t SET a = 1", ClassDML},
	{"DELETE FROM t WHERE id = 1", ClassDML},
	// DDL: CREATE/ALTER/DROP (R5.9)
	{"CREATE TABLE t (id int)", ClassDDL},
	{"ALTER TABLE t ADD c int", ClassDDL},
	{"DROP TABLE t", ClassDDL},
	// BEGIN (R5.10)
	{"BEGIN", ClassBegin},
	{"START TRANSACTION", ClassBegin},
	// COMMIT/ROLLBACK (R5.11)
	{"COMMIT", ClassCommitRollback},
	{"ROLLBACK", ClassCommitRollback},
	// COPY ... FROM (R5.14)
	{"COPY t FROM STDIN", ClassCopyFrom},
	{"COPY t (a, b) FROM '/tmp/data.csv'", ClassCopyFrom},
}

// p10CaseGen generates a random always-keep statement from the pool.
func p10CaseGen() gopter.Gen {
	idxs := make([]interface{}, len(p10Cases))
	for i := range p10Cases {
		idxs[i] = i
	}
	return gen.OneConstOf(idxs...).Map(func(i int) p10AlwaysKeep {
		return p10Cases[i]
	})
}

// p10StateGen generates one of the three transaction states.
func p10StateGen() gopter.Gen {
	return gen.OneConstOf(core.TxIdle, core.TxInTx, core.TxFailed)
}

// Feature: pgshadow, Property 10: Always-keep statement classes are kept regardless of transaction state
//
// Validates: Requirements 5.4, 5.5, 5.8, 5.9, 5.10, 5.11, 5.14
//
// For any SQL_Event whose class is in the always-keep set
// (Procedure_Invoking_SELECT, CALL, INSERT/UPDATE/DELETE, CREATE/ALTER/DROP,
// BEGIN, COMMIT/ROLLBACK, COPY ... FROM) and any transaction state (Idle, In
// transaction, Failed), a default-config filter keeps the event and resolves it
// to the expected class. No exclude or include patterns are configured, so the
// matrix-driven keep decision is exercised directly.
func TestProperty10_AlwaysKeepClassesKeptRegardlessOfState(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	f := mustFilter(t, Config{}) // default config: empty mode, no exclude/include patterns

	properties.Property("always-keep classes are kept in every transaction state",
		prop.ForAll(
			func(tc p10AlwaysKeep, state core.TxStatus) bool {
				got, cls := decideSQL(t, f, tc.sql, state)
				return got == Keep && cls == tc.class
			},
			p10CaseGen(),
			p10StateGen(),
		))

	properties.TestingRun(t)
}
