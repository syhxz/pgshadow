package filter

import (
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p11Case bundles the randomized inputs for Property 11: the per-state actions
// configured for the Plain_SELECT rule, the connection's transaction state, and
// a plain SELECT statement (no function call, no include/exclude match).
type p11Case struct {
	inAction  Action // configured in_transaction action for ClassPlainSelect
	outAction Action // configured out_transaction action for ClassPlainSelect
	state     core.TxStatus
	sql       string
}

// p11ActionGen generates a keep/drop action.
func p11ActionGen() gopter.Gen {
	return gen.OneConstOf(Keep, Drop)
}

// p11StateGen generates one of the three transaction states. Idle and Failed
// are out-of-transaction; InTx is in-transaction.
func p11StateGen() gopter.Gen {
	return gen.OneConstOf(core.TxIdle, core.TxInTx, core.TxFailed)
}

// p11SelectGen generates a plain SELECT with random column/table identifiers.
// The identifiers are drawn from simple alphabetic pools so the statement never
// contains a function call (which would make it a Procedure_Invoking_SELECT) and
// never matches an include/exclude pattern (none are configured).
func p11SelectGen() gopter.Gen {
	cols := gen.OneConstOf("a", "b", "id", "name", "col1", "val", "x", "amount")
	tbls := gen.OneConstOf("t", "users", "orders", "items", "accounts", "logs")
	return gopter.CombineGens(cols, tbls).Map(func(vals []interface{}) string {
		return "SELECT " + vals[0].(string) + " FROM " + vals[1].(string)
	})
}

// p11CaseGen combines the action, state, and SQL generators into one case.
func p11CaseGen() gopter.Gen {
	return gopter.CombineGens(
		p11ActionGen(),
		p11ActionGen(),
		p11StateGen(),
		p11SelectGen(),
	).Map(func(vals []interface{}) p11Case {
		return p11Case{
			inAction:  vals[0].(Action),
			outAction: vals[1].(Action),
			state:     vals[2].(core.TxStatus),
			sql:       vals[3].(string),
		}
	})
}

// Feature: pgshadow, Property 11: Plain_SELECT decisions follow the configured per-state action
//
// Validates: Requirements 5.6, 5.7
//
// For any Plain_SELECT, any transaction state, and any configured SELECT rule,
// the filter's decision equals the rule's action for that state (in_transaction
// when InTx, out_transaction when Idle/Failed), provided no include pattern,
// replayable procedure, or exclude pattern applies.
func TestProperty11_PlainSelectPerStateDecisions(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	properties.Property("Plain_SELECT decision equals the configured action for the state",
		prop.ForAll(
			func(c p11Case) bool {
				f := mustFilter(t, Config{
					Mode: "custom",
					Rules: map[StmtClass]Rule{
						ClassPlainSelect: {InTransaction: c.inAction, OutTransaction: c.outAction},
					},
				})

				// Expected action: in_transaction for InTx, out_transaction for
				// Idle/Failed (both routed as out-of-transaction).
				want := c.outAction
				if c.state == core.TxInTx {
					want = c.inAction
				}

				got, cls := decideSQL(t, f, c.sql, c.state)
				// The statement must classify as Plain_SELECT for this property
				// to be meaningful, and the decision must match the rule action.
				return cls == ClassPlainSelect && got == want
			},
			p11CaseGen(),
		))

	properties.TestingRun(t)
}
