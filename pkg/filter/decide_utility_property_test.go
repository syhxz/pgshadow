package filter

import (
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p13Case bundles the randomized inputs for Property 13: a utility statement
// (SET/RESET/DISCARD), the configured keep/drop action for ClassUtility, and the
// connection's transaction state.
type p13Case struct {
	action Action        // configured action for ClassUtility (both in/out tx)
	state  core.TxStatus // connection transaction state
	sql    string        // a SET/RESET/DISCARD utility statement
}

// p13ActionGen generates a keep/drop action.
func p13ActionGen() gopter.Gen {
	return gen.OneConstOf(Keep, Drop)
}

// p13StateGen generates one of the three transaction states. Idle and Failed
// are out-of-transaction; InTx is in-transaction.
func p13StateGen() gopter.Gen {
	return gen.OneConstOf(core.TxIdle, core.TxInTx, core.TxFailed)
}

// p13UtilityGen generates a random utility statement. The pool covers the three
// utility leading keywords (SET, RESET, DISCARD) with representative operands so
// the classifier resolves each to ClassUtility. None of the statements contain a
// function call or match any include/exclude pattern (none are configured).
func p13UtilityGen() gopter.Gen {
	return gen.OneConstOf(
		"SET search_path = public",
		"SET statement_timeout = 1000",
		"SET TIME ZONE 'UTC'",
		"SET LOCAL work_mem = '64MB'",
		"RESET ALL",
		"RESET search_path",
		"RESET statement_timeout",
		"DISCARD ALL",
		"DISCARD TEMP",
		"DISCARD PLANS",
	)
}

// p13CaseGen combines the action, state, and utility-statement generators.
func p13CaseGen() gopter.Gen {
	return gopter.CombineGens(
		p13ActionGen(),
		p13StateGen(),
		p13UtilityGen(),
	).Map(func(vals []interface{}) p13Case {
		return p13Case{
			action: vals[0].(Action),
			state:  vals[1].(core.TxStatus),
			sql:    vals[2].(string),
		}
	})
}

// Feature: pgshadow, Property 13: Utility-statement decisions follow the configured utility action
//
// Validates: Requirements 5.13
//
// For any SET, DISCARD, or RESET statement, any transaction state, and any
// configured utility action, the filter classifies the statement as
// ClassUtility and its decision equals the configured action. The action is set
// identically for in-transaction and out-of-transaction, so the decision is
// state-independent.
func TestProperty13_UtilityStatementDecisions(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	properties.Property("utility-statement decision equals the configured utility action",
		prop.ForAll(
			func(c p13Case) bool {
				f := mustFilter(t, Config{
					Mode: "custom",
					Rules: map[StmtClass]Rule{
						ClassUtility: {InTransaction: c.action, OutTransaction: c.action},
					},
				})

				got, cls := decideSQL(t, f, c.sql, c.state)
				return cls == ClassUtility && got == c.action
			},
			p13CaseGen(),
		))

	properties.TestingRun(t)
}
