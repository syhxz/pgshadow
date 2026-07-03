package filter

import (
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p7stmtChoices is the pool of statement classes fed to OnStatement. It mixes
// the two control classes that drive unidirectional inference (ClassBegin,
// ClassCommitRollback) with several non-control classes that must NOT change
// the inferred transaction state (R4.3).
var p7stmtChoices = []StmtType{
	ClassBegin,
	ClassCommitRollback,
	// Non-control classes: these must leave the inferred state unchanged.
	ClassPlainSelect,
	ClassProcSelect,
	ClassCall,
	ClassDML,
	ClassDDL,
	ClassUtility,
	ClassCopyFrom,
	ClassUnknown,
}

// p7isControl reports whether a statement class is a transaction-control
// statement that participates in unidirectional inference.
func p7isControl(st StmtType) bool {
	return st == ClassBegin || st == ClassCommitRollback
}

// p7stmtGen generates a single statement class from the mixed pool.
func p7stmtGen() gopter.Gen {
	idxs := make([]interface{}, len(p7stmtChoices))
	for i := range p7stmtChoices {
		idxs[i] = i
	}
	return gen.OneConstOf(idxs...).Map(func(i int) StmtType {
		return p7stmtChoices[i]
	})
}

// Feature: pgshadow, Property 7: Unidirectional transaction inference tracks the last control statement
//
// Validates: Requirements 4.3, 4.4, 4.5
//
// For any sequence of BEGIN / COMMIT / ROLLBACK statements (interleaved with
// non-control statements) observed for a connection in unidirectional mode, the
// inferred transaction state is In transaction if the last control statement
// was BEGIN, and Idle if the last control statement was COMMIT/ROLLBACK. If no
// control statement was observed the state remains the default Idle, and
// non-control statements never change the inferred state.
func TestProperty7_UnidirectionalInferenceTracksLastControlStatement(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	properties.Property("final inferred state equals the mapping of the last control statement",
		prop.ForAll(
			func(stmts []StmtType) bool {
				m := NewStateMachine()
				c := conn(8000)

				// Oracle: the expected final state is determined solely by the
				// most recent control statement. Default Idle if none seen.
				want := core.TxIdle
				for _, st := range stmts {
					m.OnStatement(c, st)
					switch st {
					case ClassBegin:
						want = core.TxInTx
					case ClassCommitRollback:
						want = core.TxIdle
						// Non-control statements do not affect the oracle.
					}
				}

				return m.State(c) == want
			},
			gen.SliceOf(p7stmtGen()),
		))

	properties.TestingRun(t)
}
