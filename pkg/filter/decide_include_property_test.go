package filter

import (
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p12Case bundles the randomized inputs for Property 12: a plain SELECT, the
// connection's transaction state, and which keep mechanism is configured for
// it (an include pattern that matches the SELECT, or a replayable procedure the
// SELECT invokes).
type p12Case struct {
	useReplayable bool          // true: configure a replayable proc; false: include pattern
	proc          string        // procedure name (used when useReplayable)
	tbl           string        // table name (used for the include-pattern path)
	state         core.TxStatus // transaction state at decision time
}

// p12StateGen generates one of the three transaction states. Idle and Failed
// are out-of-transaction; including Idle is essential to prove the keep
// overrides the configured out-of-transaction drop.
func p12StateGen() gopter.Gen {
	return gen.OneConstOf(core.TxIdle, core.TxInTx, core.TxFailed)
}

// p12CaseGen combines the mechanism choice, identifiers, and state into a case.
func p12CaseGen() gopter.Gen {
	procs := gen.OneConstOf("refresh_cache", "do_work", "rebuild_index", "sync_data", "run_report")
	tbls := gen.OneConstOf("critical_view", "billing", "audit_log", "ledger", "snapshots")
	return gopter.CombineGens(
		gen.Bool(),
		procs,
		tbls,
		p12StateGen(),
	).Map(func(vals []interface{}) p12Case {
		return p12Case{
			useReplayable: vals[0].(bool),
			proc:          vals[1].(string),
			tbl:           vals[2].(string),
			state:         vals[3].(core.TxStatus),
		}
	})
}

// Feature: pgshadow, Property 12: Include patterns and replayable procedures keep SELECTs and outrank the out-of-transaction drop
//
// Validates: Requirements 5.12
//
// For any SELECT that either matches a configured include pattern or invokes a
// configured replayable procedure, the filter keeps it in every transaction
// state, even when the Plain_SELECT out-of-transaction action is configured to
// drop. The include/replayable keep (R5.12) outranks the out-of-transaction
// drop (R5.6).
func TestProperty12_IncludeAndReplayableKeepSelects(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	properties.Property("included / replayable SELECTs are kept in every state despite out-of-tx drop",
		prop.ForAll(
			func(c p12Case) bool {
				// Custom mode with Plain_SELECT dropped both in and out of
				// transaction, so a keep can only come from the include
				// pattern or replayable-procedure override (R5.12).
				cfg := Config{
					Mode: "custom",
					Rules: map[StmtClass]Rule{
						ClassPlainSelect: {InTransaction: Drop, OutTransaction: Drop},
					},
				}

				var sql string
				if c.useReplayable {
					// A SELECT invoking the configured replayable procedure.
					cfg.ReplayableProcedures = []string{c.proc}
					sql = "SELECT " + c.proc + "()"
				} else {
					// A plain SELECT whose text matches the include pattern.
					cfg.IncludePatterns = []Regexp{mustRegexp(t, `(?i)from\s+`+c.tbl+`\b`)}
					sql = "SELECT * FROM " + c.tbl
				}

				f := mustFilter(t, cfg)
				got, _ := decideSQL(t, f, sql, c.state)
				return got == Keep
			},
			p12CaseGen(),
		))

	properties.TestingRun(t)
}
