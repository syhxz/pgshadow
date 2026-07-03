package filter

import (
	"fmt"
	"regexp"
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p14Statements is a pool of base statements spanning every statement class,
// including the always-keep classes (DML, DDL, CALL, COPY ... FROM, BEGIN,
// COMMIT/ROLLBACK), the Procedure_Invoking_SELECT class, the configurable
// Plain_SELECT and Utility classes. Under the default matrix each of these
// resolves to Keep in at least one transaction state, so dropping them can only
// be explained by the exclude pattern taking the highest precedence (R5.16).
var p14Statements = []string{
	"SELECT a FROM t",            // Plain_SELECT
	"SELECT do_batch(1)",         // Procedure_Invoking_SELECT (R5.4)
	"SELECT * FROM run_report()", // Procedure_Invoking_SELECT (R5.4)
	"CALL run_job()",             // CALL (R5.5)
	"INSERT INTO t VALUES (1)",   // DML (R5.8)
	"UPDATE t SET a = 1",         // DML (R5.8)
	"DELETE FROM t WHERE id = 1", // DML (R5.8)
	"CREATE TABLE t (id int)",    // DDL (R5.9)
	"ALTER TABLE t ADD c int",    // DDL (R5.9)
	"DROP TABLE t",               // DDL (R5.9)
	"BEGIN",                      // BEGIN (R5.10)
	"COMMIT",                     // COMMIT/ROLLBACK (R5.11)
	"ROLLBACK",                   // COMMIT/ROLLBACK (R5.11)
	"COPY t FROM STDIN",          // COPY ... FROM (R5.14)
	"SET search_path = public",   // Utility (R5.13)
}

// p14Case is one generated scenario: a base statement (by index), a unique
// regex-safe marker to inject and target with an exclude pattern, and the
// transaction state to decide under.
type p14Case struct {
	baseIdx int
	marker  string
	state   core.TxStatus
}

// p14Marker builds a regex-safe, highly-unique marker token from a random
// 32-bit value. Using an alphanumeric token avoids any regex metacharacters so
// the exclude pattern matches the injected substring literally.
func p14Marker(n uint32) string {
	return fmt.Sprintf("zzexcl_%08x_marker", n)
}

// p14CaseGen combines the base statement index, the unique marker and the
// transaction state into a single scenario generator.
func p14CaseGen() gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(0, len(p14Statements)-1),
		gen.UInt32().Map(p14Marker),
		gen.OneConstOf(core.TxIdle, core.TxInTx, core.TxFailed),
	).Map(func(vals []interface{}) p14Case {
		return p14Case{
			baseIdx: vals[0].(int),
			marker:  vals[1].(string),
			state:   vals[2].(core.TxStatus),
		}
	})
}

// p14Inject splices the unique marker into the statement as a trailing comment.
// A comment leaves the leading keyword (and therefore the classification)
// untouched, so the base statement keeps the class it would normally be kept
// for — isolating the exclude-pattern precedence as the sole cause of the drop.
func p14Inject(base, marker string) string {
	return base + " /* " + marker + " */"
}

// Feature: pgshadow, Property 14: Exclude patterns drop matching statements with highest precedence
//
// Validates: Requirements 5.2, 5.16
//
// For any SQL_Event of any class — including the always-keep classes (DML, DDL,
// CALL, COPY ... FROM, BEGIN, COMMIT/ROLLBACK) and the Procedure_Invoking_SELECT
// class that the default matrix keeps in every transaction state — if the
// statement text matches a configured exclude pattern then the filter drops it,
// overriding every keep rule. A fresh, unique marker is injected into each
// generated statement and an exclude pattern is configured to match exactly
// that marker, across randomly chosen transaction states (Idle, In transaction,
// Failed).
func TestProperty14_ExcludePatternsDropWithHighestPrecedence(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	properties.Property("a matching exclude pattern drops the statement regardless of class or state",
		prop.ForAll(
			func(tc p14Case) bool {
				sql := p14Inject(p14Statements[tc.baseIdx], tc.marker)
				// Configure a filter whose only exclude pattern matches the
				// injected marker literally. Default mode keeps the underlying
				// classes, so a Drop can only come from the exclude rule.
				f := mustFilter(t, Config{
					ExcludePatterns: []Regexp{{Regexp: regexp.MustCompile(regexp.QuoteMeta(tc.marker))}},
				})
				got, _ := decideSQL(t, f, sql, tc.state)
				return got == Drop
			},
			p14CaseGen(),
		))

	properties.TestingRun(t)
}
