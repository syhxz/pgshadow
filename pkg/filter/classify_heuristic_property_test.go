package filter

import (
	"fmt"
	"strings"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p9Case is one generated classification scenario: a SELECT statement and
// whether it is expected to be a Procedure_Invoking_SELECT.
type p9Case struct {
	sql      string
	wantProc bool
}

// p9SafeIdent sanitizes an arbitrary string into a valid, lower-cased SQL
// identifier that is guaranteed NOT to be a known built-in/aggregate function
// (defaultBuiltinSet) nor a SQL keyword that may legitimately precede '('
// (sqlKeywordsBeforeParen). This keeps the proc-invoking generator from
// accidentally emitting `now(...)`, `count(...)`, `in (...)`, etc., which the
// heuristic must NOT treat as user-defined procedure invocations.
func p9SafeIdent(raw string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(raw) {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	s := b.String()
	// Identifiers must start with a letter for unambiguous lexing.
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		s = "u" + s
	}
	// Force the name out of the builtin/keyword exclusion sets so it is a true
	// user-defined function/procedure name.
	for defaultBuiltinSet[s] || sqlKeywordsBeforeParen[s] {
		s += "x"
	}
	return s
}

// p9Args renders n comma-separated integer arguments (possibly zero), never
// introducing stray parentheses into the generated SQL.
func p9Args(n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = fmt.Sprintf("%d", i+1)
	}
	return strings.Join(parts, ", ")
}

// p9CaseGen builds either a procedure-invoking SELECT (a non-builtin,
// optionally schema-qualified identifier immediately followed by a
// parenthesized argument list, in plain `SELECT fn(...)` or
// `SELECT * FROM fn(...)` form) or a plain SELECT that only reads
// columns/tables (no parentheses at all).
func p9CaseGen() gopter.Gen {
	return gopter.CombineGens(
		gen.Bool(),         // isProc
		gen.Bool(),         // variantA: schema-qualified (proc) / SELECT * (plain)
		gen.Bool(),         // variantB: FROM fn(...) (proc) / has FROM clause (plain)
		gen.AnyString(),    // raw1
		gen.AnyString(),    // raw2
		gen.IntRange(0, 3), // nArgs
		gen.IntRange(1, 4), // nCols
	).Map(func(vals []interface{}) p9Case {
		isProc := vals[0].(bool)
		variantA := vals[1].(bool)
		variantB := vals[2].(bool)
		raw1 := vals[3].(string)
		raw2 := vals[4].(string)
		nArgs := vals[5].(int)
		nCols := vals[6].(int)

		if isProc {
			name := p9SafeIdent(raw2)
			if variantA { // schema-qualified function name
				name = p9SafeIdent(raw1) + "." + name
			}
			call := name + "(" + p9Args(nArgs) + ")"
			var sql string
			if variantB {
				sql = "SELECT * FROM " + call
			} else {
				sql = "SELECT " + call
			}
			return p9Case{sql: sql, wantProc: true}
		}

		// Plain SELECT: read columns / a table, with no parentheses.
		var colsStr string
		if variantA {
			colsStr = "*"
		} else {
			cols := make([]string, nCols)
			for i := 0; i < nCols; i++ {
				cols[i] = p9SafeIdent(fmt.Sprintf("%s%d", raw1, i))
			}
			colsStr = strings.Join(cols, ", ")
		}
		sql := "SELECT " + colsStr
		if variantB {
			sql += " FROM " + p9SafeIdent(raw2)
		}
		return p9Case{sql: sql, wantProc: false}
	})
}

// Feature: pgshadow, Property 9: The SELECT heuristic distinguishes procedure-invoking from plain SELECTs
//
// Validates: Requirements 5.3
//
// For any SELECT that invokes a user-defined function or stored procedure (a
// non-builtin, optionally schema-qualified identifier immediately followed by a
// parenthesized argument list, including the `SELECT * FROM fn(...)` form), the
// classifier reports IsProcInvokingSelect == true and Classify == ClassProcSelect.
// For any SELECT that only reads columns/tables (no UDF invocation), the
// classifier reports IsProcInvokingSelect == false and Classify == ClassPlainSelect.
func TestProperty9_SelectHeuristicDistinguishesProcFromPlain(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	c := NewClassifier(nil)

	properties.Property("proc-invoking SELECTs classify as ProcSelect; plain SELECTs as PlainSelect",
		prop.ForAll(
			func(tc p9Case) bool {
				gotProc := c.IsProcInvokingSelect(tc.sql)
				gotClass := c.Classify(tc.sql)
				if tc.wantProc {
					return gotProc && gotClass == ClassProcSelect
				}
				return !gotProc && gotClass == ClassPlainSelect
			},
			p9CaseGen(),
		))

	properties.TestingRun(t)
}
