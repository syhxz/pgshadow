package filter

import (
	"testing"
	"unicode"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p8Statements are representative statements covering each StmtClass produced by
// the classifier. They deliberately avoid case-sensitive constructs (string
// literals, dollar-quote tags) so that re-casing *any* character cannot change
// meaning — only the case-insensitive classification is exercised.
var p8Statements = []string{
	"SELECT col FROM tbl",             // Plain_SELECT
	"SELECT my_func(1, 2)",            // Procedure_Invoking_SELECT (UDF)
	"CALL my_proc(1)",                 // Call
	"INSERT INTO t VALUES (1)",        // DML
	"UPDATE t SET a = 1 WHERE id = 2", // DML
	"DELETE FROM t WHERE id = 1",      // DML
	"CREATE TABLE t (id int)",         // DDL
	"ALTER TABLE t ADD COLUMN c int",  // DDL
	"DROP TABLE t",                    // DDL
	"BEGIN",                           // Begin
	"COMMIT",                          // CommitRollback
	"ROLLBACK",                        // CommitRollback
	"SET search_path = public",        // Utility
	"COPY t FROM STDIN",               // CopyFrom
}

// p8recase re-cases s by toggling each character to upper/lower according to the
// flags slice, applied cyclically. Non-letters are unaffected. An empty flags
// slice returns s unchanged.
func p8recase(s string, flags []bool) string {
	if len(flags) == 0 {
		return s
	}
	runes := []rune(s)
	for i, r := range runes {
		if flags[i%len(flags)] {
			runes[i] = unicode.ToUpper(r)
		} else {
			runes[i] = unicode.ToLower(r)
		}
	}
	return string(runes)
}

// p8stmtGen picks one representative statement by index.
func p8stmtGen() gopter.Gen {
	return gen.IntRange(0, len(p8Statements)-1)
}

// p8flagsGen generates the per-character upper/lower toggle sequence.
func p8flagsGen() gopter.Gen {
	return gen.SliceOf(gen.Bool())
}

// Feature: pgshadow, Property 8: Classification is case-insensitive on the leading keyword
//
// Validates: Requirements 5.1
//
// For any SQL statement and any re-casing of its characters, the SQL filter's
// classifier assigns the same StmtClass to both the original and the re-cased
// text.
func TestProperty8_ClassificationIsCaseInsensitive(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	c := NewClassifier(nil)

	properties.Property("Classify is invariant under arbitrary re-casing",
		prop.ForAll(
			func(idx int, flags []bool) bool {
				original := p8Statements[idx]
				recased := p8recase(original, flags)
				return c.Classify(original) == c.Classify(recased)
			},
			p8stmtGen(),
			p8flagsGen(),
		))

	properties.TestingRun(t)
}
