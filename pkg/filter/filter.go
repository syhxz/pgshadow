// Package filter implements Module ③ — SQL Filter. It maintains per-ConnID
// transaction state and decides keep/drop for each SQL_Event using a
// transaction-aware rule matrix (R4, R5).
//
// This file contains compiling stubs only; method bodies are placeholders.
// The transaction status type is shared and lives in pkg/core (core.TxStatus);
// it is not redeclared here.
package filter

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"

	"pgshadow/pkg/core"
)

// StmtClass enumerates the statement categories produced by the Classifier.
type StmtClass int

const (
	ClassUnknown StmtClass = iota
	ClassPlainSelect
	ClassProcSelect // Procedure_Invoking_SELECT
	ClassCall
	ClassDML // INSERT/UPDATE/DELETE
	ClassDDL // CREATE/ALTER/DROP
	ClassBegin
	ClassCommitRollback
	ClassUtility // SET/DISCARD/RESET
	ClassCopyFrom
)

// stmtClassNames maps the YAML rule-key spellings (used in the filter.rules
// matrix of the config schema, R11.4) to their StmtClass. The "select" key
// controls the Plain_SELECT rule (R5.6, R5.7) and "set" controls utility
// statements (R5.13).
var stmtClassNames = map[string]StmtClass{
	"select":          ClassPlainSelect,
	"plain_select":    ClassPlainSelect,
	"proc_select":     ClassProcSelect,
	"call":            ClassCall,
	"dml":             ClassDML,
	"ddl":             ClassDDL,
	"begin":           ClassBegin,
	"commit_rollback": ClassCommitRollback,
	"set":             ClassUtility,
	"utility":         ClassUtility,
	"copy_from":       ClassCopyFrom,
	"copy":            ClassCopyFrom,
}

// UnmarshalYAML decodes a StmtClass from its YAML rule-key string so the
// documented filter.rules matrix parses directly into the typed map key.
func (c *StmtClass) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	cls, ok := stmtClassNames[strings.ToLower(strings.TrimSpace(s))]
	if !ok {
		return fmt.Errorf("invalid filter rule key %q", s)
	}
	*c = cls
	return nil
}

// StmtType is the statement classification consumed by the transaction state
// machine when inferring state from control statements (R4.3-4.5). It aliases
// StmtClass.
type StmtType = StmtClass

// StateMachine tracks transaction state independently per ConnID. R4.2
type StateMachine interface {
	// OnReadyForQuery applies a backend 'Z' status byte (bidirectional). R4.1
	OnReadyForQuery(conn core.ConnID, status byte)
	// OnStatement infers state from BEGIN/COMMIT/ROLLBACK (unidirectional). R4.3-4.5
	OnStatement(conn core.ConnID, stmtType StmtType)
	State(conn core.ConnID) core.TxStatus
	Forget(conn core.ConnID) // on connection close
}

// Classifier inspects SQL text. Classification is case-insensitive on the
// leading keyword (R5.1) with a SELECT sub-heuristic (R5.3).
type Classifier interface {
	Classify(sql string) StmtClass
	// IsProcInvokingSelect implements the heuristic of R5.3. It detects
	// user-defined function/procedure invocations while excluding known
	// PostgreSQL built-in and aggregate functions (Builtin_Function_List).
	IsProcInvokingSelect(sql string) bool
}

// DefaultBuiltinFunctions is the baseline exclusion set for the proc-invoking
// heuristic. Calls to these functions do NOT trigger ClassProcSelect.
var DefaultBuiltinFunctions = []string{
	// Aggregates
	"count", "sum", "avg", "min", "max", "array_agg", "string_agg",
	"bool_and", "bool_or", "every", "json_agg", "jsonb_agg",
	"json_object_agg", "jsonb_object_agg", "xmlagg",
	// Window functions
	"row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
	"ntile", "lead", "lag", "first_value", "last_value", "nth_value",
	// Date/time
	"now", "current_timestamp", "current_date", "current_time",
	"clock_timestamp", "statement_timestamp", "transaction_timestamp",
	"age", "date_part", "date_trunc", "extract", "make_date",
	"make_interval", "make_time", "make_timestamp",
	// Conditional/comparison
	"coalesce", "nullif", "greatest", "least",
	// Type casting/formatting
	"cast", "to_char", "to_date", "to_number", "to_timestamp",
	// Array/set-returning
	"generate_series", "unnest", "array_length", "array_upper",
	"array_lower", "array_cat", "array_append", "array_remove",
	// String
	"length", "upper", "lower", "trim", "ltrim", "rtrim",
	"substring", "replace", "concat", "concat_ws", "format",
	"left", "right", "repeat", "reverse", "split_part",
	// Math
	"abs", "ceil", "floor", "round", "trunc", "mod", "power",
	"sqrt", "random", "sign",
	// JSON
	"json_build_object", "jsonb_build_object", "json_build_array",
	"jsonb_build_array", "row_to_json", "to_json", "to_jsonb",
	// System/info
	"pg_typeof", "version", "current_database", "current_schema",
	"current_user", "session_user", "txid_current",
}

// Action is the keep/drop decision for a statement.
type Action int

const (
	Keep Action = iota
	Drop
)

// UnmarshalYAML decodes an Action from its YAML string form (keep|drop) so the
// documented filter.rules matrix parses directly into the typed enum (R11.4).
func (a *Action) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "keep":
		*a = Keep
	case "drop":
		*a = Drop
	default:
		return fmt.Errorf("invalid filter action %q (want keep|drop)", s)
	}
	return nil
}

// String returns the canonical YAML spelling of the action.
func (a Action) String() string {
	switch a {
	case Keep:
		return "keep"
	case Drop:
		return "drop"
	default:
		return fmt.Sprintf("Action(%d)", int(a))
	}
}

// Regexp is a YAML-decodable wrapper around *regexp.Regexp so the documented
// exclude_patterns / include_patterns string lists (R5.16, R5.12) compile
// directly into ready-to-use regular expressions at config load time.
type Regexp struct {
	*regexp.Regexp
}

// UnmarshalYAML compiles a regex pattern string into the wrapped *regexp.Regexp.
func (r *Regexp) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	re, err := regexp.Compile(s)
	if err != nil {
		return fmt.Errorf("invalid regex pattern %q: %w", s, err)
	}
	r.Regexp = re
	return nil
}

// Rule is the per-statement-type in/out-of-transaction action matrix. R11.4
type Rule struct {
	InTransaction  Action `yaml:"in_transaction"`  // applied when TxInTx
	OutTransaction Action `yaml:"out_transaction"` // applied when TxIdle
}

// Config configures the SQL filter.
type Config struct {
	Mode                 string             `yaml:"mode"` // write_only|all|ddl_only|dml_only|custom R5.15
	Rules                map[StmtClass]Rule `yaml:"rules"`
	ExcludePatterns      []Regexp           `yaml:"exclude_patterns"`      // R5.16
	IncludePatterns      []Regexp           `yaml:"include_patterns"`      // R5.12
	ReplayableProcedures []string           `yaml:"replayable_procedures"` // R5.12
	BuiltinFunctions     []string           `yaml:"builtin_functions"`     // additional names excluded from proc-invoking heuristic (R5.3)
}

// Filter is the top-level decision function.
type Filter interface {
	// Decide returns Keep or Drop for an event given current tx state.
	Decide(ev *core.SQLEvent, state core.TxStatus) (Action, StmtClass)
}
