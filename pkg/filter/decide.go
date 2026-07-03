package filter

import (
	"fmt"
	"strings"

	"pgshadow/pkg/core"
)

// This file implements task 5.3: the default rule matrix, the filter-mode
// presets (write_only|all|ddl_only|dml_only|custom, R5.15), and the ordered
// Decide pipeline that realizes the "Filter Decision Flow" from design.md and
// the precedence of Requirement 5 criterion 2.
//
// Precedence (highest first), per the decision-flow diagram:
//
//  1. exclude patterns           → DROP + record               (R5.16)
//  2. indeterminate (Unknown)    → DROP + record indeterminate  (R5.17)
//  3. always-keep classes        → matrix action                (R5.5,8,9,10,11,14)
//     (DML/DDL/CALL/BEGIN/COMMIT-ROLLBACK/COPY FROM)
//  4. SELECT include-pattern / replayable-procedure → KEEP       (R5.12)
//  5. Procedure_Invoking_SELECT  → matrix action (default keep)  (R5.4)
//  6. Plain_SELECT per-state     → matrix action                (R5.6, R5.7)
//  7. utility (SET/DISCARD/RESET)→ matrix action                (R5.13)
//
// The mode preset populates the per-StmtClass rule matrix consulted at the
// class-terminal nodes; the structural overrides (exclude drop, indeterminate
// drop, SELECT include/replayable keep) are not matrix-driven because they are
// explicit operator intent that outranks the matrix (R5.12, R5.16).

// ClassifiedHook is an optional, nil-safe callback invoked once per Decide with
// the resolved statement class and whether the event was kept. It is the
// decoupling seam for the metrics layer: pkg/metrics depends on pkg/filter, so
// the filter MUST NOT import pkg/metrics. The wiring layer adapts a
// metrics.Collector.Classified(class, kept) call into this function type to
// avoid an import cycle while still recording indeterminate classifications
// (R5.17) and overall keep/drop counts (R5).
type ClassifiedHook func(class StmtClass, kept bool)

// filterImpl is the concrete Filter. It is safe for concurrent use: all fields
// are read-only after construction and the embedded Classifier is stateless.
type filterImpl struct {
	cls          Classifier
	rules        map[StmtClass]Rule
	exclude      []Regexp
	include      []Regexp
	replayable   map[string]bool // lowercased replayable proc names (R5.12)
	onClassified ClassifiedHook
}

// NewFilter constructs a Filter from cfg with no metrics hook. It returns an
// error if cfg.Mode is not one of the supported presets.
func NewFilter(cfg Config) (Filter, error) {
	return NewFilterWithHook(cfg, nil)
}

// NewFilterWithHook constructs a Filter from cfg, wiring hook (which may be nil)
// as the per-decision classification callback. The hook is the only coupling
// to metrics and is injected to avoid an import cycle.
func NewFilterWithHook(cfg Config, hook ClassifiedHook) (Filter, error) {
	rules, err := resolveRules(cfg)
	if err != nil {
		return nil, err
	}
	rep := make(map[string]bool, len(cfg.ReplayableProcedures))
	for _, p := range cfg.ReplayableProcedures {
		if name := strings.ToLower(strings.TrimSpace(p)); name != "" {
			rep[name] = true
		}
	}
	return &filterImpl{
		cls:          NewClassifier(cfg.BuiltinFunctions),
		rules:        rules,
		exclude:      cfg.ExcludePatterns,
		include:      cfg.IncludePatterns,
		replayable:   rep,
		onClassified: hook,
	}, nil
}

// Decide classifies ev, runs the ordered decision pipeline against the current
// transaction state, records the outcome through the metrics hook, and returns
// the keep/drop action together with the resolved class.
func (f *filterImpl) Decide(ev *core.SQLEvent, state core.TxStatus) (Action, StmtClass) {
	sql := ev.SQL
	class := f.cls.Classify(sql)
	action := f.decide(sql, class, state)
	f.record(class, action == Keep)
	return action, class
}

// decide applies the ordered precedence pipeline. It is separated from Decide
// so the metrics recording happens in exactly one place.
func (f *filterImpl) decide(sql string, class StmtClass, state core.TxStatus) Action {
	// 1. Exclude patterns have the highest precedence (R5.16).
	if matchesAny(f.exclude, sql) {
		return Drop
	}
	// 2. Indeterminate classification → drop + record indeterminate (R5.17).
	if class == ClassUnknown {
		return Drop
	}
	switch class {
	case ClassPlainSelect, ClassProcSelect:
		// SELECT branch. Include-pattern and replayable-procedure matches keep
		// the statement regardless of transaction state and outrank the
		// configurable Plain_SELECT out-of-transaction action (R5.12).
		if matchesAny(f.include, sql) || f.invokesReplayable(sql) {
			return Keep
		}
		// Procedure_Invoking_SELECT (R5.4) and Plain_SELECT (R5.6, R5.7) both
		// resolve through the per-class rule matrix; the preset/default matrix
		// encodes the documented keep/drop behavior.
		return f.ruleAction(class, state)
	default:
		// Always-keep classes (R5.5, R5.8-R5.11, R5.14) and utility statements
		// (R5.13) resolve through the matrix. Under the default and write_only/
		// all presets the always-keep classes map to keep/keep; the ddl_only
		// and dml_only presets narrow the matrix to a single kept class.
		return f.ruleAction(class, state)
	}
}

// ruleAction looks up the configured action for a class given the connection's
// transaction state. In-transaction state uses the rule's InTransaction action;
// Idle and Failed both use the OutTransaction action (the decision-flow diagram
// routes Idle/Failed identically). A class with no matrix entry is dropped.
func (f *filterImpl) ruleAction(class StmtClass, state core.TxStatus) Action {
	r, ok := f.rules[class]
	if !ok {
		return Drop
	}
	if state == core.TxInTx {
		return r.InTransaction
	}
	return r.OutTransaction
}

// record forwards the classification outcome to the metrics hook when present.
func (f *filterImpl) record(class StmtClass, kept bool) {
	if f.onClassified != nil {
		f.onClassified(class, kept)
	}
}

// invokesReplayable reports whether sql invokes a function or stored procedure
// whose name appears in the configured replayable_procedures list (R5.12). It
// reuses the package lexer so parentheses inside comments and string/dollar-
// quoted literals never produce a false positive. Both the final identifier
// segment (e.g. do_work) and the fully qualified name (e.g. schema.do_work) are
// matched case-insensitively.
func (f *filterImpl) invokesReplayable(sql string) bool {
	if len(f.replayable) == 0 {
		return false
	}
	toks := lex(sql)
	for i := 0; i < len(toks); i++ {
		if toks[i].kind != tokWord {
			continue
		}
		segs := []string{strings.ToLower(toks[i].text)}
		j := i
		for j+2 < len(toks) && toks[j+1].kind == tokDot && toks[j+2].kind == tokWord {
			segs = append(segs, strings.ToLower(toks[j+2].text))
			j += 2
		}
		if j+1 < len(toks) && toks[j+1].kind == tokLParen {
			last := segs[len(segs)-1]
			qualified := strings.Join(segs, ".")
			if f.replayable[last] || f.replayable[qualified] {
				return true
			}
		}
		i = j
	}
	return false
}

// matchesAny reports whether s matches any of the compiled patterns. Nil
// patterns (e.g. a zero-value Regexp) are skipped.
func matchesAny(res []Regexp, s string) bool {
	for _, re := range res {
		if re.Regexp != nil && re.MatchString(s) {
			return true
		}
	}
	return false
}

// allRuleClasses is the full set of classifiable statement classes that carry a
// keep/drop rule. ClassUnknown is intentionally excluded: indeterminate
// statements are always dropped structurally (R5.17), never via the matrix.
var allRuleClasses = []StmtClass{
	ClassPlainSelect, ClassProcSelect, ClassCall, ClassDML, ClassDDL,
	ClassBegin, ClassCommitRollback, ClassCopyFrom, ClassUtility,
}

// defaultRules returns the default rule matrix encoding Requirement 5: every
// classifiable class keeps in and out of a transaction, except Plain_SELECT
// which keeps in-transaction (R5.7) and drops out-of-transaction (R5.6).
func defaultRules() map[StmtClass]Rule {
	return map[StmtClass]Rule{
		ClassPlainSelect:    {InTransaction: Keep, OutTransaction: Drop}, // R5.6, R5.7
		ClassProcSelect:     {InTransaction: Keep, OutTransaction: Keep}, // R5.4
		ClassCall:           {InTransaction: Keep, OutTransaction: Keep}, // R5.5
		ClassDML:            {InTransaction: Keep, OutTransaction: Keep}, // R5.8
		ClassDDL:            {InTransaction: Keep, OutTransaction: Keep}, // R5.9
		ClassBegin:          {InTransaction: Keep, OutTransaction: Keep}, // R5.10
		ClassCommitRollback: {InTransaction: Keep, OutTransaction: Keep}, // R5.11
		ClassCopyFrom:       {InTransaction: Keep, OutTransaction: Keep}, // R5.14
		ClassUtility:        {InTransaction: Keep, OutTransaction: Keep}, // R5.13 (default keep)
	}
}

// onlyKeep builds a matrix that keeps a single class in and out of a
// transaction and drops every other classifiable class. It backs the ddl_only
// and dml_only presets.
func onlyKeep(keep StmtClass) map[StmtClass]Rule {
	m := make(map[StmtClass]Rule, len(allRuleClasses))
	for _, c := range allRuleClasses {
		if c == keep {
			m[c] = Rule{InTransaction: Keep, OutTransaction: Keep}
		} else {
			m[c] = Rule{InTransaction: Drop, OutTransaction: Drop}
		}
	}
	return m
}

// resolveRules selects the rule matrix for the configured filter mode (R5.15):
//
//   - "" / "custom": the default matrix overlaid with cfg.Rules (R11.4). An
//     empty mode behaves like custom with no overrides.
//   - "write_only":  default matrix but Plain_SELECT dropped in and out of tx.
//   - "all":         every classifiable class kept (Plain_SELECT keep/keep).
//   - "ddl_only":    only CREATE/ALTER/DROP kept.
//   - "dml_only":    only INSERT/UPDATE/DELETE kept.
//
// Any other non-empty value is rejected.
func resolveRules(cfg Config) (map[StmtClass]Rule, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Mode)) {
	case "", "custom":
		m := defaultRules()
		for k, v := range cfg.Rules {
			m[k] = v
		}
		return m, nil
	case "write_only":
		m := defaultRules()
		m[ClassPlainSelect] = Rule{InTransaction: Drop, OutTransaction: Drop}
		return m, nil
	case "all":
		m := defaultRules()
		m[ClassPlainSelect] = Rule{InTransaction: Keep, OutTransaction: Keep}
		return m, nil
	case "ddl_only":
		return onlyKeep(ClassDDL), nil
	case "dml_only":
		return onlyKeep(ClassDML), nil
	default:
		return nil, fmt.Errorf("invalid filter mode %q (want write_only|all|ddl_only|dml_only|custom)", cfg.Mode)
	}
}
