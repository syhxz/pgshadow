// Package replayer — Greenplum/PostgreSQL dialect handling (task 10.4).
//
// This file adds one Executor decorator that slots into the same composition
// chain built by buildExecutor (task 10.3). It is the INNERMOST decorator,
// wrapping the production poolExecutor directly so dialect-specific shaping
// happens last — right before a statement reaches the Target_Database.
//
// Dialect behavior (R8.2, R8.4, R8.5):
//
//   - postgresql (default): standard execution. The dialect decorator is a
//     pass-through; statements run exactly as routed.
//   - greenplum: avoid acquiring distributed transaction locks on the target.
//     pgshadow never wraps replayed statements in an explicit transaction —
//     each statement reaching poolExecutor runs standalone (autocommit per
//     statement) on the leased connection. On top of that, the decorator drops
//     standalone transaction-control statements (BEGIN/START/COMMIT/ROLLBACK/
//     END/ABORT) so they are never sent to the target. Sending an explicit
//     BEGIN to a Greenplum Master would open a multi-statement distributed
//     transaction and hold distributed locks across segments for the lifetime
//     of the session; skipping these keeps every replayed statement in its own
//     autocommit scope (R8.5).
//
// Replay target (R8.4): the pool (task 10.1) connects only to the configured
// target host, which is the Target_Database Master. No segment fan-out is
// performed here or anywhere in the replayer — replay targets the Master only.
// This decorator does not change the connection target; it only shapes which
// statements are executed and ensures no explicit transaction is opened.
//
// To keep dependencies minimal and avoid any import of pkg/filter (which would
// be acyclic but heavier than needed), transaction-control statements are
// recognized with a tiny local leading-keyword scan that skips leading
// whitespace and SQL comments. This mirrors the leading-keyword spellings the
// filter's classifier maps to BEGIN/COMMIT-ROLLBACK.
package replayer

import (
	"context"
	"strings"

	"pgshadow/pkg/core"
)

// txControlKeywords is the set of leading keywords that begin a standalone
// transaction-control statement. For the greenplum dialect these are dropped
// rather than replayed so the target never opens a held distributed
// transaction (R8.5). The spellings mirror the filter classifier's mapping of
// BEGIN/START → begin and COMMIT/ROLLBACK/END → commit-rollback, plus ABORT
// (a PostgreSQL/Greenplum synonym for ROLLBACK).
var txControlKeywords = map[string]bool{
	"begin":    true, // BEGIN [TRANSACTION|WORK]
	"start":    true, // START TRANSACTION
	"commit":   true, // COMMIT [TRANSACTION|WORK]
	"end":      true, // END — synonym for COMMIT
	"rollback": true, // ROLLBACK [TO SAVEPOINT ...]
	"abort":    true, // ABORT — synonym for ROLLBACK
}

// greenplumExecutor is the dialect decorator for Target_Dialect=greenplum. It
// drops standalone transaction-control statements (so the target never holds a
// distributed transaction) and delegates every other statement to next, which
// runs it standalone in autocommit mode (R8.5).
type greenplumExecutor struct {
	next Executor
}

// Exec drops transaction-control statements and otherwise delegates. Dropped
// statements return nil (success) so the lane proceeds to the next event in
// captured order, exactly as if the statement had been replayed.
func (g *greenplumExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	if isTransactionControl(ev.SQL) {
		// Skip: do not send BEGIN/COMMIT/ROLLBACK to the Greenplum Master, so
		// no multi-statement distributed transaction (and its distributed
		// locks) is ever opened (R8.5).
		return nil
	}
	// Standard delegation: poolExecutor runs the statement standalone on the
	// affinity-leased connection — autocommit per statement (R8.5).
	return g.next.Exec(ctx, conn, ev)
}

// newDialectExecutor wraps next with dialect-specific behavior. For greenplum
// it returns a greenplumExecutor; for postgresql (and the default/empty value,
// R8.2) it returns next unchanged so execution is standard. The result is
// inserted as the innermost decorator by buildExecutor.
func newDialectExecutor(next Executor, dialect string) Executor {
	if strings.EqualFold(strings.TrimSpace(dialect), "greenplum") {
		return &greenplumExecutor{next: next}
	}
	return next
}

// isTransactionControl reports whether sql is a standalone transaction-control
// statement (BEGIN/START/COMMIT/ROLLBACK/END/ABORT), matched case-insensitively
// on the leading keyword after skipping leading whitespace and SQL comments.
func isTransactionControl(sql string) bool {
	return txControlKeywords[leadingKeyword(sql)]
}

// leadingKeyword returns the lowercased leading word of sql, skipping leading
// whitespace, line comments (-- ...) and block comments (/* ... */, nestable).
// It returns "" when no leading word is present. This is a deliberately tiny
// local scan (no pkg/filter dependency) sufficient to recognize the handful of
// transaction-control keywords.
func leadingKeyword(sql string) string {
	i := skipLeadingNoise(sql)
	n := len(sql)
	start := i
	for i < n {
		c := sql[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			i++
			continue
		}
		break
	}
	return strings.ToLower(sql[start:i])
}

// skipLeadingNoise returns the index of the first byte of sql that is not
// leading whitespace or a comment.
func skipLeadingNoise(sql string) int {
	i, n := 0, len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			i += 2
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			i += 2
			depth := 1
			for i < n && depth > 0 {
				if i+1 < n && sql[i] == '/' && sql[i+1] == '*' {
					depth++
					i += 2
					continue
				}
				if i+1 < n && sql[i] == '*' && sql[i+1] == '/' {
					depth--
					i += 2
					continue
				}
				i++
			}
		default:
			return i
		}
	}
	return i
}
