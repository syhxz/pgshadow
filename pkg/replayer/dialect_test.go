package replayer

import (
	"context"
	"sync"
	"testing"

	"pgshadow/pkg/core"
)

// recordingExec records the SQL text of every event it is asked to execute, so
// tests can assert exactly which statements reached the target (and which the
// dialect decorator dropped). It always succeeds.
type recordingExec struct {
	mu   sync.Mutex
	sqls []string
}

func (r *recordingExec) Exec(ctx context.Context, conn core.ConnID, e *core.SQLEvent) error {
	r.mu.Lock()
	r.sqls = append(r.sqls, e.SQL)
	r.mu.Unlock()
	return nil
}

func (r *recordingExec) executed() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.sqls))
	copy(out, r.sqls)
	return out
}

// run feeds a sequence of statements through ex on a single ConnID.
func run(t *testing.T, ex Executor, stmts []string) {
	t.Helper()
	c := connID(1)
	for i, s := range stmts {
		if err := ex.Exec(context.Background(), c, ev(c, uint64(i+1), s)); err != nil {
			t.Fatalf("Exec(%q): %v", s, err)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- isTransactionControl (leading-keyword classification) -------------------

// Transaction-control statements are recognized case-insensitively, after
// skipping leading whitespace and comments; everything else is not (R8.5).
func TestIsTransactionControl(t *testing.T) {
	txControl := []string{
		"BEGIN",
		"begin",
		"  BEGIN TRANSACTION",
		"Begin Work",
		"START TRANSACTION",
		"COMMIT",
		"commit work",
		"END",
		"ROLLBACK",
		"rollback to savepoint sp1",
		"ABORT",
		"-- open a tx\nBEGIN",
		"/* lead */ COMMIT",
		"\t\n  rollback",
	}
	for _, s := range txControl {
		if !isTransactionControl(s) {
			t.Errorf("expected %q to be transaction-control", s)
		}
	}

	notTxControl := []string{
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET x = 1",
		"DELETE FROM t",
		"SELECT run_batch()",
		"CREATE TABLE t (id int)",
		"SET search_path = public",
		"CALL do_work()",
		"COPY t FROM STDIN",
		"",
		"   ",
		"beginning_of_time()", // identifier starting with 'begin' but not the keyword
	}
	for _, s := range notTxControl {
		if isTransactionControl(s) {
			t.Errorf("expected %q to NOT be transaction-control", s)
		}
	}
}

// --- Greenplum dialect (R8.5) ------------------------------------------------

// For greenplum, the decorator drops standalone transaction-control statements
// (so the target never holds a distributed transaction) and forwards DML/DDL
// and other statements to run standalone in autocommit mode (R8.5).
func TestDialectExecutor_GreenplumDropsTxControl(t *testing.T) {
	base := &recordingExec{}
	ex := newDialectExecutor(base, "greenplum")

	stmts := []string{
		"BEGIN",
		"INSERT INTO accounts(id, bal) VALUES (1, 100)",
		"UPDATE accounts SET bal = bal - 10 WHERE id = 1",
		"COMMIT",
		"SELECT run_batch()",
		"CREATE TABLE audit (id int)",
		"ROLLBACK",
		"  begin transaction",
		"DELETE FROM audit",
		"end",
	}
	run(t, ex, stmts)

	// Only the non-transaction-control statements should reach the target,
	// in captured order.
	want := []string{
		"INSERT INTO accounts(id, bal) VALUES (1, 100)",
		"UPDATE accounts SET bal = bal - 10 WHERE id = 1",
		"SELECT run_batch()",
		"CREATE TABLE audit (id int)",
		"DELETE FROM audit",
	}
	if got := base.executed(); !equalStrings(got, want) {
		t.Fatalf("greenplum executed %v, want %v", got, want)
	}
}

// case-insensitive greenplum spelling still selects the dialect decorator.
func TestNewDialectExecutor_GreenplumCaseInsensitive(t *testing.T) {
	base := &recordingExec{}
	ex := newDialectExecutor(base, "  GreenPlum ")
	if _, ok := ex.(*greenplumExecutor); !ok {
		t.Fatalf("expected *greenplumExecutor for greenplum dialect, got %T", ex)
	}
}

// --- PostgreSQL dialect (R8.2) -----------------------------------------------

// For postgresql, the decorator is a pass-through: every statement (including
// transaction-control) reaches the target unchanged, and no wrapper is added.
func TestDialectExecutor_PostgresqlPassesThrough(t *testing.T) {
	base := &recordingExec{}
	ex := newDialectExecutor(base, "postgresql")

	if ex != Executor(base) {
		t.Fatalf("postgresql dialect must not wrap the base executor, got %T", ex)
	}

	stmts := []string{
		"BEGIN",
		"INSERT INTO t VALUES (1)",
		"COMMIT",
		"ROLLBACK",
		"SELECT 1",
	}
	run(t, ex, stmts)
	if got := base.executed(); !equalStrings(got, stmts) {
		t.Fatalf("postgresql executed %v, want all %v", got, stmts)
	}
}

// The empty/default dialect behaves like postgresql (standard execution, R8.2).
func TestDialectExecutor_DefaultPassesThrough(t *testing.T) {
	base := &recordingExec{}
	ex := newDialectExecutor(base, "")
	if ex != Executor(base) {
		t.Fatalf("default dialect must not wrap the base executor, got %T", ex)
	}
	stmts := []string{"BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"}
	run(t, ex, stmts)
	if got := base.executed(); !equalStrings(got, stmts) {
		t.Fatalf("default dialect executed %v, want all %v", got, stmts)
	}
}

// --- Wiring via buildExecutor (innermost decorator) --------------------------

// buildExecutor inserts the dialect decorator as the innermost layer, so a
// greenplum chain drops transaction-control statements before they reach the
// base executor even with the other decorators active. Pacing and rate limiting
// are disabled here so the test exercises only the dialect wiring.
func TestBuildExecutor_GreenplumDropsTxControl(t *testing.T) {
	base := &recordingExec{}
	cfg := Config{
		TargetDialect: "greenplum",
		ErrorPolicy:   Skip,
		// SpeedFactor 0 (ASAP) and RateLimitQPS 0 (unlimited) keep the chain
		// free of timing/throttling for a deterministic test.
	}
	ex := buildExecutor(cfg, base, nil, realSleep)

	run(t, ex, []string{
		"BEGIN",
		"INSERT INTO t VALUES (1)",
		"COMMIT",
		"SELECT proc()",
	})

	want := []string{"INSERT INTO t VALUES (1)", "SELECT proc()"}
	if got := base.executed(); !equalStrings(got, want) {
		t.Fatalf("greenplum chain executed %v, want %v", got, want)
	}
}

// buildExecutor for postgresql leaves all statements flowing to the base.
func TestBuildExecutor_PostgresqlPassesAll(t *testing.T) {
	base := &recordingExec{}
	cfg := Config{TargetDialect: "postgresql", ErrorPolicy: Skip}
	ex := buildExecutor(cfg, base, nil, realSleep)

	stmts := []string{"BEGIN", "INSERT INTO t VALUES (1)", "COMMIT"}
	run(t, ex, stmts)
	if got := base.executed(); !equalStrings(got, stmts) {
		t.Fatalf("postgresql chain executed %v, want all %v", got, stmts)
	}
}
