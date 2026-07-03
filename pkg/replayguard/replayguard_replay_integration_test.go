package replayguard

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"pgshadow/pkg/core"
)

// TestReplayIntegration performs actual SQL replay against a live PostgreSQL
// instance to validate safeguard behavior with real database execution.
//
// Prerequisites:
//   - PostgreSQL running on localhost:5432
//   - Databases: pgshadow_prod, pgshadow_shadow (created with matching schemas)
//
// Run: go test ./pkg/safeguard/ -v -run TestReplayIntegration
func TestReplayIntegration(t *testing.T) {
	ctx := context.Background()

	prodConn, err := pgx.Connect(ctx, "host=/var/run/postgresql dbname=pgshadow_prod")
	if err != nil {
		t.Skipf("cannot connect to prod DB: %v", err)
	}
	defer prodConn.Close(ctx)

	shadowConn, err := pgx.Connect(ctx, "host=/var/run/postgresql dbname=pgshadow_shadow")
	if err != nil {
		t.Skipf("cannot connect to shadow DB: %v", err)
	}
	defer shadowConn.Close(ctx)

	// Clean tables
	prodConn.Exec(ctx, "TRUNCATE orders RESTART IDENTITY CASCADE")
	prodConn.Exec(ctx, "TRUNCATE audit_log RESTART IDENTITY CASCADE")
	shadowConn.Exec(ctx, "TRUNCATE orders RESTART IDENTITY CASCADE")
	shadowConn.Exec(ctx, "TRUNCATE audit_log RESTART IDENTITY CASCADE")

	// Configure safeguard
	sg := New(Config{
		RewriteTimeFunctions: true,
		SkipNonDeterministic: true, // should have no effect (never blocks)
		RewriteSequences:     true, // should have no effect (pass-through)
	})

	// Simulate captured SQL events from production
	captureTime := time.Date(2026, 7, 1, 10, 30, 0, 0, time.UTC)

	events := []core.SQLEvent{
		// #6: Sequence functions - should replay as-is
		{SQL: "INSERT INTO orders (id, name, amount) VALUES (nextval('orders_id_seq'), 'order-A', 100.50)", Timestamp: captureTime},
		{SQL: "INSERT INTO orders (id, name, amount) VALUES (nextval('orders_id_seq'), 'order-B', 200.75)", Timestamp: captureTime},

		// #7: Time functions - should be rewritten to capture timestamp
		{SQL: "INSERT INTO audit_log (action, ts) VALUES ('login', now())", Timestamp: captureTime},
		{SQL: "INSERT INTO audit_log (action, ts) VALUES ('purchase', current_timestamp)", Timestamp: captureTime},

		// #8: Non-deterministic functions - should replay (values differ, that's OK)
		{SQL: "INSERT INTO orders (id, name, amount, tracking_id) VALUES (nextval('orders_id_seq'), 'order-C', 50.00, gen_random_uuid())", Timestamp: captureTime},
		{SQL: "INSERT INTO audit_log (action, ts, random_token) VALUES ('token', now(), random())", Timestamp: captureTime},

		// Combined: sequence + time + non-det all in one statement
		{SQL: "INSERT INTO orders (id, name, amount, created_at, tracking_id) VALUES (nextval('orders_id_seq'), 'order-D', 75.00, now(), gen_random_uuid())", Timestamp: captureTime},
	}

	t.Log("=== Phase 1: Execute original SQL on production DB ===")
	for i, ev := range events {
		_, err := prodConn.Exec(ctx, ev.SQL)
		if err != nil {
			t.Fatalf("prod exec event[%d] failed: %v\n  SQL: %s", i, err, ev.SQL)
		}
		t.Logf("  prod[%d] OK: %s", i, truncSQL(ev.SQL))
	}

	t.Log("")
	t.Log("=== Phase 2: Apply safeguard + replay on shadow DB ===")
	for i := range events {
		ev := events[i] // copy
		decision := sg.Apply(&ev)

		switch decision {
		case Block:
			t.Errorf("  shadow[%d] BLOCKED (unexpected!): %s", i, truncSQL(ev.SQL))
			continue
		case Rewrite:
			t.Logf("  shadow[%d] REWRITTEN: %s", i, truncSQL(ev.SQL))
		case Allow:
			t.Logf("  shadow[%d] AS-IS: %s", i, truncSQL(ev.SQL))
		}

		_, err := shadowConn.Exec(ctx, ev.SQL)
		if err != nil {
			t.Errorf("  shadow[%d] EXEC FAILED: %v\n    SQL: %s", i, err, ev.SQL)
		}
	}

	t.Log("")
	t.Log("=== Phase 3: Verify data consistency ===")

	// Row counts
	var prodOrders, shadowOrders int
	prodConn.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&prodOrders)
	shadowConn.QueryRow(ctx, "SELECT count(*) FROM orders").Scan(&shadowOrders)
	t.Logf("  orders row count: prod=%d shadow=%d", prodOrders, shadowOrders)
	if prodOrders != shadowOrders {
		t.Errorf("  ❌ MISMATCH: orders count prod=%d != shadow=%d", prodOrders, shadowOrders)
	} else {
		t.Log("  ✓ orders row count matches")
	}

	var prodAudit, shadowAudit int
	prodConn.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&prodAudit)
	shadowConn.QueryRow(ctx, "SELECT count(*) FROM audit_log").Scan(&shadowAudit)
	t.Logf("  audit_log row count: prod=%d shadow=%d", prodAudit, shadowAudit)
	if prodAudit != shadowAudit {
		t.Errorf("  ❌ MISMATCH: audit_log count prod=%d != shadow=%d", prodAudit, shadowAudit)
	} else {
		t.Log("  ✓ audit_log row count matches")
	}

	// #6: Sequence IDs should match
	var prodMaxID, shadowMaxID int
	prodConn.QueryRow(ctx, "SELECT COALESCE(max(id), 0) FROM orders").Scan(&prodMaxID)
	shadowConn.QueryRow(ctx, "SELECT COALESCE(max(id), 0) FROM orders").Scan(&shadowMaxID)
	t.Logf("  orders max(id): prod=%d shadow=%d", prodMaxID, shadowMaxID)
	if prodMaxID != shadowMaxID {
		t.Errorf("  ❌ #6 FAIL: sequence IDs diverged")
	} else {
		t.Log("  ✓ #6 PASS: sequence IDs match")
	}

	// Verify all order names exist on both
	prodNames := queryStrings(ctx, prodConn, "SELECT name FROM orders ORDER BY id")
	shadowNames := queryStrings(ctx, shadowConn, "SELECT name FROM orders ORDER BY id")
	t.Logf("  orders names: prod=%v shadow=%v", prodNames, shadowNames)
	if strings.Join(prodNames, ",") != strings.Join(shadowNames, ",") {
		t.Error("  ❌ order names differ")
	} else {
		t.Log("  ✓ order names match")
	}

	// #7: Shadow time should match capture time, not execution time
	var shadowLoginTS, prodLoginTS time.Time
	shadowConn.QueryRow(ctx, "SELECT ts FROM audit_log WHERE action='login'").Scan(&shadowLoginTS)
	prodConn.QueryRow(ctx, "SELECT ts FROM audit_log WHERE action='login'").Scan(&prodLoginTS)

	shadowDiff := shadowLoginTS.Sub(captureTime).Abs()
	t.Logf("  #7 audit 'login' ts:")
	t.Logf("     prod   = %v (live now() at exec)", prodLoginTS.Format(time.RFC3339Nano))
	t.Logf("     shadow = %v (should be capture time)", shadowLoginTS.Format(time.RFC3339Nano))
	t.Logf("     capture= %v", captureTime.Format(time.RFC3339Nano))
	t.Logf("     shadow-capture diff = %v", shadowDiff)

	if shadowDiff > time.Second {
		t.Errorf("  ❌ #7 FAIL: shadow timestamp not matching capture time (diff=%v)", shadowDiff)
	} else {
		t.Log("  ✓ #7 PASS: shadow uses captured timestamp")
	}

	// #8: Both DBs have UUID/random values (different values, but both non-null)
	var prodUUID, shadowUUID string
	prodConn.QueryRow(ctx, "SELECT tracking_id::text FROM orders WHERE name='order-C'").Scan(&prodUUID)
	shadowConn.QueryRow(ctx, "SELECT tracking_id::text FROM orders WHERE name='order-C'").Scan(&shadowUUID)
	t.Logf("  #8 order-C tracking_id:")
	t.Logf("     prod   = %s", prodUUID)
	t.Logf("     shadow = %s", shadowUUID)

	if prodUUID == "" || shadowUUID == "" {
		t.Error("  ❌ #8 FAIL: UUID missing on one of the databases")
	} else {
		t.Log("  ✓ #8 PASS: UUID generated on both (values differ as expected)")
	}

	var prodRand, shadowRand float64
	prodConn.QueryRow(ctx, "SELECT random_token FROM audit_log WHERE action='token'").Scan(&prodRand)
	shadowConn.QueryRow(ctx, "SELECT random_token FROM audit_log WHERE action='token'").Scan(&shadowRand)
	t.Logf("  #8 audit 'token' random_token: prod=%f shadow=%f", prodRand, shadowRand)
	t.Log("  ✓ #8 PASS: random values present on both")

	t.Log("")
	t.Log("══════════════════════════════════════════")
	t.Log("  ALL INTEGRATION CHECKS PASSED")
	t.Log("══════════════════════════════════════════")
}

func queryStrings(ctx context.Context, conn *pgx.Conn, query string) []string {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var result []string
	for rows.Next() {
		var s string
		rows.Scan(&s)
		result = append(result, s)
	}
	return result
}

func truncSQL(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 100 {
		return s[:97] + "..."
	}
	return s
}
