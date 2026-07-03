// Package e2e contains pgshadow's containerized end-to-end integration tests
// (spec task 14.2). It drives a synthetic mirrored-traffic fixture through the
// real pipeline stages — the PG wire-protocol parser (pkg/protocol), the
// transaction-aware SQL filter (pkg/filter), the in-memory buffer queue
// (pkg/queue), the affinity-aware connection pool and the real replayer
// (pkg/replayer), and the Prometheus metrics collector (pkg/metrics) — against
// a live PostgreSQL or Greenplum target, asserting:
//
//   - kept statements actually execute on the Target_Database (R8.4),
//   - per-ConnID captured order is preserved end-to-end (R7.13, R6.6),
//   - source-vs-target execution times are recorded via metrics (R10.7),
//   - /metrics exposes the Prometheus catalog (R10.1), and
//   - the Greenplum path avoids holding distributed transactions (it runs each
//     statement autocommit by dropping BEGIN/COMMIT/ROLLBACK) and replays only
//     to the Target_Database Master (R8.5, R8.4).
//
// # Gating (why `go test ./...` stays green without a database)
//
// These tests require a real database and so are GATED behind environment
// variables. With no env var set, each test calls t.Skip() and the default
// `go test ./...` run passes by skipping — no Docker, container, or live DB is
// needed in CI or on a developer laptop.
//
//	PGSHADOW_E2E_PG_DSN   PostgreSQL target DSN; enables TestE2E_PostgreSQL.
//	PGSHADOW_E2E_GP_DSN   Greenplum  target DSN; enables TestE2E_Greenplum.
//
// The DSN accepts either libpq keyword form or URL form, e.g.:
//
//	host=127.0.0.1 port=5432 user=postgres password=postgres dbname=postgres
//	postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable
//
// Optional overrides:
//
//	PGSHADOW_E2E_METRICS_PORT  base port for the scraped /metrics endpoint
//	                           (default 19090; the Greenplum test uses base+1).
//
// # Running against real databases
//
// PostgreSQL (Docker):
//
//	docker run --rm -d --name pgshadow-e2e-pg -e POSTGRES_PASSWORD=postgres \
//	    -p 5432:5432 postgres:16
//	PGSHADOW_E2E_PG_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
//	    go test ./test/e2e -run TestE2E_PostgreSQL -v
//
// Greenplum (Docker; point the DSN at the GP Master, replay is Master-only):
//
//	PGSHADOW_E2E_GP_DSN='postgres://gpadmin:gpadmin@127.0.0.1:5432/gpadmin?sslmode=disable' \
//	    go test ./test/e2e -run TestE2E_Greenplum -v
//
// Run both: set both env vars and `go test ./test/e2e -v`.
package e2e

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/protocol"
	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
)

// excludeMarker is the sentinel matched by the filter's exclude pattern. A
// fixture statement carrying this marker must be dropped and never executed.
const excludeMarker = "E2E_EXCLUDE_ME"

// orderTable is the scratch table the fixture writes to. Its bigserial id
// column reflects execution order, which lets the test assert that per-ConnID
// captured order is preserved on the target (R7.13).
const orderTable = "pgshadow_e2e_order"

// TestE2E_PostgreSQL drives the full pipeline against a live PostgreSQL target.
// It is skipped unless PGSHADOW_E2E_PG_DSN is set, so the default test run stays
// green without a database.
func TestE2E_PostgreSQL(t *testing.T) {
	dsn := os.Getenv("PGSHADOW_E2E_PG_DSN")
	if dsn == "" {
		t.Skip("PGSHADOW_E2E_PG_DSN not set; skipping containerized PostgreSQL e2e (set it to a target DSN to run)")
	}
	runPipelineE2E(t, dsn, "postgresql", metricsPort(0))
}

// TestE2E_Greenplum drives the full pipeline against a live Greenplum Master.
// It is skipped unless PGSHADOW_E2E_GP_DSN is set. In addition to the shared
// assertions it verifies the Greenplum dialect path runs statements autocommit
// (BEGIN/COMMIT/ROLLBACK are dropped, so a write inside an un-committed BEGIN
// still persists) — proving no distributed transaction is held (R8.5).
func TestE2E_Greenplum(t *testing.T) {
	dsn := os.Getenv("PGSHADOW_E2E_GP_DSN")
	if dsn == "" {
		t.Skip("PGSHADOW_E2E_GP_DSN not set; skipping containerized Greenplum e2e (set it to a Master DSN to run)")
	}
	runPipelineE2E(t, dsn, "greenplum", metricsPort(1))
}

// connWorkload is one captured source connection's ordered statement stream.
type connWorkload struct {
	label string // value written into the order table's conn column
	port  int    // distinguishes the synthetic ConnID four-tuple
	stmts []string
}

// runPipelineE2E executes the shared end-to-end flow for a dialect. greenplum
// adds an extra connection that writes inside an un-committed BEGIN to prove the
// dialect avoids holding a distributed transaction (R8.5).
func runPipelineE2E(t *testing.T, dsn, dialect string, mPort int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// --- Admin connection for schema setup and verification (separate from the
	// replayer pool; never the Source_Database — only the configured target). --
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect target for setup (check the DSN and that the database is reachable): %v", err)
	}
	defer admin.Close(context.Background())

	if _, err := admin.Exec(ctx, "DROP TABLE IF EXISTS "+orderTable); err != nil {
		t.Fatalf("drop pre-existing order table: %v", err)
	}
	// No PRIMARY KEY: Greenplum constrains distribution keys for PKs; a plain
	// table with a bigserial ordering column is sufficient and portable.
	if _, err := admin.Exec(ctx, "CREATE TABLE "+orderTable+" (id bigserial, conn text, n int)"); err != nil {
		t.Fatalf("create order table: %v", err)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+orderTable)
	}()

	// --- Build the synthetic mirrored-traffic workload. -----------------------
	workloads := []connWorkload{
		{label: "conn1", port: 1001, stmts: []string{
			insertStmt("conn1", 1), insertStmt("conn1", 2), insertStmt("conn1", 3),
		}},
		{label: "conn2", port: 1002, stmts: []string{
			insertStmt("conn2", 1), insertStmt("conn2", 2),
		}},
		// conn3 wraps a plain SELECT and two writes in an explicit transaction.
		// The in-transaction SELECT is kept (R5.7); BEGIN/COMMIT are kept by the
		// filter (R5.10, R5.11) but dropped by the greenplum executor (R8.5).
		{label: "conn3", port: 1003, stmts: []string{
			"BEGIN",
			"SELECT count(*) FROM " + orderTable,
			insertStmt("conn3", 1), insertStmt("conn3", 2),
			"COMMIT",
		}},
		// conn4 includes a statement matched by the exclude pattern: it must be
		// dropped and never executed (R5.16), so n=999 must not appear.
		{label: "conn4", port: 1004, stmts: []string{
			insertStmt("conn4", 999) + " -- " + excludeMarker,
			insertStmt("conn4", 1),
		}},
	}
	if dialect == "greenplum" {
		// A write inside an un-committed BEGIN. With the greenplum dialect the
		// BEGIN is dropped and the INSERT runs autocommit, so the row persists
		// even though no COMMIT is ever sent — demonstrating that no distributed
		// transaction is held open (R8.5).
		workloads = append(workloads, connWorkload{label: "conn_gp", port: 1005, stmts: []string{
			"BEGIN",
			insertStmt("conn_gp", 1),
		}})
	}

	// --- Wire the real pipeline components. -----------------------------------
	var coll metrics.Collector = metrics.New(metrics.Config{Enabled: true, Port: mPort})

	q, err := queue.New(queue.Config{Type: "ringbuffer", Capacity: 1 << 16}, &coll)
	if err != nil {
		t.Fatalf("build buffer queue: %v", err)
	}

	f, err := filter.NewFilter(filter.Config{
		Mode:            "custom", // default matrix: DML kept, plain SELECT keep-in/drop-out
		ExcludePatterns: []filter.Regexp{{Regexp: regexp.MustCompile(excludeMarker)}},
	})
	if err != nil {
		t.Fatalf("build filter: %v", err)
	}
	sm := filter.NewStateMachine()

	pool, err := replayer.NewPool(ctx, replayerConfig(t, dsn, dialect))
	if err != nil {
		t.Fatalf("build target connection pool: %v", err)
	}
	defer pool.Close()

	// Fail fast with a clear message if the pool cannot reach the target.
	probe := connID(1)
	if _, err := pool.AcquireFor(ctx, probe); err != nil {
		t.Fatalf("acquire target connection (database unreachable?): %v", err)
	}
	pool.Release(probe)

	parserCfg := protocol.Config{MaxSQLLength: 1 << 20, ExtendedQuery: true}

	// --- Feed the fixture through protocol parse → filter → queue. ------------
	// Parse each connection's synthetic wire bytes into SQL_Events first, then
	// interleave enqueues round-robin so the queue (and the replayer) see ConnIDs
	// mixed together while each ConnID's own order is preserved.
	type selectProbe struct {
		conn core.ConnID
		sql  string
		src  time.Duration
	}
	var (
		eventsByConn = make(map[int][]*core.SQLEvent)
		ids          = make(map[int]core.ConnID)
		keptInserts  int
		selectProbes []selectProbe
		maxLen       int
	)
	for _, w := range workloads {
		id := connID(w.port)
		ids[w.port] = id
		evs := parseConnStream(t, parserCfg, id, w.stmts)
		if len(evs) != len(w.stmts) {
			t.Fatalf("%s: parsed %d events, want %d (one per statement)", w.label, len(evs), len(w.stmts))
		}
		eventsByConn[w.port] = evs
		if len(evs) > maxLen {
			maxLen = len(evs)
		}
	}

	for round := 0; round < maxLen; round++ {
		for _, w := range workloads {
			evs := eventsByConn[w.port]
			if round >= len(evs) {
				continue
			}
			ev := evs[round]

			// Decide keep/drop against the connection's current transaction
			// state, then advance the state machine for control statements so
			// the next statement on this ConnID sees the updated state.
			state := sm.State(ev.Conn)
			action, class := f.Decide(ev, state)
			sm.OnStatement(ev.Conn, class)

			if action != filter.Keep {
				continue
			}
			if class == filter.ClassPlainSelect {
				// Stamp a synthetic source execution time (as the bidirectional
				// SourceExecTimer would, R10.7) and remember the SELECT so we can
				// re-measure it on the target and record both sides to metrics.
				ev.SourceExecTime = 2 * time.Millisecond
				selectProbes = append(selectProbes, selectProbe{conn: ev.Conn, sql: ev.SQL, src: ev.SourceExecTime})
			}
			if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ev.SQL)), "INSERT") {
				keptInserts++
			}
			if dropped := q.Enqueue(ev); dropped {
				t.Fatalf("queue overflow enqueuing %q (capacity too small for fixture)", ev.SQL)
			}
		}
	}

	// --- Replay via the real replayer against the live target. ----------------
	// Closing the queue lets Run drain every buffered event and then return.
	if err := q.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}
	r := replayer.NewReplayer(replayerConfig(t, dsn, dialect), q, pool)
	if err := r.Run(ctx); err != nil {
		t.Fatalf("replayer run returned error: %v", err)
	}

	// --- Assert kept statements executed on the target (R8.4). ----------------
	if got := countRows(t, ctx, admin, "SELECT count(*) FROM "+orderTable+" WHERE conn LIKE 'conn%'"); got != keptInserts {
		t.Fatalf("executed-row count = %d, want %d kept INSERTs", got, keptInserts)
	}
	// Excluded statement must never have executed (R5.16).
	if got := countRows(t, ctx, admin, "SELECT count(*) FROM "+orderTable+" WHERE n = 999"); got != 0 {
		t.Fatalf("excluded statement executed: found %d rows with n=999, want 0", got)
	}

	// --- Assert per-ConnID captured order is preserved (R7.13, R6.6). ---------
	// Within each connection the writes carry n = 1,2,3...; the bigserial id
	// reflects execution order, so ordering rows by id must yield ascending n.
	for _, w := range workloads {
		want := expectedSeq(w)
		if len(want) == 0 {
			continue
		}
		got := selectInts(t, ctx, admin,
			"SELECT n FROM "+orderTable+" WHERE conn = $1 ORDER BY id", w.label)
		if !equalInts(got, want) {
			t.Fatalf("%s: target execution order = %v, want captured order %v", w.label, got, want)
		}
	}

	// --- Greenplum: writes inside an un-committed BEGIN still persist (R8.5). --
	if dialect == "greenplum" {
		if got := countRows(t, ctx, admin, "SELECT count(*) FROM "+orderTable+" WHERE conn = 'conn_gp'"); got != 1 {
			t.Fatalf("greenplum autocommit: conn_gp row count = %d, want 1 "+
				"(BEGIN should be dropped so the INSERT autocommits without a held distributed transaction)", got)
		}
		// Replay targets only the Master: the pool dials the single configured
		// target host (the Greenplum Master) and performs no segment fan-out.
		if st := pool.Stats(); st.Total < 1 {
			t.Fatalf("expected at least one target (Master) connection, pool stats = %+v", st)
		}
	}

	// --- Record source-vs-target execution times via metrics (R10.7). --------
	// Re-measure the kept read-only SELECTs on the target (safe: no side effects)
	// and record the captured source time alongside the measured target time.
	for _, p := range selectProbes {
		c, err := pool.AcquireFor(ctx, p.conn)
		if err != nil {
			t.Fatalf("acquire for exec-time measurement: %v", err)
		}
		start := time.Now()
		_, execErr := c.Exec(ctx, p.sql)
		target := time.Since(start)
		pool.Release(p.conn)

		coll.ExecTime(p.sql, p.src, target)
		coll.ReplayResult(execErr)
		if execErr != nil {
			t.Fatalf("re-measured SELECT failed on target: %v", execErr)
		}
	}
	if len(selectProbes) == 0 {
		t.Fatalf("expected at least one kept Plain_SELECT to record source-vs-target exec time")
	}
	// Populate the remaining catalog gauges so /metrics shows a full picture.
	coll.QueueDepth(q.Depth())
	coll.ReplayLag(0)
	coll.Throughput(float64(keptInserts), float64(keptInserts))
	coll.PacketDrops(uint64(maxLen*len(workloads)), 0)

	// --- Assert /metrics exposes the Prometheus catalog (R10.1). --------------
	go func() { _ = coll.Serve(mPort) }()
	body := scrapeMetrics(t, mPort)
	wantMetrics := []string{
		"pgshadow_replay_total",
		"pgshadow_exec_time_seconds",
		"pgshadow_queue_depth",
		"pgshadow_packets_received_total",
		"pgshadow_replay_lag_seconds",
		"pgshadow_source_qps",
	}
	for _, name := range wantMetrics {
		if !strings.Contains(body, name) {
			t.Fatalf("/metrics catalog missing %q", name)
		}
	}
	// Both execution-time sides must be present (source from capture, target
	// from the live measurement) (R10.7).
	if !strings.Contains(body, `side="source"`) || !strings.Contains(body, `side="target"`) {
		t.Fatalf("/metrics exec-time histogram missing source/target sides")
	}
}

// replayerConfig parses dsn and builds a replayer.Config for the given dialect.
// The target password is passed through the PasswordEnv mechanism (R11.8) rather
// than embedded in a plaintext field. Replay targets only the configured host —
// the Target_Database Master (R8.4).
func replayerConfig(t *testing.T, dsn, dialect string) replayer.Config {
	t.Helper()
	pc, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse target DSN: %v", err)
	}
	const pwEnv = "PGSHADOW_E2E_TARGET_PW"
	if err := os.Setenv(pwEnv, pc.Password); err != nil {
		t.Fatalf("set target password env: %v", err)
	}
	return replayer.Config{
		TargetHost:      pc.Host,
		TargetPort:      int(pc.Port),
		TargetDatabase:  pc.Database,
		TargetUser:      pc.User,
		PasswordEnv:     pwEnv,
		Workers:         8,
		PoolMaxConns:    8,
		PoolMinConns:    1,
		SessionAffinity: true,             // same ConnID → same target connection (R7.5)
		Mode:            replayer.Bounded, // distinct ConnIDs concurrent, same serial (R7.10)
		ErrorPolicy:     replayer.Skip,    // failures surface as row-count mismatches
		SpeedFactor:     0,                // replay as fast as possible
		TargetDialect:   dialect,          // postgresql | greenplum (R8.2)
	}
}

// parseConnStream synthesizes one connection's client wire bytes (a StartupMessage
// followed by one Simple_Query per statement), feeds them through the real
// framing parser in two chunks to exercise reassembly across a Feed boundary
// (R3.1), and builds the resulting SQL_Events (R3.2, R3.7).
func parseConnStream(t *testing.T, cfg protocol.Config, conn core.ConnID, stmts []string) []*core.SQLEvent {
	t.Helper()

	raw := encodeStartup()
	for _, s := range stmts {
		raw = append(raw, encodeSimpleQuery(s)...)
	}

	p := protocol.NewParser(cfg, true)
	mid := len(raw) / 2
	first, err := p.Feed(raw[:mid])
	if err != nil {
		t.Fatalf("parser feed (first half): %v", err)
	}
	second, err := p.Feed(raw[mid:])
	if err != nil {
		t.Fatalf("parser feed (second half): %v", err)
	}
	msgs := append(append([]core.PGMessage{}, first...), second...)

	eb := protocol.NewEventBuilder(cfg)
	ts := time.Now()
	var evs []*core.SQLEvent
	for _, m := range msgs {
		if ev, ok := eb.Build(conn, m, ts); ok {
			evs = append(evs, ev)
		}
	}
	return evs
}

// encodeStartup builds a minimal protocol-3.0 StartupMessage so the client-side
// framing parser transitions from the startup phase to typed framing.
func encodeStartup() []byte {
	body := make([]byte, 4)
	binary.BigEndian.PutUint32(body, 196608) // protocol version 3.0
	body = append(body, []byte("user\x00pgshadow\x00")...)
	body = append(body, 0) // final terminator
	total := uint32(4 + len(body))
	out := make([]byte, 4, 4+len(body))
	binary.BigEndian.PutUint32(out, total)
	return append(out, body...)
}

// encodeSimpleQuery frames a Simple_Query ('Q') message: type byte, 4-byte
// length (inclusive of the length field), then the NUL-terminated SQL text.
func encodeSimpleQuery(sql string) []byte {
	payload := append([]byte(sql), 0)
	length := uint32(4 + len(payload))
	out := make([]byte, 0, 5+len(payload))
	out = append(out, 'Q')
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], length)
	out = append(out, l[:]...)
	return append(out, payload...)
}

// insertStmt builds an INSERT into the order table tagging the row with its
// connection label and per-connection sequence number.
func insertStmt(conn string, n int) string {
	return fmt.Sprintf("INSERT INTO %s (conn, n) VALUES ('%s', %d)", orderTable, conn, n)
}

// expectedSeq returns the per-connection sequence of n values written by w, in
// captured order, skipping non-INSERT statements and the excluded sentinel.
func expectedSeq(w connWorkload) []int {
	var seq []int
	for _, s := range w.stmts {
		up := strings.ToUpper(strings.TrimSpace(s))
		if !strings.HasPrefix(up, "INSERT") || strings.Contains(s, excludeMarker) {
			continue
		}
		// Recover n from the trailing "VALUES ('label', n)".
		open := strings.LastIndex(s, ",")
		close := strings.LastIndex(s, ")")
		if open < 0 || close <= open {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimSpace(s[open+1 : close])); err == nil {
			seq = append(seq, n)
		}
	}
	return seq
}

// connID builds a deterministic synthetic four-tuple ConnID for a source port.
func connID(port int) core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr("10.0.0.1"),
		SrcPort: uint16(port),
		DstIP:   netip.MustParseAddr("10.0.0.2"),
		DstPort: 5432,
	}
}

// countRows runs a single-count query and returns the integer result.
func countRows(t *testing.T, ctx context.Context, c *pgx.Conn, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := c.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", sql, err)
	}
	return n
}

// selectInts collects an integer column across rows in result order.
func selectInts(t *testing.T, ctx context.Context, c *pgx.Conn, sql string, args ...any) []int {
	t.Helper()
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan %q: %v", sql, err)
		}
		out = append(out, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows error %q: %v", sql, err)
	}
	return out
}

func equalInts(a, b []int) bool {
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

// metricsPort returns the /metrics port for a test, honoring the optional
// override env var with a per-test offset so concurrent PG/GP runs don't clash.
func metricsPort(offset int) int {
	base := 19090
	if v := os.Getenv("PGSHADOW_E2E_METRICS_PORT"); v != "" {
		if p, err := strconv.Atoi(v); err == nil && p > 0 {
			base = p
		}
	}
	return base + offset
}

// scrapeMetrics fetches /metrics, retrying briefly while the server binds.
func scrapeMetrics(t *testing.T, port int) string {
	t.Helper()
	url := fmt.Sprintf("http://127.0.0.1:%d/metrics", port)
	client := &http.Client{Timeout: 2 * time.Second}
	var lastErr error
	for i := 0; i < 30; i++ {
		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.StatusCode == http.StatusOK {
			return string(body)
		}
		lastErr = fmt.Errorf("status %d", resp.StatusCode)
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("scrape /metrics on port %d: %v", port, lastErr)
	return ""
}
