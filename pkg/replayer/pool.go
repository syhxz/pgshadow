// Package replayer — affinity-aware connection pool (task 10.1).
//
// This file implements the Pool interface declared in replayer.go. It wraps
// pgxpool with MaxConns/MinConns and health checks (R9.1, R9.2, R9.3),
// discards and replaces dead connections (R9.5), blocks-or-errors when the
// pool is exhausted at its maximum (R9.6), and maps the same source ConnID to
// the same target connection under session affinity (R7.5).
//
// To keep the affinity / lease / replacement logic unit-testable WITHOUT a
// live database, the core leasing logic operates over two small internal
// interfaces (connSource and connHandle) rather than directly over the
// concrete pgx types. The production path supplies a pgxpool-backed source
// (pgxConnSource); tests inject a fake source. Only the thin public ConnPool
// wrapper depends on *pgxpool.Conn.
package replayer

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"pgshadow/pkg/core"
)

// defaultHealthCheckPeriod is how often pgxpool probes idle connections for
// liveness so that dead connections are proactively replaced (R9.5).
const defaultHealthCheckPeriod = 30 * time.Second

// connHandle abstracts a single leased connection. It is satisfied by the
// production pgx lease (pgxLease) and by test fakes, so the affinity logic can
// be exercised without a live database.
type connHandle interface {
	// Ping reports whether the underlying connection is still usable. A
	// non-nil error means the connection is dead and must be replaced (R9.5).
	Ping(ctx context.Context) error
	// Release returns the connection to its source pool.
	Release()
}

// connSource abstracts the underlying connection pool. The production
// implementation wraps *pgxpool.Pool; tests inject a fake. Acquire honors the
// supplied context: when the source is exhausted at its maximum it blocks
// until a connection frees or the context is done (R9.6).
type connSource interface {
	Acquire(ctx context.Context) (connHandle, error)
	Stat() PoolStats
	Close()
}

// affinityPool holds the database-independent leasing logic. It maps a source
// ConnID to the target connection currently leased for it (R7.5) and replaces
// dead connections on demand (R9.5).
type affinityPool struct {
	src             connSource
	sessionAffinity bool

	mu     sync.Mutex
	leases map[core.ConnID]connHandle
	closed bool
}

func newAffinityPool(src connSource, sessionAffinity bool) *affinityPool {
	return &affinityPool{
		src:             src,
		sessionAffinity: sessionAffinity,
		leases:          make(map[core.ConnID]connHandle),
	}
}

// acquireFor leases a connection for conn. Under session affinity, repeated
// calls for the same ConnID return the same connection until it is released or
// found dead (R7.5, R7.10). A dead leased connection is discarded and replaced
// transparently (R9.5). When the underlying source is exhausted, the call
// blocks until a connection frees or ctx is done, surfacing the context error
// (R9.6).
func (a *affinityPool) acquireFor(ctx context.Context, conn core.ConnID) (connHandle, error) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, fmt.Errorf("connection pool is closed")
	}

	// Fast path: an existing affinity lease can be reused if it is still alive.
	// The handle reference is copied under the lock, then Ping is called
	// outside the lock so a slow/unreachable target does not block acquireFor
	// for other ConnIDs.
	var existing connHandle
	if a.sessionAffinity {
		existing = a.leases[conn]
	}
	a.mu.Unlock()

	if existing != nil {
		if err := existing.Ping(ctx); err == nil {
			// Re-check under lock: if the lease was released (or replaced) during
			// the Ping, do NOT return this handle — it may have been handed to
			// another ConnID by the pool. Fall through to acquire a fresh one.
			a.mu.Lock()
			if a.leases[conn] == existing {
				a.mu.Unlock()
				return existing, nil
			}
			a.mu.Unlock()
			// The lease was released/replaced during Ping; we must not use it.
			// Do NOT call existing.Release() here — whoever removed it from
			// leases already released it. Fall through to acquire a new one.
		} else {
			// Dead connection: discard it and acquire a replacement (R9.5).
			a.mu.Lock()
			// Only delete if the lease hasn't been replaced by a concurrent call.
			if a.leases[conn] == existing {
				delete(a.leases, conn)
			}
			a.mu.Unlock()
			existing.Release()
		}
	}

	// Acquire outside the lock so a blocking source (exhausted at max, R9.6)
	// does not stall Release on other ConnIDs that would free a connection.
	h, err := a.src.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	if !a.sessionAffinity {
		// Without affinity each acquisition is independent; the caller owns
		// the returned handle and releases it directly.
		return h, nil
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		h.Release()
		return nil, fmt.Errorf("connection pool is closed")
	}
	// A concurrent acquireFor for the same ConnID may have won the race; if so
	// reuse the established lease and return the extra connection to the pool.
	if existing, ok := a.leases[conn]; ok {
		a.mu.Unlock()
		h.Release()
		return existing, nil
	}
	a.leases[conn] = h
	a.mu.Unlock()
	return h, nil
}

// release frees the connection leased for conn back to the source pool and
// drops the affinity mapping so a later acquireFor starts fresh (R7.5).
func (a *affinityPool) release(conn core.ConnID) {
	a.mu.Lock()
	h, ok := a.leases[conn]
	if ok {
		delete(a.leases, conn)
	}
	a.mu.Unlock()
	if ok {
		h.Release()
	}
}

// stats reports the underlying pool usage (R9.1).
func (a *affinityPool) stats() PoolStats {
	return a.src.Stat()
}

// close releases every outstanding lease and closes the source pool.
func (a *affinityPool) close() {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	leases := a.leases
	a.leases = make(map[core.ConnID]connHandle)
	a.mu.Unlock()

	for _, h := range leases {
		h.Release()
	}
	a.src.Close()
}

// --- Production pgxpool-backed implementation ---------------------------------

// pgxLease adapts an acquired *pgxpool.Conn to the connHandle interface.
type pgxLease struct {
	conn *pgxpool.Conn
}

func (l *pgxLease) Ping(ctx context.Context) error { return l.conn.Ping(ctx) }
func (l *pgxLease) Release()                       { l.conn.Release() }

// pgxConnSource wraps *pgxpool.Pool to satisfy connSource.
type pgxConnSource struct {
	pool *pgxpool.Pool
}

func (s *pgxConnSource) Acquire(ctx context.Context) (connHandle, error) {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return &pgxLease{conn: c}, nil
}

func (s *pgxConnSource) Stat() PoolStats {
	st := s.pool.Stat()
	return PoolStats{
		Total:    st.TotalConns(),
		Acquired: st.AcquiredConns(),
		Idle:     st.IdleConns(),
	}
}

func (s *pgxConnSource) Close() { s.pool.Close() }

// ConnPool is the production affinity-aware connection pool. It implements the
// Pool interface from replayer.go.
type ConnPool struct {
	core *affinityPool
	src  *pgxConnSource
}

// compile-time assertion that ConnPool satisfies the Pool interface.
var _ Pool = (*ConnPool)(nil)

// NewPool builds a pgxpool-backed, affinity-aware connection pool from cfg.
// MaxConns/MinConns come from the replayer config (R9.2, R9.3) and a periodic
// health check enables proactive dead-connection replacement (R9.5). The
// target password is read only from the environment variable named by
// cfg.PasswordEnv (R11.8); it is never taken from a plaintext field.
func NewPool(ctx context.Context, cfg Config) (*ConnPool, error) {
	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid target connection config: %w", err)
	}

	maxConns := cfg.PoolMaxConns
	if maxConns <= 0 {
		maxConns = 100 // R9.2 default
	}
	minConns := cfg.PoolMinConns
	if minConns < 0 {
		minConns = 0
	}
	if minConns > maxConns {
		// Defensive: config validation (task 2.2) enforces min <= max (R9.4),
		// but clamp here so pool construction cannot panic.
		minConns = maxConns
	}
	poolCfg.MaxConns = int32(maxConns)
	poolCfg.MinConns = int32(minConns)
	poolCfg.HealthCheckPeriod = defaultHealthCheckPeriod

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("connect target pool: %w", err)
	}

	src := &pgxConnSource{pool: pool}
	return &ConnPool{
		core: newAffinityPool(src, cfg.SessionAffinity),
		src:  src,
	}, nil
}

// AcquireFor leases a target connection for the given source ConnID. Under
// session affinity the same ConnID always maps to the same connection until it
// is released (R7.5, R7.10). When the pool is exhausted at MaxConns the call
// blocks until a connection frees or ctx is done (R9.6).
func (p *ConnPool) AcquireFor(ctx context.Context, conn core.ConnID) (*pgxpool.Conn, error) {
	h, err := p.core.acquireFor(ctx, conn)
	if err != nil {
		return nil, err
	}
	lease, ok := h.(*pgxLease)
	if !ok {
		return nil, fmt.Errorf("pool: unexpected lease type %T for conn %v", h, conn)
	}
	return lease.conn, nil
}

// Release frees the connection leased for conn (R7.5).
func (p *ConnPool) Release(conn core.ConnID) { p.core.release(conn) }

// Stats reports current pool usage (R9.1).
func (p *ConnPool) Stats() PoolStats { return p.core.stats() }

// Close releases outstanding leases and closes the underlying pool.
func (p *ConnPool) Close() { p.core.close() }

// forbiddenPasswordEnvPrefixes are prefixes that should not be used for
// password environment variable names to prevent accidental exposure of
// common sensitive variables.
var forbiddenPasswordEnvPrefixes = []string{
	"AWS_",      // AWS credentials
	"AZURE_",    // Azure credentials
	"GCP_",      // GCP credentials
	"HEROKU_",   // Heroku credentials
	"DATABASE_", // Generic database passwords
	"DB_",       // Generic database passwords
	"MYSQL_",    // MySQL passwords
	"MONGODB_",  // MongoDB passwords
	"REDIS_",    // Redis passwords
	"SECRET_",   // Generic secrets
	"TOKEN_",    // Generic tokens
}

// validatePasswordEnvName validates that the password environment variable
// name doesn't match common sensitive patterns that could be accidentally
// exposed. Returns an error if the name is forbidden.
func validatePasswordEnvName(name string) error {
	if name == "" {
		return nil // Empty is handled elsewhere
	}
	
	upperName := strings.ToUpper(name)
	for _, prefix := range forbiddenPasswordEnvPrefixes {
		if strings.HasPrefix(upperName, prefix) {
			return fmt.Errorf("password environment variable name %q uses forbidden prefix %q - use a pgshadow-specific name", name, prefix)
		}
	}
	
	return nil
}

// buildDSN assembles a libpq-style connection string from cfg. The password is
// resolved from the environment variable named by cfg.PasswordEnv (R11.8).
// Values that contain spaces or special characters are single-quoted with
// internal quotes escaped per the libpq quoting rules.
//
// Security: The password environment variable name is validated to prevent
// accidental exposure of common cloud/service credentials.
func buildDSN(cfg Config) (string, error) {
	host := cfg.TargetHost
	if host == "" {
		host = "localhost"
	}
	port := cfg.TargetPort
	if port == 0 {
		port = 5432
	}
	dsn := fmt.Sprintf("host=%s port=%d", dsnQuote(host), port)
	if cfg.TargetDatabase != "" {
		dsn += " dbname=" + dsnQuote(cfg.TargetDatabase)
	}
	if cfg.TargetUser != "" {
		dsn += " user=" + dsnQuote(cfg.TargetUser)
	}
	if cfg.PasswordEnv != "" {
		if err := validatePasswordEnvName(cfg.PasswordEnv); err != nil {
			return "", fmt.Errorf("password_env security check failed: %w", err)
		}
		if pw := os.Getenv(cfg.PasswordEnv); pw != "" {
			dsn += " password=" + dsnQuote(pw)
		}
	}
	return dsn, nil
}

// dsnQuote quotes a libpq connection-string value. Values that contain spaces,
// single quotes, or backslashes must be enclosed in single quotes with internal
// single quotes doubled and backslashes doubled. Plain values are returned
// unquoted so the DSN remains human-readable in the common case.
func dsnQuote(s string) string {
	needsQuote := false
	for _, c := range s {
		if c == ' ' || c == '\'' || c == '\\' || c == '\t' || c == '\n' || c == ':' || c == '=' {
			needsQuote = true
			break
		}
	}
	if !needsQuote {
		return s
	}
	var b []byte
	b = append(b, '\'')
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'':
			b = append(b, '\'', '\'')
		case '\\':
			b = append(b, '\\', '\\')
		default:
			b = append(b, s[i])
		}
	}
	b = append(b, '\'')
	return string(b)
}
