// Package router implements multi-database routing for replay (#3).
// When the source has multiple databases, this routes captured SQL to the
// correct target database based on the source connection's database context.
package router

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolRouter manages multiple connection pools, one per target database.
// When source traffic comes from different databases, each is routed to
// the corresponding target pool.
type PoolRouter struct {
	mu           sync.RWMutex
	pools        map[string]*pgxpool.Pool // database name → pool
	template     PoolConfig              // base config (host, user, password, etc.)
	closed       bool
	maxDatabases int                      // max pools to create; 0 = default (64)
}

// defaultMaxDatabases is the maximum number of database-specific pools the
// router will create. This prevents unbounded connection creation when routing
// traffic from a large multi-tenant source.
const defaultMaxDatabases = 64

// PoolConfig is the base connection configuration template.
// The Database field is replaced per-pool. The password is resolved at runtime
// from the environment variable named by PasswordEnv (never stored in plaintext).
type PoolConfig struct {
	Host        string
	Port        int
	User        string
	PasswordEnv string // environment variable name holding the password
	MaxConns    int32
	MinConns    int32
	SSLMode     string
}

// NewPoolRouter creates a router with the given base config.
func NewPoolRouter(cfg PoolConfig) *PoolRouter {
	return &PoolRouter{
		pools:        make(map[string]*pgxpool.Pool),
		template:     cfg,
		maxDatabases: defaultMaxDatabases,
	}
}

// GetPool returns (or lazily creates) a connection pool for the given database.
func (r *PoolRouter) GetPool(ctx context.Context, database string) (*pgxpool.Pool, error) {
	// Fast path: read lock
	r.mu.RLock()
	if pool, ok := r.pools[database]; ok {
		r.mu.RUnlock()
		return pool, nil
	}
	r.mu.RUnlock()

	// Slow path: create pool
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, fmt.Errorf("pool router is closed")
	}

	// Double-check after acquiring write lock
	if pool, ok := r.pools[database]; ok {
		return pool, nil
	}

	// Enforce pool count limit to prevent unbounded connection creation.
	maxDB := r.maxDatabases
	if maxDB <= 0 {
		maxDB = defaultMaxDatabases
	}
	if len(r.pools) >= maxDB {
		return nil, fmt.Errorf("pool router: max databases reached (%d), cannot create pool for %q", maxDB, database)
	}

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s pool_max_conns=%d pool_min_conns=%d",
		dsnQuoteRouter(r.template.Host), r.template.Port, dsnQuoteRouter(r.template.User),
		dsnQuoteRouter(database),
		r.template.MaxConns, r.template.MinConns,
	)
	// Resolve password from environment variable at runtime — never store in struct.
	if r.template.PasswordEnv != "" {
		if pw := os.Getenv(r.template.PasswordEnv); pw != "" {
			connStr += " password=" + dsnQuoteRouter(pw)
		}
	}
	if r.template.SSLMode != "" {
		connStr += " sslmode=" + dsnQuoteRouter(r.template.SSLMode)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("create pool for database %q: %w", database, err)
	}

	r.pools[database] = pool
	return pool, nil
}

// Close closes all pools.
func (r *PoolRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, pool := range r.pools {
		pool.Close()
	}
}

// Databases returns the list of databases with active pools.
func (r *PoolRouter) Databases() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dbs := make([]string, 0, len(r.pools))
	for db := range r.pools {
		dbs = append(dbs, db)
	}
	return dbs
}

// Stats returns pool stats per database.
func (r *PoolRouter) Stats() map[string]PoolStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := make(map[string]PoolStats, len(r.pools))
	for db, pool := range r.pools {
		s := pool.Stat()
		stats[db] = PoolStats{
			Total:    s.TotalConns(),
			Acquired: s.AcquiredConns(),
			Idle:     s.IdleConns(),
		}
	}
	return stats
}

// PoolStats holds per-database pool statistics.
type PoolStats struct {
	Total    int32
	Acquired int32
	Idle     int32
}

// dsnQuoteRouter quotes a DSN value if it contains spaces, quotes, or backslashes
// per libpq connection string quoting rules.
func dsnQuoteRouter(s string) string {
	needsQuote := false
	for _, c := range s {
		if c == ' ' || c == '\'' || c == '\\' || c == '=' {
			needsQuote = true
			break
		}
	}
	if !needsQuote && s != "" {
		return s
	}
	// Single-quote with internal single-quotes escaped as ''
	var buf []byte
	buf = append(buf, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			buf = append(buf, '\'', '\'')
		} else if s[i] == '\\' {
			buf = append(buf, '\\', '\\')
		} else {
			buf = append(buf, s[i])
		}
	}
	buf = append(buf, '\'')
	return string(buf)
}
