// Package replayer implements Module ⑤ — Replayer. It consumes SQL_Events and
// executes them against the Target_Database via a connection pool, honoring
// concurrency mode, session affinity, pacing, rate limit, and error policy
// (R7), with dialect-specific behavior (R8) and connection pooling (R9).
//
// This file contains compiling stubs only; method bodies are placeholders.
package replayer

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"

	"pgshadow/pkg/core"
)

// ConcurrencyMode selects how lanes are scheduled onto workers/connections. R7.9
type ConcurrencyMode int

const (
	Bounded ConcurrencyMode = iota
	Faithful
	Serial
)

// UnmarshalYAML decodes the concurrency mode from its YAML string form
// (bounded|faithful|serial) so the documented config schema parses directly
// into the typed enum. An empty value defaults to bounded (R7.9).
func (m *ConcurrencyMode) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "bounded":
		*m = Bounded
	case "faithful":
		*m = Faithful
	case "serial":
		*m = Serial
	default:
		return fmt.Errorf("invalid replay_concurrency_mode %q (want bounded|faithful|serial)", s)
	}
	return nil
}

// String returns the canonical YAML spelling of the concurrency mode.
func (m ConcurrencyMode) String() string {
	switch m {
	case Bounded:
		return "bounded"
	case Faithful:
		return "faithful"
	case Serial:
		return "serial"
	default:
		return fmt.Sprintf("ConcurrencyMode(%d)", int(m))
	}
}

// ErrorPolicy selects behavior on target execution failure. R7.6
type ErrorPolicy int

const (
	Skip ErrorPolicy = iota
	Retry
	Abort
)

// UnmarshalYAML decodes the error policy from its YAML string form
// (skip|retry|abort) so the documented config schema parses directly into the
// typed enum. An empty value defaults to skip (R7.6).
func (e *ErrorPolicy) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "skip":
		*e = Skip
	case "retry":
		*e = Retry
	case "abort":
		*e = Abort
	default:
		return fmt.Errorf("invalid error_policy %q (want skip|retry|abort)", s)
	}
	return nil
}

// String returns the canonical YAML spelling of the error policy.
func (e ErrorPolicy) String() string {
	switch e {
	case Skip:
		return "skip"
	case Retry:
		return "retry"
	case Abort:
		return "abort"
	default:
		return fmt.Sprintf("ErrorPolicy(%d)", int(e))
	}
}

// Config configures the replayer.
type Config struct {
	TargetHost      string          `yaml:"target_host"`
	TargetPort      int             `yaml:"target_port"`
	TargetDatabase  string          `yaml:"target_database"`
	TargetUser      string          `yaml:"target_user"`
	PasswordEnv     string          `yaml:"password_env"`            // env var name (R11.8)
	Workers         int             `yaml:"workers"`                 // default 32 R7.2
	RateLimitQPS    float64         `yaml:"rate_limit"`              // 0 = unlimited R7.3
	SpeedFactor     float64         `yaml:"speed_factor"`            // 1.0=original, 0=ASAP R7.4
	SessionAffinity bool            `yaml:"session_affinity"`        // default true R7.5
	ErrorPolicy     ErrorPolicy     `yaml:"error_policy"`            // default skip R7.6
	Mode            ConcurrencyMode `yaml:"replay_concurrency_mode"` // default bounded R7.9
	MaxConcurrency  int             `yaml:"max_concurrency"`         // cap for faithful mode R7.11
	TargetDialect   string          `yaml:"target_dialect"`          // postgresql|greenplum; default postgresql R8.2
	PoolMaxConns    int             `yaml:"pool_max_conns"`          // default 100 R9.2
	PoolMinConns    int             `yaml:"pool_min_conns"`          // default 20 R9.3
}

// PoolStats reports connection pool usage.
type PoolStats struct {
	Total    int32
	Acquired int32
	Idle     int32
}

// Pool wraps pgxpool to provide affinity-aware connection leasing. R9
type Pool interface {
	// AcquireFor leases a connection. With affinity, the same ConnID maps to
	// the same target connection for its lifetime (R7.5, R7.10).
	AcquireFor(ctx context.Context, conn core.ConnID) (*pgxpool.Conn, error)
	Release(conn core.ConnID)
	Stats() PoolStats
	Close()
}

// Replayer orchestrates workers and routing.
type Replayer interface {
	Run(ctx context.Context) error // returns on context cancel (graceful stop)
}
