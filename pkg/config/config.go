// Package config implements the configuration module (R11). It loads the
// pgshadow YAML configuration file, applies all documented defaults, and
// resolves the Target_Database password from the environment variable named by
// the configured password_env (R11.8) — the password is never read from a
// plaintext YAML field.
//
// The aggregate Config embeds the per-module Config structs (capture, parser,
// filter, queue, replayer, metrics) so that each module owns its own schema and
// the YAML layout matches the documented config.yaml exactly (R11.1).
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"pgshadow/pkg/capture"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/protocol"
	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
)

// Documented default values (R11). These are applied whenever the corresponding
// field is omitted from the configuration file.
const (
	defaultCaptureMode    = "pcap"                     // R1.2
	defaultCapturePort    = 5432                       // builds the default BPF filter (R1.3)
	defaultBufferSize     = 64 << 20                   // 64 MiB (R1.4)
	defaultMaxSQLLength   = 1 << 20                    // 1 MiB (R3.5)
	defaultTimeoutIdle    = 300                        // seconds (R2.4)
	defaultFilterMode     = "write_only"               // R5.15
	defaultQueueType      = "ringbuffer"               // R6.2
	defaultQueueCapacity  = 500000                     // R6.3
	defaultWorkers        = 32                         // R7.2
	defaultSpeedFactor    = 1.0                        // R7.4
	defaultMaxConcurrency = 256                        // cap for faithful mode (R7.11)
	defaultTargetPort     = 5432                       // standard PostgreSQL port
	defaultTargetDialect  = "postgresql"               // R8.2
	defaultPoolMaxConns   = 100                        // R9.2
	defaultPoolMinConns   = 20                         // R9.3
	defaultMetricsPort    = 9090                       // R10.1
	// defaultPasswordEnv uses a pgshadow-specific prefix to prevent accidental
	// exposure of common cloud/service credentials (R11.8)
	defaultPasswordEnv    = "PGSHADOW_DB_PASSWORD"    // R11.8
	defaultKafkaTopic     = "pgshadow"
	defaultKafkaParts     = 16
)

// Config is the aggregate pgshadow configuration. It embeds each module's own
// Config under the section keys defined by the documented config.yaml schema
// (R11.1). The resolved Target_Database password is held in TargetPassword,
// which is intentionally excluded from YAML (yaml:"-") so the password can only
// ever come from the environment (R11.8).
type Config struct {
	Capture    capture.Config    `yaml:"capture"`
	Parser     protocol.Config   `yaml:"parser"`
	Filter     filter.Config     `yaml:"filter"`
	Queue      queue.Config      `yaml:"queue"`
	Replayer   replayer.Config   `yaml:"replayer"`
	Metrics    metrics.Config    `yaml:"metrics"`
	ReplayGuard ReplayGuardConfig   `yaml:"safeguard"`
	Monitor    MonitorConfig     `yaml:"monitor"`
	Control    ControlConfig     `yaml:"control"`

	// TargetPassword is resolved from the environment variable named by
	// Replayer.PasswordEnv during Load. It is never serialized to or read from
	// the YAML file (R11.8).
	TargetPassword string `yaml:"-"`
}

// ControlConfig configures the external signal-driven control plane.
// When enabled, pgshadow exposes HTTP endpoints for external orchestration
// (Patroni, CNPG, K8s hooks, scripts) to activate/pause capture.
type ControlConfig struct {
	Enabled bool `yaml:"enabled"` // default false — opt-in
	Port    int  `yaml:"port"`    // default 9091
}

// ReplayGuardConfig configures production safeguards.
type ReplayGuardConfig struct {
	ExcludeDDL             bool     `yaml:"exclude_ddl"`
	RewriteSequences       bool     `yaml:"rewrite_sequences"`        // reserved for future use
	RewriteTimeFunctions   bool     `yaml:"rewrite_time_functions"`
	SkipNonDeterministic   bool     `yaml:"skip_non_deterministic"`   // deprecated: no-op
	SkipExternalDeps       bool     `yaml:"skip_external_deps"`
	MaxReplayLagSeconds    float64  `yaml:"max_replay_lag_seconds"`
	MaxQueueDepth          int      `yaml:"max_queue_depth"`
	MaxTransactionDuration string   `yaml:"max_transaction_duration"` // e.g. "30s"
	MaxStatementsPerTx     int      `yaml:"max_statements_per_tx"`
	ReplayPercentage       float64  `yaml:"replay_percentage"`
	IncludeTables          []string `yaml:"include_tables"`
	ExcludeTables          []string `yaml:"exclude_tables"`
	IncludeUsers           []string `yaml:"include_users"`
	ReadOnly               bool     `yaml:"read_only"`
	MaskPatterns           []struct {
		Pattern     string `yaml:"pattern"`
		Replacement string `yaml:"replacement"`
	} `yaml:"mask_patterns"`
}

// MonitorConfig configures operational monitoring.
type MonitorConfig struct {
	FailoverCheckInterval string `yaml:"failover_check_interval"` // e.g. "10s"
	SourceHost            string `yaml:"source_host"`             // DNS name to monitor for failover
	DiffOutput            string `yaml:"diff_output"`             // file path for result diffs
	SlowThreshold         float64 `yaml:"slow_threshold"`         // e.g. 2.0 = flag if 2x slower
}

// defaults returns a Config pre-populated with every documented default value.
//
// The defaults are applied by constructing this baseline and then unmarshaling
// the YAML document over it: gopkg.in/yaml.v3 only assigns fields that are
// present in the document, so any omitted field retains its default. This makes
// default-true booleans (bidirectional, extended_query, session_affinity,
// metrics.enabled) behave correctly — an explicit `false` in the file overrides
// the default, while omission keeps it true.
func defaults() Config {
	return Config{
		Capture: capture.Config{
			Mode:          defaultCaptureMode,
			Port:          defaultCapturePort,
			BufferSize:    defaultBufferSize,
			Bidirectional: true, // R1.5
		},
		Parser: protocol.Config{
			MaxSQLLength:  defaultMaxSQLLength,
			TimeoutIdle:   defaultTimeoutIdle,
			ExtendedQuery: true, // R3.4
		},
		Filter: filter.Config{
			Mode: defaultFilterMode,
		},
		Queue: queue.Config{
			Type:     defaultQueueType,
			Capacity: defaultQueueCapacity,
			Overflow: queue.DropOldest, // R6.4
			Kafka: queue.KafkaConfig{
				Topic:      defaultKafkaTopic,
				Partitions: defaultKafkaParts,
			},
		},
		Replayer: replayer.Config{
			TargetPort:      defaultTargetPort,
			PasswordEnv:     defaultPasswordEnv,
			Workers:         defaultWorkers,
			SpeedFactor:     defaultSpeedFactor,
			SessionAffinity: true,             // R7.5
			ErrorPolicy:     replayer.Skip,    // R7.6
			Mode:            replayer.Bounded, // R7.9
			MaxConcurrency:  defaultMaxConcurrency,
			TargetDialect:   defaultTargetDialect, // R8.2
			PoolMaxConns:    defaultPoolMaxConns,
			PoolMinConns:    defaultPoolMinConns,
		},
		Metrics: metrics.Config{
			Enabled: true, // metrics on by default
			Port:    defaultMetricsPort,
		},
	}
}

// Load reads the configuration file at path, applies all documented defaults
// for omitted fields (R11), resolves the Target_Database password from the
// environment variable named by password_env (R11.8), and validates the
// result. It returns an error if the file cannot be read or parsed, if the
// password environment variable is unset, or if validation fails.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %q: %w", path, err)
	}

	cfg := defaults()

	// KnownFields=true would reject unknown keys; we keep it lenient so that
	// operator comments / future keys do not break older binaries. Defaults are
	// preserved for any field the document omits.
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parsing %q: %w", path, err)
	}

	cfg.applyDerivedDefaults()

	if err := cfg.resolvePassword(); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// applyDerivedDefaults fills in defaults that depend on other already-resolved
// fields (so they cannot be expressed in the static defaults() baseline).
func (c *Config) applyDerivedDefaults() {
	// The default BPF filter selects the configured capture port (R1.3).
	if c.Capture.BPFFilter == "" {
		port := c.Capture.Port
		if port == 0 {
			port = defaultCapturePort
			c.Capture.Port = port
		}
		c.Capture.BPFFilter = fmt.Sprintf("tcp port %d", port)
	}
	// An empty password_env falls back to the documented default name (R11.8).
	if c.Replayer.PasswordEnv == "" {
		c.Replayer.PasswordEnv = defaultPasswordEnv
	}
}

// resolvePassword reads the Target_Database password from the environment
// variable named by Replayer.PasswordEnv and stores it in TargetPassword. The
// password is never sourced from a YAML field (R11.8). It is an error for the
// named environment variable to be unset.
func (c *Config) resolvePassword() error {
	name := c.Replayer.PasswordEnv
	value, ok := os.LookupEnv(name)
	if !ok {
		return fmt.Errorf("config: target password environment variable %q is not set", name)
	}
	c.TargetPassword = value
	return nil
}

// validFilterModes is the set of filter modes accepted by the SQL_Filter (R5.15).
var validFilterModes = map[string]bool{
	"write_only": true,
	"all":        true,
	"ddl_only":   true,
	"dml_only":   true,
	"custom":     true,
}

// Validate enforces the configuration contract in two phases (R11.9, R11.10).
//
// Phase 1 (structural) checks that every required parameter is present and
// fails with an error naming the missing parameter. Phase 2 (semantic) checks
// value ranges and cross-field constraints and fails with an error naming the
// offending parameter and its permitted range. The first violation encountered
// is returned.
func (c *Config) Validate() error {
	if err := c.validateStructural(); err != nil {
		return err
	}
	return c.validateSemantic()
}

// validateStructural implements Phase 1: required parameters must be present
// (R11.9). The capture interface and the target host/database/user have no
// usable default and must be supplied by the operator.
func (c *Config) validateStructural() error {
	type required struct {
		name  string
		value string
	}
	for _, r := range []required{
		{"capture.interface", c.Capture.Interface},      // R1.1
		{"replayer.target_host", c.Replayer.TargetHost}, // R9.1
		{"replayer.target_database", c.Replayer.TargetDatabase},
		{"replayer.target_user", c.Replayer.TargetUser},
	} {
		if strings.TrimSpace(r.value) == "" {
			return fmt.Errorf("config: required parameter %s is missing", r.name)
		}
	}
	return nil
}

// validateSemantic implements Phase 2: value ranges and cross-field constraints
// (R11.10). Each error names the offending parameter and its permitted range.
func (c *Config) validateSemantic() error {
	// Target dialect must be exactly postgresql or greenplum (R8.6).
	switch c.Replayer.TargetDialect {
	case "postgresql", "greenplum":
	default:
		return fmt.Errorf("config: replayer.target_dialect %q is invalid (want postgresql|greenplum)", c.Replayer.TargetDialect)
	}

	// Connection-pool bounds must be positive and min must not exceed max (R9.4).
	if c.Replayer.PoolMaxConns < 1 {
		return fmt.Errorf("config: replayer.pool_max_conns (%d) must be >= 1", c.Replayer.PoolMaxConns)
	}
	if c.Replayer.PoolMinConns < 0 {
		return fmt.Errorf("config: replayer.pool_min_conns (%d) must be >= 0", c.Replayer.PoolMinConns)
	}
	if c.Replayer.PoolMinConns > c.Replayer.PoolMaxConns {
		return fmt.Errorf("config: replayer.pool_min_conns (%d) must not exceed replayer.pool_max_conns (%d)", c.Replayer.PoolMinConns, c.Replayer.PoolMaxConns)
	}

	// Worker count must be positive (R7.2, R11.10).
	if c.Replayer.Workers < 1 {
		return fmt.Errorf("config: replayer.workers (%d) must be positive", c.Replayer.Workers)
	}

	// Queue capacity must be positive (R6.3, R11.10).
	if c.Queue.Capacity < 1 {
		return fmt.Errorf("config: queue.capacity (%d) must be positive", c.Queue.Capacity)
	}

	// Filter mode must be one of the documented presets (R5.15).
	if !validFilterModes[c.Filter.Mode] {
		return fmt.Errorf("config: filter.mode %q is invalid (want write_only|all|ddl_only|dml_only|custom)", c.Filter.Mode)
	}

	// Overflow policy must be a recognized enum value (R6.4).
	switch c.Queue.Overflow {
	case queue.DropOldest, queue.DropNewest, queue.Block:
	default:
		return fmt.Errorf("config: queue.overflow_policy %q is invalid (want drop_oldest|drop_newest|block)", c.Queue.Overflow)
	}

	// Error policy must be a recognized enum value (R7.6).
	switch c.Replayer.ErrorPolicy {
	case replayer.Skip, replayer.Retry, replayer.Abort:
	default:
		return fmt.Errorf("config: replayer.error_policy %q is invalid (want skip|retry|abort)", c.Replayer.ErrorPolicy)
	}

	// Replay concurrency mode must be a recognized enum value (R7.9).
	switch c.Replayer.Mode {
	case replayer.Bounded, replayer.Faithful, replayer.Serial:
	default:
		return fmt.Errorf("config: replayer.replay_concurrency_mode %q is invalid (want bounded|faithful|serial)", c.Replayer.Mode)
	}

	// Parser limits: MaxSQLLength must be positive (R3.5).
	if c.Parser.MaxSQLLength < 1 {
		return fmt.Errorf("config: parser.max_sql_length (%d) must be >= 1", c.Parser.MaxSQLLength)
	}

	// Idle timeout must be non-negative (R2.4).
	if c.Parser.TimeoutIdle < 0 {
		return fmt.Errorf("config: parser.timeout_idle (%d) must be >= 0", c.Parser.TimeoutIdle)
	}

	// Port numbers must be in the valid TCP range 1-65535.
	for name, port := range map[string]int{
		"capture.port":        c.Capture.Port,
		"replayer.target_port": c.Replayer.TargetPort,
		"metrics.port":        c.Metrics.Port,
	} {
		if port < 1 || port > 65535 {
			return fmt.Errorf("config: %s (%d) must be in range 1-65535", name, port)
		}
	}

	// MaxConcurrency must be positive when specified for faithful mode (R7.11).
	if c.Replayer.MaxConcurrency < 1 {
		return fmt.Errorf("config: replayer.max_concurrency (%d) must be >= 1", c.Replayer.MaxConcurrency)
	}

	return nil
}
