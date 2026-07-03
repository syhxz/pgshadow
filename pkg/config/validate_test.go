package config

import (
	"strings"
	"testing"

	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
)

// validConfig returns a fully-populated, valid aggregate Config: the documented
// defaults plus the required parameters that have no default (capture interface
// and target host/database/user).
func validConfig() Config {
	c := defaults()
	c.Capture.Interface = "eth0"
	c.Replayer.TargetHost = "target.example.com"
	c.Replayer.TargetDatabase = "appdb"
	c.Replayer.TargetUser = "replayer"
	return c
}

func TestValidate_FullyValidConfigPasses(t *testing.T) {
	c := validConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("expected valid config to pass, got error: %v", err)
	}
}

// TestValidate_MissingRequiredParamNamesParam asserts Phase 1 structural
// validation fails for each missing required parameter and names it (R11.9).
func TestValidate_MissingRequiredParamNamesParam(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(c *Config)
		param   string
	}{
		{
			name:    "missing capture.interface",
			corrupt: func(c *Config) { c.Capture.Interface = "" },
			param:   "capture.interface",
		},
		{
			name:    "missing replayer.target_host",
			corrupt: func(c *Config) { c.Replayer.TargetHost = "" },
			param:   "replayer.target_host",
		},
		{
			name:    "missing replayer.target_database",
			corrupt: func(c *Config) { c.Replayer.TargetDatabase = "" },
			param:   "replayer.target_database",
		},
		{
			name:    "missing replayer.target_user",
			corrupt: func(c *Config) { c.Replayer.TargetUser = "" },
			param:   "replayer.target_user",
		},
		{
			name:    "blank (whitespace) target_host is treated as missing",
			corrupt: func(c *Config) { c.Replayer.TargetHost = "   " },
			param:   "replayer.target_host",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.corrupt(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.param)
			}
			if !strings.Contains(err.Error(), tc.param) {
				t.Fatalf("error %q does not name the missing parameter %q", err.Error(), tc.param)
			}
		})
	}
}

// TestValidate_SemanticViolationNamesParamAndRange asserts Phase 2 semantic
// validation fails for each range / cross-field violation and that the error
// names the offending parameter and its permitted range (R11.10, R8.6, R9.4).
func TestValidate_SemanticViolationNamesParamAndRange(t *testing.T) {
	cases := []struct {
		name     string
		corrupt  func(c *Config)
		param    string   // parameter name that must appear in the error
		mentions []string // additional substrings naming the permitted range
	}{
		{
			name:     "invalid target_dialect",
			corrupt:  func(c *Config) { c.Replayer.TargetDialect = "mysql" },
			param:    "target_dialect",
			mentions: []string{"postgresql", "greenplum"},
		},
		{
			name:     "pool_min_conns exceeds pool_max_conns",
			corrupt:  func(c *Config) { c.Replayer.PoolMinConns = 200; c.Replayer.PoolMaxConns = 100 },
			param:    "pool_min_conns",
			mentions: []string{"pool_max_conns"},
		},
		{
			name:     "non-positive pool_max_conns",
			corrupt:  func(c *Config) { c.Replayer.PoolMaxConns = 0 },
			param:    "pool_max_conns",
			mentions: []string{">= 1"},
		},
		{
			name:     "negative pool_min_conns",
			corrupt:  func(c *Config) { c.Replayer.PoolMinConns = -1 },
			param:    "pool_min_conns",
			mentions: []string{">= 0"},
		},
		{
			name:     "non-positive workers",
			corrupt:  func(c *Config) { c.Replayer.Workers = 0 },
			param:    "workers",
			mentions: []string{"positive"},
		},
		{
			name:     "non-positive queue capacity",
			corrupt:  func(c *Config) { c.Queue.Capacity = 0 },
			param:    "capacity",
			mentions: []string{"positive"},
		},
		{
			name:     "invalid filter mode",
			corrupt:  func(c *Config) { c.Filter.Mode = "bogus" },
			param:    "filter.mode",
			mentions: []string{"write_only", "custom"},
		},
		{
			name:     "invalid overflow policy",
			corrupt:  func(c *Config) { c.Queue.Overflow = queue.OverflowPolicy(99) },
			param:    "overflow_policy",
			mentions: []string{"drop_oldest"},
		},
		{
			name:     "invalid error policy",
			corrupt:  func(c *Config) { c.Replayer.ErrorPolicy = replayer.ErrorPolicy(99) },
			param:    "error_policy",
			mentions: []string{"skip"},
		},
		{
			name:     "invalid concurrency mode",
			corrupt:  func(c *Config) { c.Replayer.Mode = replayer.ConcurrencyMode(99) },
			param:    "replay_concurrency_mode",
			mentions: []string{"bounded"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.corrupt(&c)
			err := c.Validate()
			if err == nil {
				t.Fatalf("expected validation error for %s, got nil", tc.param)
			}
			if !strings.Contains(err.Error(), tc.param) {
				t.Fatalf("error %q does not name the offending parameter %q", err.Error(), tc.param)
			}
			for _, m := range tc.mentions {
				if !strings.Contains(err.Error(), m) {
					t.Fatalf("error %q does not mention permitted range substring %q", err.Error(), m)
				}
			}
		})
	}
}

// TestValidate_StructuralBeforeSemantic asserts that a config that is both
// missing a required parameter and has a semantic violation reports the
// structural (Phase 1) error first.
func TestValidate_StructuralBeforeSemantic(t *testing.T) {
	c := validConfig()
	c.Capture.Interface = ""       // structural violation
	c.Replayer.TargetDialect = "x" // semantic violation
	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "capture.interface") {
		t.Fatalf("expected structural error first, got %q", err.Error())
	}
}

// TestValidate_GreenplumDialectAccepted confirms the greenplum dialect passes (R8.6).
func TestValidate_GreenplumDialectAccepted(t *testing.T) {
	c := validConfig()
	c.Replayer.TargetDialect = "greenplum"
	if err := c.Validate(); err != nil {
		t.Fatalf("expected greenplum dialect to be accepted, got %v", err)
	}
}

// TestValidate_PoolMinEqualsMaxAccepted confirms the boundary min == max is
// allowed (R9.4 requires min <= max).
func TestValidate_PoolMinEqualsMaxAccepted(t *testing.T) {
	c := validConfig()
	c.Replayer.PoolMinConns = 50
	c.Replayer.PoolMaxConns = 50
	if err := c.Validate(); err != nil {
		t.Fatalf("expected min == max to be accepted, got %v", err)
	}
}
