package config

import (
	"strings"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"

	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
)

// Feature: pgshadow, Property 19: Configuration validation accepts well-formed configs and rejects ill-formed ones by name

// corruption is a single-field mutation applied to an otherwise well-formed
// Config. param is the substring the resulting Validate() error must contain so
// the offending parameter is named (R11.9, R11.10). A corruption with an empty
// param represents the "no corruption" (well-formed) case.
type corruption struct {
	param string
	apply func(c *Config)
}

// corruptions enumerates every single-field structural/range violation the
// validator must reject, each naming the offending parameter (R8.6, R9.4,
// R11.9, R11.10).
var corruptions = []corruption{
	{"capture.interface", func(c *Config) { c.Capture.Interface = "" }},
	{"replayer.target_host", func(c *Config) { c.Replayer.TargetHost = "" }},
	{"replayer.target_database", func(c *Config) { c.Replayer.TargetDatabase = "" }},
	{"replayer.target_user", func(c *Config) { c.Replayer.TargetUser = "" }},
	{"target_dialect", func(c *Config) { c.Replayer.TargetDialect = "mysql" }},
	{"pool_max_conns", func(c *Config) { c.Replayer.PoolMaxConns = 0 }},
	{"pool_min_conns", func(c *Config) { c.Replayer.PoolMinConns = -1 }},
	{"pool_min_conns", func(c *Config) {
		c.Replayer.PoolMinConns = c.Replayer.PoolMaxConns + 1
	}},
	{"workers", func(c *Config) { c.Replayer.Workers = 0 }},
	{"capacity", func(c *Config) { c.Queue.Capacity = 0 }},
	{"filter.mode", func(c *Config) { c.Filter.Mode = "bogus" }},
	{"overflow_policy", func(c *Config) { c.Queue.Overflow = queue.OverflowPolicy(99) }},
	{"error_policy", func(c *Config) { c.Replayer.ErrorPolicy = replayer.ErrorPolicy(99) }},
	{"replay_concurrency_mode", func(c *Config) { c.Replayer.Mode = replayer.ConcurrencyMode(99) }},
}

// validDialects, validFilterModeList, validOverflows, validErrorPolicies and
// validModes are the in-range value sets the generator draws from when building
// the well-formed baseline so the accept case exercises the whole input space.
var (
	validDialects       = []string{"postgresql", "greenplum"}
	validFilterModeList = []string{"write_only", "all", "ddl_only", "dml_only", "custom"}
	validOverflows      = []queue.OverflowPolicy{queue.DropOldest, queue.DropNewest, queue.Block}
	validErrorPolicies  = []replayer.ErrorPolicy{replayer.Skip, replayer.Retry, replayer.Abort}
	validModes          = []replayer.ConcurrencyMode{replayer.Bounded, replayer.Faithful, replayer.Serial}
)

// configCase pairs a generated Config with the parameter the validator is
// expected to flag. offending == "" means the Config is well-formed and
// Validate() must succeed.
type configCase struct {
	cfg       Config
	offending string
}

// genConfigCase produces well-formed configs (with randomized in-range values)
// plus single-field corruptions/range violations (Property 19's genConfig).
func genConfigCase() gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(0, len(corruptions)), // == len(corruptions) means "no corruption"
		gen.IntRange(0, len(validDialects)-1),
		gen.IntRange(1, 500),  // poolMax
		gen.IntRange(0, 500),  // poolMin (clamped to <= poolMax)
		gen.IntRange(1, 1024), // workers
		gen.IntRange(1, 1000000),
		gen.IntRange(0, len(validFilterModeList)-1),
		gen.IntRange(0, len(validOverflows)-1),
		gen.IntRange(0, len(validErrorPolicies)-1),
		gen.IntRange(0, len(validModes)-1),
	).Map(func(vals []interface{}) configCase {
		sel := vals[0].(int)
		poolMax := vals[2].(int)
		poolMin := vals[3].(int)
		if poolMin > poolMax {
			poolMin = poolMax
		}

		c := validConfig()
		c.Replayer.TargetDialect = validDialects[vals[1].(int)]
		c.Replayer.PoolMaxConns = poolMax
		c.Replayer.PoolMinConns = poolMin
		c.Replayer.Workers = vals[4].(int)
		c.Queue.Capacity = vals[5].(int)
		c.Filter.Mode = validFilterModeList[vals[6].(int)]
		c.Queue.Overflow = validOverflows[vals[7].(int)]
		c.Replayer.ErrorPolicy = validErrorPolicies[vals[8].(int)]
		c.Replayer.Mode = validModes[vals[9].(int)]

		cc := configCase{cfg: c}
		if sel < len(corruptions) {
			corruptions[sel].apply(&cc.cfg)
			cc.offending = corruptions[sel].param
		}
		return cc
	})
}

// TestProperty19_ValidationAcceptsWellFormedRejectsIllFormedByName verifies that
// Validate() succeeds iff the config is well-formed, and otherwise returns an
// error naming the offending parameter (R8.6, R9.4, R11.9, R11.10).
func TestProperty19_ValidationAcceptsWellFormedRejectsIllFormedByName(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 200
	properties := gopter.NewProperties(params)

	properties.Property("Validate accepts well-formed configs and rejects ill-formed ones by name",
		prop.ForAll(func(cc configCase) bool {
			err := cc.cfg.Validate()
			if cc.offending == "" {
				return err == nil
			}
			return err != nil && strings.Contains(err.Error(), cc.offending)
		}, genConfigCase()))

	properties.TestingRun(t)
}
