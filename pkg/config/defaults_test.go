package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
)

// writeTempConfig writes body to a config.yaml inside a fresh temp directory and
// returns its path. It fails the test if the file cannot be written.
func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}
	return path
}

// minimalYAML supplies only the parameters that have no usable default and must
// be provided by the operator (R11.9): the capture interface and the target
// host/database/user. Every other field is omitted so the loader must apply its
// documented default.
const minimalYAML = `
capture:
  interface: eth0
replayer:
  target_host: target.example.com
  target_database: appdb
  target_user: replayer
`

// TestLoad_AppliesDocumentedDefaults loads a minimal configuration and asserts
// that every documented default is applied for the omitted fields (R11 and all
// default-bearing acceptance criteria).
func TestLoad_AppliesDocumentedDefaults(t *testing.T) {
	// The minimal config omits password_env, so the default env var name is
	// used; it must be set for Load to resolve the password successfully.
	t.Setenv(defaultPasswordEnv, "secret")

	path := writeTempConfig(t, minimalYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	// --- Capture defaults ---
	if got := cfg.Capture.Mode; got != "pcap" { // R1.2
		t.Errorf("capture.mode = %q, want %q", got, "pcap")
	}
	if got := cfg.Capture.BufferSize; got != 64<<20 { // 64 MiB R1.4
		t.Errorf("capture.buffer_size = %d, want %d", got, 64<<20)
	}
	if !cfg.Capture.Bidirectional { // R1.5
		t.Errorf("capture.bidirectional = false, want true")
	}
	if got := cfg.Capture.BPFFilter; got != "tcp port 5432" { // R1.3
		t.Errorf("capture.bpf_filter = %q, want %q", got, "tcp port 5432")
	}

	// --- Parser defaults ---
	if got := cfg.Parser.MaxSQLLength; got != 1<<20 { // 1 MiB R3.5
		t.Errorf("parser.max_sql_length = %d, want %d", got, 1<<20)
	}
	if got := cfg.Parser.TimeoutIdle; got != 300 { // seconds R2.4
		t.Errorf("parser.timeout_idle_conn = %d, want 300", got)
	}
	if !cfg.Parser.ExtendedQuery { // R3.4
		t.Errorf("parser.extended_query = false, want true")
	}

	// --- Filter defaults ---
	if got := cfg.Filter.Mode; got != "write_only" { // R5.15
		t.Errorf("filter.mode = %q, want %q", got, "write_only")
	}

	// --- Queue defaults ---
	if got := cfg.Queue.Type; got != "ringbuffer" { // R6.2
		t.Errorf("queue.type = %q, want %q", got, "ringbuffer")
	}
	if got := cfg.Queue.Capacity; got != 500000 { // R6.3
		t.Errorf("queue.capacity = %d, want 500000", got)
	}
	if cfg.Queue.Overflow != queue.DropOldest { // R6.4
		t.Errorf("queue.overflow_policy = %v, want drop_oldest", cfg.Queue.Overflow)
	}

	// --- Replayer defaults ---
	if got := cfg.Replayer.Workers; got != 32 { // R7.2
		t.Errorf("replayer.workers = %d, want 32", got)
	}
	if got := cfg.Replayer.SpeedFactor; got != 1.0 { // R7.4
		t.Errorf("replayer.speed_factor = %v, want 1.0", got)
	}
	if !cfg.Replayer.SessionAffinity { // R7.5
		t.Errorf("replayer.session_affinity = false, want true")
	}
	if cfg.Replayer.ErrorPolicy != replayer.Skip { // R7.6
		t.Errorf("replayer.error_policy = %v, want skip", cfg.Replayer.ErrorPolicy)
	}
	if cfg.Replayer.Mode != replayer.Bounded { // R7.9
		t.Errorf("replayer.replay_concurrency_mode = %v, want bounded", cfg.Replayer.Mode)
	}
	if got := cfg.Replayer.TargetDialect; got != "postgresql" { // R8.2
		t.Errorf("replayer.target_dialect = %q, want %q", got, "postgresql")
	}
	if got := cfg.Replayer.PoolMaxConns; got != 100 { // R9.2
		t.Errorf("replayer.pool_max_conns = %d, want 100", got)
	}
	if got := cfg.Replayer.PoolMinConns; got != 20 { // R9.3
		t.Errorf("replayer.pool_min_conns = %d, want 20", got)
	}

	// --- Metrics defaults ---
	if got := cfg.Metrics.Port; got != 9090 { // R10.1
		t.Errorf("metrics.port = %d, want 9090", got)
	}
}

// TestLoad_PasswordFromEnvNamedByPasswordEnv asserts the Target_Database
// password is read from the environment variable whose name is given by
// password_env, and that no plaintext password YAML field exists: a plaintext
// password key in the file is ignored and the env value wins (R11.8).
func TestLoad_PasswordFromEnvNamedByPasswordEnv(t *testing.T) {
	const customEnv = "PGSHADOW_TEST_CUSTOM_PW_ENV"
	t.Setenv(customEnv, "from-environment")

	// The replayer section names a custom env var via password_env and also
	// includes plaintext password fields that MUST be ignored (no such field
	// exists in the schema).
	body := `
capture:
  interface: eth0
replayer:
  target_host: target.example.com
  target_database: appdb
  target_user: replayer
  password_env: PGSHADOW_TEST_CUSTOM_PW_ENV
  password: plaintext-should-be-ignored
  target_password: plaintext-should-be-ignored
`
	path := writeTempConfig(t, body)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}

	if cfg.Replayer.PasswordEnv != customEnv {
		t.Errorf("password_env = %q, want %q", cfg.Replayer.PasswordEnv, customEnv)
	}
	if cfg.TargetPassword != "from-environment" {
		t.Errorf("TargetPassword = %q, want %q (resolved from env, not YAML)", cfg.TargetPassword, "from-environment")
	}
	if strings.Contains(cfg.TargetPassword, "plaintext") {
		t.Errorf("TargetPassword = %q, must not come from a plaintext YAML field", cfg.TargetPassword)
	}
}

// TestLoad_MissingEnvVarErrors asserts that Load fails when the environment
// variable named by password_env is unset, and that the error names the
// missing variable (R11.8).
func TestLoad_MissingEnvVarErrors(t *testing.T) {
	const missingEnv = "PGSHADOW_TEST_DEFINITELY_UNSET_PW_ENV"
	// Ensure the variable is not set in the ambient environment.
	if _, ok := os.LookupEnv(missingEnv); ok {
		t.Skipf("environment variable %q unexpectedly set; skipping", missingEnv)
	}

	body := `
capture:
  interface: eth0
replayer:
  target_host: target.example.com
  target_database: appdb
  target_user: replayer
  password_env: PGSHADOW_TEST_DEFINITELY_UNSET_PW_ENV
`
	path := writeTempConfig(t, body)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected Load to fail when password env var is unset, got nil")
	}
	if !strings.Contains(err.Error(), missingEnv) {
		t.Errorf("error %q does not name the missing env var %q", err.Error(), missingEnv)
	}
}
