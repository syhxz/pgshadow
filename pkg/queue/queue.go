// Package queue implements Module ④ — Buffer Queue. It decouples the producer
// (filter) from the consumer (replayer), bounds memory, and preserves per-ConnID
// order (R6).
//
// This file contains compiling stubs only; method bodies are placeholders.
package queue

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"pgshadow/pkg/core"
	"pgshadow/pkg/metrics"
)

// DefaultCapacity is the queue capacity applied when none is configured. R6.3
// Sized to buffer ~7 minutes of 4,600 TPS traffic (2,000,000 events × ~500B
// average ≈ 1 GB memory for the ring buffer), giving the replayer ample runway
// to catch up during transient target-side slowdowns without ever triggering
// overflow under normal production loads.
const DefaultCapacity = 2000000

// Queue is the common interface for all three backends. R6.2
type Queue interface {
	// Enqueue adds an event. Applies overflow policy at capacity. R6.4
	// Returns dropped=true if an event was discarded due to overflow. R6.5
	Enqueue(ev *core.SQLEvent) (dropped bool)
	// Dequeue blocks until an event is available or the queue is closed.
	Dequeue(ctx context.Context) (*core.SQLEvent, bool)
	Depth() int // R10.3
	Close() error
}

// OverflowPolicy decides which event is discarded at capacity. R6.4
type OverflowPolicy int

const (
	DropOldest OverflowPolicy = iota
	DropNewest
	Block
)

// UnmarshalYAML decodes the overflow policy from its YAML string form
// (drop_oldest|drop_newest|block) so the documented config schema parses
// directly into the typed enum. An empty value defaults to drop_oldest (R6.4).
func (p *OverflowPolicy) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "drop_oldest":
		*p = DropOldest
	case "drop_newest":
		*p = DropNewest
	case "block":
		*p = Block
	default:
		return fmt.Errorf("invalid overflow_policy %q (want drop_oldest|drop_newest|block)", s)
	}
	return nil
}

// String returns the canonical YAML spelling of the overflow policy.
func (p OverflowPolicy) String() string {
	switch p {
	case DropOldest:
		return "drop_oldest"
	case DropNewest:
		return "drop_newest"
	case Block:
		return "block"
	default:
		return fmt.Sprintf("OverflowPolicy(%d)", int(p))
	}
}

// KafkaConfig configures the external Kafka backend (used only when Type=kafka).
// Supports plain (no auth), SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512
// with optional TLS. This enables connectivity to:
//   - Self-hosted Kafka clusters
//   - AWS MSK (IAM auth not yet supported; use SASL/SCRAM with MSK)
//   - Confluent Cloud (SASL/PLAIN + TLS)
//   - Alibaba Cloud / Azure Event Hubs Kafka endpoint
type KafkaConfig struct {
	Brokers    []string `yaml:"brokers"`
	Topic      string   `yaml:"topic"`
	Partitions int      `yaml:"partitions"`

	// SASL authentication (optional). Leave empty for no auth.
	// Mechanism: "plain", "scram-sha-256", "scram-sha-512"
	SASLMechanism string `yaml:"sasl_mechanism"`
	SASLUsername  string `yaml:"sasl_username"`
	SASLPassword  string `yaml:"sasl_password"`
	// SASLPasswordEnv: if set, read password from this env var instead of sasl_password.
	SASLPasswordEnv string `yaml:"sasl_password_env"`

	// TLS configuration (optional). Enable for encrypted connections.
	TLSEnabled bool `yaml:"tls_enabled"`
	// TLSSkipVerify disables server certificate verification (not for production).
	TLSSkipVerify bool `yaml:"tls_skip_verify"`
	// TLSCAFile: path to CA certificate (PEM) for verifying broker certificates.
	TLSCAFile string `yaml:"tls_ca_file"`
	// TLSCertFile + TLSKeyFile: client certificate for mutual TLS (mTLS).
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`
}

// Config configures the buffer queue.
type Config struct {
	Type     string         `yaml:"type"`            // ringbuffer|file|kafka; default ringbuffer R6.2
	Capacity int            `yaml:"capacity"`        // default 2000000 R6.3
	Overflow OverflowPolicy `yaml:"overflow_policy"` // default drop_oldest R6.4
	DataDir  string         `yaml:"data_dir"`        // file queue WAL directory; default OS temp dir
	Kafka    KafkaConfig    `yaml:"kafka"`
}

// New constructs the configured backend. For kafka, verifies broker
// reachability and fails fast if unreachable (R6.10).
func New(cfg Config, m *metrics.Collector) (Queue, error) {
	capacity := cfg.Capacity
	if capacity <= 0 {
		capacity = DefaultCapacity // R6.3 default
	}

	switch cfg.Type {
	case "", "ringbuffer": // default queue type is ringbuffer (R6.2)
		return newRingBuffer(capacity, cfg.Overflow, m), nil
	case "file":
		return newFileQueue(capacity, cfg.Overflow, cfg.DataDir, m)
	case "kafka":
		return newKafkaQueue(cfg.Kafka, m)
	default:
		return nil, fmt.Errorf("queue.New: unknown queue type %q", cfg.Type)
	}
}
