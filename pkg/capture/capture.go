// Package capture implements Module ① — Packet Capture. It opens the configured
// network interface in the configured mode, applies the BPF filter, hands
// packets to the TCP reassembler, and reports packet drops. It never writes to
// the network (R1.6, R12.1).
//
// # Build tags
//
// The libpcap-backed implementations are isolated behind build tags so that the
// default `go build ./...` has no dependency on cgo or the libpcap development
// headers:
//
//   - pcap mode    — file source_pcap.go, guarded by `//go:build pcap`. Build
//     with `-tags pcap` (requires cgo + libpcap). Without the tag a stub
//     (source_pcap_stub.go) returns a clear runtime error.
//   - af_packet    — file source_afpacket.go, guarded by `//go:build linux &&
//     pcap` (AF_PACKET is Linux-only and BPF compilation reuses libpcap). On
//     any other platform/tag combination a stub (source_afpacket_stub.go)
//     returns a clear runtime error.
//   - ebpf mode    — file source_ebpf.go, guarded by `//go:build linux &&
//     ebpf` (eBPF/XDP is Linux-only and requires a recent kernel + privileges).
//     Build with `GOOS=linux go build -tags ebpf ./...` and run `go mod tidy`
//     in that environment to record the eBPF loader dependency. On any other
//     platform/tag combination a stub (source_ebpf_stub.go) returns a clear
//     runtime error. The real loader is a documented skeleton (task 8.2 is a
//     stretch goal); see source_ebpf.go for the intended XDP/ring design.
//
// The mode validation, default application, and default-BPF construction in this
// file are tag-free and therefore always compiled and unit-testable.
package capture

import (
	"fmt"
	"time"

	"github.com/google/gopacket"

	"pgshadow/pkg/core"
)

// SSLConfig extends capture Config with SSL-specific settings.
// Used by ebpf_ssl mode only.
type SSLConfig struct {
	TargetPID  int    // 0 = capture all processes using libssl
	LibSSLPath string // path to libssl.so (auto-detected if empty)
}

// Capture modes (R1.2).
const (
	ModePcap     = "pcap"
	ModeAFPacket = "af_packet"
	ModeEBPF     = "ebpf"
	ModeEBPFSSL  = "ebpf_ssl"
)

// Documented capture defaults.
const (
	// DefaultMode is the capture mode used when none is configured (R1.2).
	DefaultMode = ModePcap
	// DefaultPort is the PostgreSQL port used to build the default BPF filter
	// when no capture port is configured (R1.3).
	DefaultPort = 5432
	// DefaultBufferSize is the capture buffer size used when none is
	// configured: 64 MiB (R1.4).
	DefaultBufferSize = 64 << 20
	// DefaultSnapLen is the per-packet capture length used when none is
	// configured. It is large enough to capture full PG wire-protocol messages
	// without truncation.
	DefaultSnapLen = 65536
)

// Source abstracts the capture mode (pcap | af_packet | ebpf). R1.2
type Source interface {
	// Packets returns a channel of captured packets; closed on shutdown.
	Packets() <-chan gopacket.Packet
	// Stats returns cumulative capture-layer counters (incl. drops). R1.7
	Stats() (CaptureStats, error)
	Close() error
}

// CaptureStats holds cumulative capture-layer counters. R1.7
type CaptureStats struct {
	Received uint64
	Dropped  uint64 // kernel + interface drops; feeds packet-drop metric
}

// Config configures the packet capture source.
type Config struct {
	Interface     string `yaml:"interface"`     // R1.1
	Port          int    `yaml:"port"`          // used to build default BPF
	Mode          string `yaml:"mode"`          // "pcap" | "af_packet" | "ebpf" | "ebpf_ssl"; default "pcap" R1.2
	BPFFilter     string `yaml:"bpf_filter"`    // default "tcp port <Port>" R1.3
	BufferSize    int    `yaml:"buffer_size"`   // bytes; default 64<<20 R1.4
	Bidirectional bool   `yaml:"bidirectional"` // default true R1.5
	SnapLen       int    `yaml:"snap_len"`
	SourceHost    string `yaml:"source_host"`   // source DB IP/hostname — filters traffic to/from this host only
	SourcePort    int    `yaml:"source_port"`   // source DB port (defaults to port if unset)
}

// withDefaults returns a copy of cfg with all documented capture defaults
// applied to unset fields (R1.2, R1.3, R1.4). The Bidirectional default of true
// (R1.5) is applied by the configuration loader, which can distinguish an unset
// field from an explicit false; capture itself takes Bidirectional as given.
func (c Config) withDefaults() Config {
	out := c
	if out.Mode == "" {
		out.Mode = DefaultMode
	}
	if out.Port == 0 {
		out.Port = DefaultPort
	}
	if out.BufferSize == 0 {
		out.BufferSize = DefaultBufferSize
	}
	if out.SnapLen == 0 {
		out.SnapLen = DefaultSnapLen
	}
	out.BPFFilter = buildBPFFilter(out)
	return out
}

// buildBPFFilter returns the configured BPF filter, or the default
// "tcp port <Port>" when none is configured (R1.3).
// When source_host is set, generates "tcp port <Port> and host <SourceHost>"
// to only capture traffic to/from the specified source database.
func buildBPFFilter(cfg Config) string {
	if cfg.BPFFilter != "" {
		return cfg.BPFFilter
	}
	port := cfg.Port
	if port == 0 {
		port = DefaultPort
	}

	// If source_host is specified, filter to only that host's traffic
	if cfg.SourceHost != "" {
		srcPort := cfg.SourcePort
		if srcPort == 0 {
			srcPort = port
		}
		return fmt.Sprintf("tcp port %d and host %s", srcPort, cfg.SourceHost)
	}

	return fmt.Sprintf("tcp port %d", port)
}

// validateMode reports whether mode is one of the supported capture modes
// (R1.2). The returned error names the offending mode.
func validateMode(mode string) error {
	switch mode {
	case ModePcap, ModeAFPacket, ModeEBPF, ModeEBPFSSL:
		return nil
	default:
		return fmt.Errorf("capture: invalid mode %q (must be one of %q, %q, %q, %q)",
			mode, ModePcap, ModeAFPacket, ModeEBPF, ModeEBPFSSL)
	}
}

// errCannotOpenInterface builds a fail-fast error that names the capture
// interface that could not be opened (R1.8).
func errCannotOpenInterface(iface string, cause error) error {
	if iface == "" {
		iface = "<empty>"
	}
	if cause == nil {
		return fmt.Errorf("capture: cannot open interface %q", iface)
	}
	return fmt.Errorf("capture: cannot open interface %q: %w", iface, cause)
}

// errInvalidBPF builds a fail-fast error that names the invalid BPF filter
// (R1.9).
func errInvalidBPF(filter string, cause error) error {
	if cause == nil {
		return fmt.Errorf("capture: invalid BPF filter %q", filter)
	}
	return fmt.Errorf("capture: invalid BPF filter %q: %w", filter, cause)
}

// Open validates and opens the capture source. It applies capture defaults,
// builds the default BPF filter when none is configured, validates the mode,
// validates the BPF filter syntax, and dispatches to the mode-specific
// implementation. It returns an error identifying the interface (R1.8) or the
// BPF filter (R1.9) when those are invalid, and never opens a writable
// socket to the network (R1.6).
func Open(cfg Config) (Source, error) {
	cfg = cfg.withDefaults()

	if cfg.Interface == "" {
		return nil, errCannotOpenInterface(cfg.Interface, fmt.Errorf("no capture interface configured"))
	}
	if err := validateMode(cfg.Mode); err != nil {
		return nil, err
	}

	// Validate BPF filter syntax (R1.9) - fail fast on invalid filters
	// Only validate user-provided custom filters, not the auto-generated ones
	if cfg.BPFFilter != "" && cfg.BPFFilter != buildBPFFilter(cfg) {
		// User provided a custom filter - validate it
		if err := QuickBPFFilterValidation(cfg.BPFFilter); err != nil {
			return nil, errInvalidBPF(cfg.BPFFilter, err)
		}
		
		// Try tcpdump validation if available (more thorough)
		if IsBPFToolAvailable() {
			if err := ValidateBPFFilterWithTimeout(cfg.BPFFilter, 5); err != nil {
				return nil, errInvalidBPF(cfg.BPFFilter, err)
			}
		}
	}

	switch cfg.Mode {
	case ModePcap:
		return newPcapSource(cfg)
	case ModeAFPacket:
		return newAFPacketSource(cfg)
	case ModeEBPF:
		// eBPF/XDP capture (R1.2).
		return newEBPFSource(cfg)
	case ModeEBPFSSL:
		// eBPF SSL uprobe capture — hooks SSL_read/SSL_write to capture
		// plaintext from TLS-encrypted connections.
		return newEBPFSSLSource(cfg, SSLConfig{})
	default:
		// Unreachable: validateMode already rejected unknown modes.
		return nil, validateMode(cfg.Mode)
	}
}

// Reassembler turns ordered TCP payloads into per-connection byte streams. R2
type Reassembler interface {
	// Assemble processes one packet, invoking the StreamHandler with ordered bytes.
	Assemble(pkt gopacket.Packet)
	// FlushOlderThan releases reassembly state idle past the timeout. R2.4
	FlushOlderThan(idle time.Duration) (flushed int)
	Close()
}

// StreamHandler receives contiguous, in-order payload bytes for one direction
// of one ConnID, plus a flag indicating client->server vs server->client.
type StreamHandler interface {
	OnBytes(conn core.ConnID, fromClient bool, data []byte)
	OnClose(conn core.ConnID) // R2.5
}
