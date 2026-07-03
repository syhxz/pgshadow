package capture

import (
	"strings"
	"testing"
)

// These tests are tag-free: they exercise the default-application, BPF
// construction, mode validation, and error-naming logic that does not require
// libpcap or a live NIC. Tests that need libpcap (interface-open failure, BPF
// compilation) live in source_pcap_test.go behind `//go:build pcap`.

func TestWithDefaults_AppliesDocumentedDefaults(t *testing.T) {
	got := Config{Interface: "eth0"}.withDefaults()

	if got.Mode != ModePcap {
		t.Errorf("Mode default = %q, want %q (R1.2)", got.Mode, ModePcap)
	}
	if got.Port != DefaultPort {
		t.Errorf("Port default = %d, want %d", got.Port, DefaultPort)
	}
	if got.BufferSize != DefaultBufferSize {
		t.Errorf("BufferSize default = %d, want %d (R1.4)", got.BufferSize, DefaultBufferSize)
	}
	if got.SnapLen != DefaultSnapLen {
		t.Errorf("SnapLen default = %d, want %d", got.SnapLen, DefaultSnapLen)
	}
	if got.BPFFilter != "tcp port 5432" {
		t.Errorf("BPFFilter default = %q, want %q (R1.3)", got.BPFFilter, "tcp port 5432")
	}
}

func TestWithDefaults_PreservesExplicitValues(t *testing.T) {
	in := Config{
		Interface:  "eth1",
		Port:       6000,
		Mode:       ModeAFPacket,
		BPFFilter:  "tcp port 6000 and host 10.0.0.1",
		BufferSize: 1 << 20,
		SnapLen:    1500,
	}
	got := in.withDefaults()

	if got.Mode != ModeAFPacket {
		t.Errorf("Mode = %q, want %q", got.Mode, ModeAFPacket)
	}
	if got.BufferSize != 1<<20 {
		t.Errorf("BufferSize = %d, want %d", got.BufferSize, 1<<20)
	}
	if got.SnapLen != 1500 {
		t.Errorf("SnapLen = %d, want %d", got.SnapLen, 1500)
	}
	if got.BPFFilter != "tcp port 6000 and host 10.0.0.1" {
		t.Errorf("BPFFilter = %q, want the configured filter unchanged", got.BPFFilter)
	}
}

func TestBuildBPFFilter(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"default port", Config{}, "tcp port 5432"},
		{"configured port", Config{Port: 6543}, "tcp port 6543"},
		{"explicit filter wins", Config{Port: 6543, BPFFilter: "tcp port 1234"}, "tcp port 1234"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildBPFFilter(tt.cfg); got != tt.want {
				t.Errorf("buildBPFFilter() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateMode(t *testing.T) {
	for _, mode := range []string{ModePcap, ModeAFPacket, ModeEBPF} {
		if err := validateMode(mode); err != nil {
			t.Errorf("validateMode(%q) returned error: %v", mode, err)
		}
	}

	err := validateMode("sniff")
	if err == nil {
		t.Fatal("validateMode(\"sniff\") = nil, want error")
	}
	if !strings.Contains(err.Error(), "sniff") {
		t.Errorf("error %q does not name the offending mode %q", err.Error(), "sniff")
	}
}

func TestOpen_InvalidModeNamesMode(t *testing.T) {
	_, err := Open(Config{Interface: "eth0", Mode: "bogus"})
	if err == nil {
		t.Fatal("Open with invalid mode = nil error, want error")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Errorf("error %q does not name the invalid mode (R1.2)", err.Error())
	}
}

func TestOpen_EmptyInterfaceNamesInterface(t *testing.T) {
	_, err := Open(Config{Mode: ModePcap})
	if err == nil {
		t.Fatal("Open with empty interface = nil error, want error (R1.8)")
	}
	if !strings.Contains(err.Error(), "interface") {
		t.Errorf("error %q does not identify the capture interface (R1.8)", err.Error())
	}
}

// TestOpen_EBPFStubError asserts that in the default build (no `ebpf` tag) the
// ebpf capture path is wired through newEBPFSource and returns the documented,
// actionable stub error rather than silently succeeding (R1.2). The real
// loader is isolated behind `//go:build linux && ebpf` (source_ebpf.go); this
// test exercises the stub in source_ebpf_stub.go.
func TestOpen_EBPFStubError(t *testing.T) {
	_, err := Open(Config{Interface: "eth0", Mode: ModeEBPF})
	if err == nil {
		t.Fatal("Open with ebpf mode = nil error, want stub error")
	}
	// The stub error names the ebpf mode and the interface that could not be
	// opened (R1.8), mirroring the af_packet stub contract.
	if !strings.Contains(err.Error(), ModeEBPF) {
		t.Errorf("error %q does not name the ebpf mode", err.Error())
	}
	if !strings.Contains(err.Error(), "eth0") {
		t.Errorf("error %q does not name the capture interface (R1.8)", err.Error())
	}
}

func TestErrCannotOpenInterface_NamesInterface(t *testing.T) {
	err := errCannotOpenInterface("eth9", nil)
	if !strings.Contains(err.Error(), "eth9") {
		t.Errorf("error %q does not name the interface (R1.8)", err.Error())
	}
}

func TestErrInvalidBPF_NamesFilter(t *testing.T) {
	err := errInvalidBPF("tcp porttt 5432", nil)
	if !strings.Contains(err.Error(), "tcp porttt 5432") {
		t.Errorf("error %q does not name the invalid BPF filter (R1.9)", err.Error())
	}
}
