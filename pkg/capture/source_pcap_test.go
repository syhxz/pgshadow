//go:build pcap

package capture

import (
	"strings"
	"testing"
)

// These tests require the libpcap-backed implementation (`-tags pcap`). They do
// not require root: opening a bogus interface fails fast (either "no such
// device" or a permission error) before any privileged operation, and in both
// cases the error must name the interface (R1.8).

func TestOpen_BogusInterfaceFailsAndNamesInterface(t *testing.T) {
	const bogus = "pgshadow-nonexistent-iface0"
	_, err := Open(Config{Interface: bogus, Mode: ModePcap})
	if err == nil {
		t.Fatalf("Open(%q) = nil error, want fail-fast error (R1.8)", bogus)
	}
	if !strings.Contains(err.Error(), bogus) {
		t.Errorf("error %q does not name the capture interface %q (R1.8)", err.Error(), bogus)
	}
}
