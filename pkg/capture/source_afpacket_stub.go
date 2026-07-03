//go:build !linux || !pcap

// This stub is compiled whenever the real AF_PACKET implementation is not:
// on non-Linux platforms, or on Linux without the `pcap` build tag. It keeps
// the default build dependency-free while returning a clear runtime error if
// af_packet capture is requested. The real implementation lives in
// source_afpacket.go (`//go:build linux && pcap`).
package capture

import (
	"fmt"
	"runtime"
)

// newAFPacketSource always fails when the real AF_PACKET implementation is not
// compiled in. AF_PACKET is Linux-only and its BPF filter is compiled with
// libpcap, so the real path requires `GOOS=linux` and `-tags pcap`.
func newAFPacketSource(cfg Config) (Source, error) {
	if runtime.GOOS != "linux" {
		return nil, errCannotOpenInterface(cfg.Interface,
			fmt.Errorf("af_packet mode requires Linux (current GOOS=%s)", runtime.GOOS))
	}
	return nil, errCannotOpenInterface(cfg.Interface,
		fmt.Errorf("af_packet mode is unavailable in this build; rebuild with -tags pcap (requires cgo + libpcap)"))
}
