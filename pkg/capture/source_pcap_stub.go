//go:build !pcap

// This stub is compiled for the default build (no `pcap` tag). It keeps
// `go build ./...` free of any cgo/libpcap dependency while still returning a
// clear, actionable runtime error if pcap capture is requested. Build the real
// implementation with `-tags pcap`.
package capture

import "fmt"

// newPcapSource always fails in the default build. The libpcap-backed
// implementation lives in source_pcap.go and is compiled only with `-tags pcap`.
func newPcapSource(cfg Config) (Source, error) {
	return nil, errCannotOpenInterface(cfg.Interface,
		fmt.Errorf("pcap mode is unavailable in this build; rebuild with -tags pcap (requires cgo + libpcap)"))
}
