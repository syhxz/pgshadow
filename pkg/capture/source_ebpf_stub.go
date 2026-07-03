//go:build !linux || !ebpf

// This stub is compiled whenever the real eBPF/XDP implementation is not:
// on non-Linux platforms, or on Linux without the `ebpf` build tag. It keeps
// the default `go build ./...` free of any eBPF loader dependency (e.g.
// github.com/cilium/ebpf) while returning a clear runtime error if `ebpf`
// capture mode is requested. The real implementation lives in source_ebpf.go
// (`//go:build linux && ebpf`).
package capture

import (
	"fmt"
	"runtime"
)

// newEBPFSource always fails when the real eBPF implementation is not compiled
// in. eBPF/XDP capture is Linux-only and requires a recent kernel plus elevated
// privileges (CAP_BPF / CAP_NET_ADMIN), so the real path requires `GOOS=linux`
// and `-tags ebpf`.
func newEBPFSource(cfg Config) (Source, error) {
	if runtime.GOOS != "linux" {
		return nil, errCannotOpenInterface(cfg.Interface,
			fmt.Errorf("ebpf mode requires Linux (current GOOS=%s)", runtime.GOOS))
	}
	return nil, errCannotOpenInterface(cfg.Interface,
		fmt.Errorf("ebpf mode is unavailable in this build; rebuild with -tags ebpf "+
			"(requires a recent Linux kernel, an eBPF loader dependency, and CAP_BPF/CAP_NET_ADMIN)"))
}
