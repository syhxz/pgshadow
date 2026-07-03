//go:build !linux || !ebpf

package capture

import (
	"fmt"
	"runtime"
)

// newEBPFSSLSource is the stub for non-Linux/non-eBPF builds.
func newEBPFSSLSource(cfg Config, sslCfg SSLConfig) (Source, error) {
	if runtime.GOOS != "linux" {
		return nil, errCannotOpenInterface(cfg.Interface,
			fmt.Errorf("ebpf_ssl mode requires Linux (current GOOS=%s)", runtime.GOOS))
	}
	return nil, errCannotOpenInterface(cfg.Interface,
		fmt.Errorf("ebpf_ssl mode is unavailable in this build; rebuild with -tags ebpf"))
}
