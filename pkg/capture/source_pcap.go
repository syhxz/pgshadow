//go:build pcap

// This file holds the libpcap-backed capture Source for `pcap` mode. It is only
// compiled when the `pcap` build tag is set (which requires cgo and the libpcap
// development headers). Build with: go build -tags pcap ./...
package capture

import (
	"sync"

	"github.com/google/gopacket"
	"github.com/google/gopacket/pcap"
)

// pcapSource is a read-only capture Source backed by libpcap (R1.2). It never
// transmits on the interface (R1.6); the handle is used only for reading.
type pcapSource struct {
	handle  *pcap.Handle
	packets <-chan gopacket.Packet

	closeOnce sync.Once
}

// newPcapSource opens the configured interface with libpcap, applies the
// requested capture buffer size and snap length, installs the BPF filter, and
// begins delivering packets. It returns an error naming the interface (R1.8) or
// the BPF filter (R1.9) when those cannot be opened/compiled.
func newPcapSource(cfg Config) (Source, error) {
	inactive, err := pcap.NewInactiveHandle(cfg.Interface)
	if err != nil {
		return nil, errCannotOpenInterface(cfg.Interface, err)
	}
	// CleanUp is a no-op once the handle is activated; safe to always defer.
	defer inactive.CleanUp()

	if err := inactive.SetSnapLen(cfg.SnapLen); err != nil {
		return nil, errCannotOpenInterface(cfg.Interface, err)
	}
	// Promiscuous mode: the Bypass_Server observes a mirrored copy of traffic
	// that is not addressed to it, so promiscuous capture is required.
	if err := inactive.SetPromisc(true); err != nil {
		return nil, errCannotOpenInterface(cfg.Interface, err)
	}
	if err := inactive.SetTimeout(pcap.BlockForever); err != nil {
		return nil, errCannotOpenInterface(cfg.Interface, err)
	}
	if cfg.BufferSize > 0 {
		if err := inactive.SetBufferSize(cfg.BufferSize); err != nil {
			return nil, errCannotOpenInterface(cfg.Interface, err)
		}
	}

	handle, err := inactive.Activate()
	if err != nil {
		return nil, errCannotOpenInterface(cfg.Interface, err)
	}

	// Capture only ingress when bidirectional capture is disabled; the Master
	// receives client ingress and distributes work to segments (R8.3, R1.5).
	if !cfg.Bidirectional {
		if err := handle.SetDirection(pcap.DirectionIn); err != nil {
			handle.Close()
			return nil, errCannotOpenInterface(cfg.Interface, err)
		}
	}

	if err := handle.SetBPFFilter(cfg.BPFFilter); err != nil {
		handle.Close()
		return nil, errInvalidBPF(cfg.BPFFilter, err)
	}

	src := gopacket.NewPacketSource(handle, handle.LinkType())
	return &pcapSource{
		handle:  handle,
		packets: src.Packets(),
	}, nil
}

func (s *pcapSource) Packets() <-chan gopacket.Packet { return s.packets }

// Stats reports cumulative received and dropped counts from the kernel/libpcap
// capture layer. Both kernel drops and interface drops are summed into Dropped
// (R1.7).
func (s *pcapSource) Stats() (CaptureStats, error) {
	st, err := s.handle.Stats()
	if err != nil {
		return CaptureStats{}, err
	}
	return CaptureStats{
		Received: uint64(st.PacketsReceived),
		Dropped:  uint64(st.PacketsDropped) + uint64(st.PacketsIfDropped),
	}, nil
}

func (s *pcapSource) Close() error {
	s.closeOnce.Do(func() {
		s.handle.Close()
	})
	return nil
}
