//go:build linux && pcap

// This file holds the AF_PACKET capture Source for `af_packet` mode. AF_PACKET
// is a Linux-only kernel facility, and the BPF filter is compiled with libpcap,
// so this file is compiled only on Linux with the `pcap` build tag. On any
// other platform/tag combination the stub in source_afpacket_stub.go is used.
//
// Build with: GOOS=linux go build -tags pcap ./...
// (this adds a dependency on golang.org/x/net/bpf; run `go mod tidy` for the
// linux+pcap build to record it).
package capture

import (
	"sync"

	"github.com/google/gopacket"
	"github.com/google/gopacket/afpacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"golang.org/x/net/bpf"
)

// afpacketSource is a read-only capture Source backed by an AF_PACKET TPacketV3
// ring (R1.2). It only reads from the ring and never transmits (R1.6).
type afpacketSource struct {
	tp      *afpacket.TPacket
	packets <-chan gopacket.Packet

	closeOnce sync.Once
}

// newAFPacketSource binds an AF_PACKET ring to the configured interface, sizes
// the ring from the configured capture buffer size, compiles and installs the
// BPF filter via libpcap, and begins delivering packets. It returns an error
// naming the interface (R1.8) or the BPF filter (R1.9) on failure.
func newAFPacketSource(cfg Config) (Source, error) {
	frameSize, blockSize, numBlocks := afpacketSizing(cfg)

	tp, err := afpacket.NewTPacket(
		afpacket.OptInterface(cfg.Interface),
		afpacket.OptFrameSize(frameSize),
		afpacket.OptBlockSize(blockSize),
		afpacket.OptNumBlocks(numBlocks),
		afpacket.OptPollTimeout(pcap.BlockForever),
		afpacket.SocketRaw,
		afpacket.TPacketVersion3,
	)
	if err != nil {
		return nil, errCannotOpenInterface(cfg.Interface, err)
	}

	// Compile the tcpdump-syntax BPF expression with libpcap and install it on
	// the AF_PACKET socket so filtering happens in the kernel (R1.3).
	insns, err := pcap.CompileBPFFilter(layers.LinkTypeEthernet, cfg.SnapLen, cfg.BPFFilter)
	if err != nil {
		tp.Close()
		return nil, errInvalidBPF(cfg.BPFFilter, err)
	}
	raw := make([]bpf.RawInstruction, len(insns))
	for i, ins := range insns {
		raw[i] = bpf.RawInstruction{Op: ins.Code, Jt: ins.Jt, Jf: ins.Jf, K: ins.K}
	}
	if err := tp.SetBPF(raw); err != nil {
		tp.Close()
		return nil, errInvalidBPF(cfg.BPFFilter, err)
	}

	src := gopacket.NewPacketSource(tp, layers.LinkTypeEthernet)
	return &afpacketSource{
		tp:      tp,
		packets: src.Packets(),
	}, nil
}

// afpacketSizing derives TPacket frame/block/num-block ring parameters from the
// configured snap length and capture buffer size, falling back to the
// afpacket package defaults.
func afpacketSizing(cfg Config) (frameSize, blockSize, numBlocks int) {
	frameSize = afpacket.DefaultFrameSize
	if cfg.SnapLen > frameSize {
		// Round the frame size up to the next power of two >= snaplen so that a
		// full snapshot fits in a single frame.
		frameSize = 1
		for frameSize < cfg.SnapLen {
			frameSize <<= 1
		}
	}
	blockSize = afpacket.DefaultBlockSize
	if blockSize < frameSize {
		blockSize = frameSize
	}
	numBlocks = afpacket.DefaultNumBlocks
	if cfg.BufferSize > 0 {
		if n := cfg.BufferSize / blockSize; n > numBlocks {
			numBlocks = n
		}
	}
	return frameSize, blockSize, numBlocks
}

func (s *afpacketSource) Packets() <-chan gopacket.Packet { return s.packets }

// Stats reports cumulative received and dropped counts from the AF_PACKET
// socket statistics (R1.7).
func (s *afpacketSource) Stats() (CaptureStats, error) {
	_, v3, err := s.tp.SocketStats()
	if err != nil {
		return CaptureStats{}, err
	}
	return CaptureStats{
		Received: uint64(v3.Packets()),
		Dropped:  uint64(v3.Drops()),
	}, nil
}

func (s *afpacketSource) Close() error {
	s.closeOnce.Do(func() {
		s.tp.Close()
	})
	return nil
}
