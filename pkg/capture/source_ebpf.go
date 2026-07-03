//go:build linux && ebpf

// This file implements the eBPF/XDP capture Source for `ebpf` mode (R1.2).
// It loads a compiled XDP program that mirrors matched TCP packets (by port)
// into a BPF ring buffer, then reads them in userspace and forwards as
// gopacket.Packet onto the packets channel.
//
// The XDP program always returns XDP_PASS so the kernel still delivers the
// packet normally — this is a read-only mirror that never transmits (R1.6).
//
// Build with: GOOS=linux go build -tags ebpf ./...
//
// Requires:
//   - Linux kernel ≥ 5.8 (ring buffer support)
//   - BTF enabled (/sys/kernel/btf/vmlinux)
//   - CAP_BPF + CAP_NET_ADMIN (or root)
package capture

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

// ebpfSource is a read-only capture Source backed by an eBPF/XDP program that
// mirrors matched frames into a kernel ring buffer (R1.2). It only reads from
// the ring and never transmits (R1.6).
type ebpfSource struct {
	packets chan gopacket.Packet

	objs   *pgshadowXdpObjects
	xdpLnk link.Link
	reader  *ringbuf.Reader

	received atomic.Uint64
	dropped  atomic.Uint64

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup
}

// pktRecord header matches struct pkt_record in the BPF C program.
// The ring buffer record is: [len(4)] [cap_len(4)] [data(FIXED_SNAP_LEN)]
// Only the first cap_len bytes of data are valid.
const (
	pktRecordHeaderSize = 8    // 2 x uint32 (len + cap_len)
	fixedSnapLen        = 4096 // must match FIXED_SNAP_LEN in the BPF C code
)

// newEBPFSource loads the XDP program, attaches it to the configured interface,
// opens the ring buffer reader, and starts the packet reader goroutine.
func newEBPFSource(cfg Config) (Source, error) {
	// Remove rlimit restriction for BPF (kernels < 5.11 need this)
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, errCannotOpenInterface(cfg.Interface,
			fmt.Errorf("ebpf: failed to remove memlock rlimit: %w", err))
	}

	// Load the compiled BPF objects (program + maps)
	var objs pgshadowXdpObjects
	if err := loadPgshadowXdpObjects(&objs, &ebpf.CollectionOptions{}); err != nil {
		return nil, errCannotOpenInterface(cfg.Interface,
			fmt.Errorf("ebpf: failed to load BPF objects: %w", err))
	}

	// Configure the target port in the BPF map
	port := cfg.Port
	if port == 0 {
		port = DefaultPort
	}
	portNet := hostToNet16(uint16(port))
	key := uint32(0)
	if err := objs.CfgPort.Update(key, portNet, ebpf.UpdateAny); err != nil {
		objs.Close()
		return nil, errCannotOpenInterface(cfg.Interface,
			fmt.Errorf("ebpf: failed to set capture port %d in BPF map: %w", port, err))
	}

	// Attach XDP program to the interface.
	// Try native mode first (highest performance); automatically fall back to
	// generic/SKB mode if the NIC driver doesn't support native XDP (e.g., AWS
	// ENA, virtio). Loopback also uses generic mode transparently.
	iface := cfg.Interface
	idx := ifaceIndex(iface)
	xdpLnk, err := link.AttachXDP(link.XDPOptions{
		Program:   objs.PgshadowXdp,
		Interface: idx,
	})
	if err != nil {
		// Fallback to generic/SKB mode
		xdpLnk, err = link.AttachXDP(link.XDPOptions{
			Program:   objs.PgshadowXdp,
			Interface: idx,
			Flags:     link.XDPGenericMode,
		})
		if err != nil {
			objs.Close()
			return nil, errCannotOpenInterface(iface,
				fmt.Errorf("ebpf: failed to attach XDP to interface %q (tried native and generic mode): %w", iface, err))
		}
	}

	// Open ring buffer reader
	rd, err := ringbuf.NewReader(objs.PktRing)
	if err != nil {
		xdpLnk.Close()
		objs.Close()
		return nil, errCannotOpenInterface(iface,
			fmt.Errorf("ebpf: failed to open ring buffer: %w", err))
	}

	s := &ebpfSource{
		packets: make(chan gopacket.Packet, 4096),
		objs:    &objs,
		xdpLnk: xdpLnk,
		reader:  rd,
		done:    make(chan struct{}),
	}

	// Start the reader goroutine that drains the ring buffer
	s.wg.Add(1)
	go s.readLoop()

	return s, nil
}

// readLoop reads records from the BPF ring buffer, decodes the packet data,
// and forwards them onto the packets channel. It exits when the reader is
// closed (triggered by Close).
func (s *ebpfSource) readLoop() {
	defer s.wg.Done()
	defer close(s.packets)

	for {
		record, err := s.reader.Read()
		if err != nil {
			// Reader was closed — normal shutdown path
			return
		}

		raw := record.RawSample
		// Record layout: [len:4][cap_len:4][data:FIXED_SNAP_LEN]
		if len(raw) < pktRecordHeaderSize+1 {
			continue // malformed record, skip
		}

		// Parse header
		frameLen := binary.LittleEndian.Uint32(raw[0:4])
		capLen := binary.LittleEndian.Uint32(raw[4:8])
		_ = frameLen // original length, not used for decoding

		dataStart := pktRecordHeaderSize
		if capLen > fixedSnapLen {
			capLen = fixedSnapLen
		}
		if uint32(len(raw)-dataStart) < capLen {
			continue // truncated record
		}
		pktData := raw[dataStart : dataStart+int(capLen)]

		// Decode as Ethernet frame (XDP captures at L2)
		pkt := gopacket.NewPacket(
			pktData,
			layers.LayerTypeEthernet,
			gopacket.NoCopy,
		)

		s.received.Add(1)

		// Non-blocking send: if the channel is full, increment dropped counter
		select {
		case s.packets <- pkt:
		default:
			s.dropped.Add(1)
		}
	}
}

// Packets returns the channel of captured packets; it is closed on shutdown.
func (s *ebpfSource) Packets() <-chan gopacket.Packet { return s.packets }

// Stats reports cumulative received and dropped counts. It reads from the BPF
// per-CPU stats map for kernel-side drops and combines with userspace drops
// (channel overflow).
func (s *ebpfSource) Stats() (CaptureStats, error) {
	var kernelReceived, kernelDropped uint64

	// Read per-CPU stats from BPF map
	if s.objs != nil && s.objs.Stats != nil {
		var values []uint64

		keyRecv := uint32(0)
		if err := s.objs.Stats.Lookup(keyRecv, &values); err == nil {
			for _, v := range values {
				kernelReceived += v
			}
		}

		keyDrop := uint32(1)
		if err := s.objs.Stats.Lookup(keyDrop, &values); err == nil {
			for _, v := range values {
				kernelDropped += v
			}
		}
	}

	// Combine kernel stats with userspace stats
	received := s.received.Load()
	if kernelReceived > received {
		received = kernelReceived
	}
	dropped := s.dropped.Load() + kernelDropped

	return CaptureStats{Received: received, Dropped: dropped}, nil
}

// Close detaches the XDP program and releases all eBPF resources. It is
// idempotent.
func (s *ebpfSource) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.done)

		// Close the ring buffer reader — this unblocks readLoop
		if s.reader != nil {
			s.reader.Close()
		}

		// Wait for the reader goroutine to finish
		s.wg.Wait()

		// Detach XDP program
		if s.xdpLnk != nil {
			if err := s.xdpLnk.Close(); err != nil {
				closeErr = fmt.Errorf("ebpf: detach XDP: %w", err)
			}
		}

		// Close BPF objects (maps + program)
		if s.objs != nil {
			s.objs.Close()
		}
	})
	return closeErr
}

// ifaceIndex returns the interface index for the given interface name.
func ifaceIndex(name string) int {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return 0
	}
	return iface.Index
}

// hostToNet16 converts a uint16 from host byte order to network byte order (big-endian).
func hostToNet16(v uint16) uint16 {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return binary.LittleEndian.Uint16(b)
}
