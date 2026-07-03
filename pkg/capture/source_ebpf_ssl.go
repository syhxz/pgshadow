//go:build linux && ebpf

// source_ebpf_ssl.go implements the eBPF SSL uprobe capture Source (mode "ebpf_ssl").
//
// It hooks SSL_read and SSL_write via uprobes on the target libssl.so, capturing
// plaintext PG wire protocol data from TLS-encrypted connections without any
// decryption keys or MITM proxy. This is fully passive — it reads the plaintext
// buffer *after* OpenSSL has processed it.
//
// Architecture:
//   Client → [TLS encrypted] → PostgreSQL (SSL_read/SSL_write) → uprobe → ring buffer → pgshadow
//
// The captured plaintext data is reconstructed into gopacket-compatible packets
// (TCP payload only) and fed into the same reassembler pipeline as other capture
// modes.
package capture

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
	"github.com/cilium/ebpf/ringbuf"
	"github.com/cilium/ebpf/rlimit"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
)

// sslEvent matches struct ssl_event in the BPF C program.
type sslEvent struct {
	PID         uint32
	TID         uint32
	FD          uint32
	DataLen     uint32
	Direction   uint8 // 0=read (client→server), 1=write (server→client)
	_           [3]byte
	TimestampNs uint64
	Data        [4096]byte
}

const sslEventSize = 4 + 4 + 4 + 4 + 1 + 7 + 8 + 4096 // = 4128 (matches C struct with alignment padding)

// ebpfSSLSource captures plaintext from TLS-encrypted PG connections via
// SSL_read/SSL_write uprobes. Fully passive, never modifies traffic.
type ebpfSSLSource struct {
	packets chan gopacket.Packet

	objs   *pgshadowSslObjects
	reader *ringbuf.Reader

	// uprobe links (need to keep alive while capturing)
	readEnterLink  link.Link
	readExitLink   link.Link
	writeEnterLink link.Link
	writeExitLink  link.Link

	received atomic.Uint64
	dropped  atomic.Uint64

	closeOnce sync.Once
	done      chan struct{}
	wg        sync.WaitGroup

	// fd→socket mapping cache for ConnID reconstruction
	targetPID int

	// seqNums tracks per-connection TCP sequence numbers so the reassembler
	// correctly orders multiple SSL records from the same connection. Only
	// accessed from the single readLoop goroutine — no lock needed.
	seqNums map[uint64]uint32 // key: (pid<<32|tid)<<1|direction → next TCP seq number
}

// newEBPFSSLSource creates and starts the SSL uprobe capture source.
func newEBPFSSLSource(cfg Config, sslCfg SSLConfig) (Source, error) {
	if err := rlimit.RemoveMemlock(); err != nil {
		return nil, fmt.Errorf("ebpf_ssl: remove memlock: %w", err)
	}

	// Auto-detect libssl path if not specified
	libsslPath := sslCfg.LibSSLPath
	if libsslPath == "" {
		var err error
		libsslPath, err = findLibSSL(sslCfg.TargetPID)
		if err != nil {
			return nil, fmt.Errorf("ebpf_ssl: auto-detect libssl: %w", err)
		}
	}

	// Load BPF objects
	var objs pgshadowSslObjects
	if err := loadPgshadowSslObjects(&objs, &ebpf.CollectionOptions{}); err != nil {
		return nil, fmt.Errorf("ebpf_ssl: load BPF objects: %w", err)
	}

	// Set target PID filter (0 = capture all)
	key := uint32(0)
	pid := uint32(sslCfg.TargetPID)
	if err := objs.TargetPid.Update(key, pid, ebpf.UpdateAny); err != nil {
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: set target PID: %w", err)
	}

	// Open the executable for uprobe attachment
	ex, err := link.OpenExecutable(libsslPath)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: open %q: %w", libsslPath, err)
	}

	// Attach uprobes/uretprobes to SSL_read and SSL_write
	readEnter, err := ex.Uprobe("SSL_read", objs.UprobeSslReadEnter, nil)
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: attach uprobe SSL_read: %w", err)
	}

	readExit, err := ex.Uretprobe("SSL_read", objs.UretprobeSslReadExit, nil)
	if err != nil {
		readEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: attach uretprobe SSL_read: %w", err)
	}

	writeEnter, err := ex.Uprobe("SSL_write", objs.UprobeSslWriteEnter, nil)
	if err != nil {
		readExit.Close()
		readEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: attach uprobe SSL_write: %w", err)
	}

	writeExit, err := ex.Uretprobe("SSL_write", objs.UretprobeSslWriteExit, nil)
	if err != nil {
		writeEnter.Close()
		readExit.Close()
		readEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: attach uretprobe SSL_write: %w", err)
	}

	// Open ring buffer reader
	rd, err := ringbuf.NewReader(objs.SslEvents)
	if err != nil {
		writeExit.Close()
		writeEnter.Close()
		readExit.Close()
		readEnter.Close()
		objs.Close()
		return nil, fmt.Errorf("ebpf_ssl: open ring buffer: %w", err)
	}

	s := &ebpfSSLSource{
		packets:        make(chan gopacket.Packet, 4096),
		objs:           &objs,
		reader:         rd,
		readEnterLink:  readEnter,
		readExitLink:   readExit,
		writeEnterLink: writeEnter,
		writeExitLink:  writeExit,
		done:           make(chan struct{}),
		targetPID:      sslCfg.TargetPID,
		seqNums:        make(map[uint64]uint32),
	}

	s.wg.Add(1)
	go s.readLoop()

	return s, nil
}

// readLoop reads SSL events from the ring buffer and converts them to packets.
func (s *ebpfSSLSource) readLoop() {
	defer s.wg.Done()
	defer close(s.packets)

	for {
		record, err := s.reader.Read()
		if err != nil {
			return // reader closed
		}

		raw := record.RawSample
		if len(raw) < sslEventSize {
			continue
		}

		// Parse the SSL event
		ev := parseSslEvent(raw)
		if ev.DataLen == 0 {
			continue
		}

		dataLen := ev.DataLen
		if dataLen > 4096 {
			dataLen = 4096
		}

		// Get next sequence number for this connection+direction.
		seq := s.nextSeq(ev.PID, ev.TID, ev.Direction, dataLen)

		// Build a synthetic TCP packet with the plaintext payload and correct seq.
		pkt := s.buildSyntheticPacket(ev.Data[:dataLen], ev.Direction, ev.PID, ev.TID, ev.TimestampNs, seq)

		s.received.Add(1)

		select {
		case s.packets <- pkt:
		default:
			s.dropped.Add(1)
		}
	}
}

// nextSeq returns the current TCP sequence number for the given connection
// (identified by PID/TID/direction) and advances it by dataLen bytes. This
// ensures the TCP reassembler sees monotonically increasing sequence numbers
// and correctly reassembles multiple SSL_read/SSL_write calls on the same
// connection (critical for Extended Query: Parse→Bind→Execute).
func (s *ebpfSSLSource) nextSeq(pid, tid uint32, direction uint8, dataLen uint32) uint32 {
	key := (uint64(pid)<<32 | uint64(tid))<<1 | uint64(direction)
	seq := s.seqNums[key]
	if seq == 0 {
		seq = 1 // start at 1 like a real TCP connection post-handshake
	}
	s.seqNums[key] = seq + dataLen
	return seq
}

// parseSslEvent decodes the raw ring buffer record into an sslEvent.
// The C struct layout (with alignment padding for uint64) is:
//   offset 0:  pid (u32)
//   offset 4:  tid (u32)
//   offset 8:  fd (u32)
//   offset 12: data_len (u32)
//   offset 16: direction (u8)
//   offset 17: _pad[3] + 4 bytes implicit alignment padding
//   offset 24: timestamp_ns (u64)
//   offset 32: data[4096]
func parseSslEvent(raw []byte) sslEvent {
	var ev sslEvent
	ev.PID = binary.LittleEndian.Uint32(raw[0:4])
	ev.TID = binary.LittleEndian.Uint32(raw[4:8])
	ev.FD = binary.LittleEndian.Uint32(raw[8:12])
	ev.DataLen = binary.LittleEndian.Uint32(raw[12:16])
	ev.Direction = raw[16]
	ev.TimestampNs = binary.LittleEndian.Uint64(raw[24:32])
	copy(ev.Data[:], raw[32:])
	return ev
}

// buildSyntheticPacket creates a gopacket.Packet from plaintext SSL data.
// It wraps the payload in synthetic Ethernet/IP/TCP headers so the existing
// reassembler and protocol parser can process it unchanged. The seq parameter
// provides a monotonically increasing TCP sequence number per connection so the
// reassembler correctly orders multiple SSL_read/SSL_write calls (required for
// Extended Query protocol: Parse→Bind→Execute arrive as separate SSL records).
func (s *ebpfSSLSource) buildSyntheticPacket(payload []byte, direction uint8, pid, tid uint32, tsNs uint64, seq uint32) gopacket.Packet {
	// Synthetic addresses — use PID/TID as port to differentiate connections
	srcPort := uint16(tid & 0xFFFF)
	dstPort := uint16(5432)
	srcIP := net.IPv4(127, 0, 0, 1)
	dstIP := net.IPv4(127, 0, 0, 1)

	if direction == 1 { // write = server→client
		srcPort, dstPort = dstPort, srcPort
	}

	// Build the packet layers
	eth := &layers.Ethernet{
		SrcMAC:       net.HardwareAddr{0, 0, 0, 0, 0, 1},
		DstMAC:       net.HardwareAddr{0, 0, 0, 0, 0, 2},
		EthernetType: layers.EthernetTypeIPv4,
	}
	ip := &layers.IPv4{
		Version:  4,
		IHL:      5,
		TTL:      64,
		Protocol: layers.IPProtocolTCP,
		SrcIP:    srcIP,
		DstIP:    dstIP,
		Length:   uint16(20 + 20 + len(payload)),
	}
	tcp := &layers.TCP{
		SrcPort: layers.TCPPort(srcPort),
		DstPort: layers.TCPPort(dstPort),
		Seq:     seq,
		PSH:     true,
		ACK:     true,
		Window:  65535,
	}
	tcp.SetNetworkLayerForChecksum(ip)

	buf := gopacket.NewSerializeBuffer()
	opts := gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true}
	_ = gopacket.SerializeLayers(buf, opts, eth, ip, tcp, gopacket.Payload(payload))

	return gopacket.NewPacket(buf.Bytes(), layers.LayerTypeEthernet, gopacket.Default)
}

// Packets returns the channel of captured packets.
func (s *ebpfSSLSource) Packets() <-chan gopacket.Packet { return s.packets }

// Stats returns capture statistics.
func (s *ebpfSSLSource) Stats() (CaptureStats, error) {
	return CaptureStats{
		Received: s.received.Load(),
		Dropped:  s.dropped.Load(),
	}, nil
}

// Close detaches all uprobes and releases resources. Idempotent.
func (s *ebpfSSLSource) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.done)
		if s.reader != nil {
			s.reader.Close()
		}
		s.wg.Wait()

		// Detach uprobes
		if s.readEnterLink != nil {
			s.readEnterLink.Close()
		}
		if s.readExitLink != nil {
			s.readExitLink.Close()
		}
		if s.writeEnterLink != nil {
			s.writeEnterLink.Close()
		}
		if s.writeExitLink != nil {
			s.writeExitLink.Close()
		}
		// Close BPF objects
		if s.objs != nil {
			s.objs.Close()
		}
	})
	return closeErr
}

// findLibSSL auto-detects the libssl.so path used by the target process (or
// the system default if targetPID is 0).
func findLibSSL(targetPID int) (string, error) {
	if targetPID > 0 {
		// Read from /proc/PID/maps
		mapsPath := fmt.Sprintf("/proc/%d/maps", targetPID)
		data, err := os.ReadFile(mapsPath)
		if err != nil {
			return "", fmt.Errorf("read %s: %w", mapsPath, err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "libssl") && strings.Contains(line, ".so") {
				fields := strings.Fields(line)
				if len(fields) >= 6 {
					return fields[len(fields)-1], nil
				}
			}
		}
		return "", fmt.Errorf("libssl.so not found in /proc/%d/maps", targetPID)
	}

	// Try common paths
	candidates := []string{
		"/usr/lib/aarch64-linux-gnu/libssl.so.3",
		"/usr/lib/x86_64-linux-gnu/libssl.so.3",
		"/usr/lib/aarch64-linux-gnu/libssl.so.1.1",
		"/usr/lib/x86_64-linux-gnu/libssl.so.1.1",
		"/usr/lib64/libssl.so.3",
		"/usr/lib64/libssl.so.1.1",
		"/lib/libssl.so.3",
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}

	// Try ldconfig
	data, err := os.ReadFile("/etc/ld.so.cache")
	_ = data
	_ = err
	// Fallback: search /proc for any postgres process
	entries, _ := os.ReadDir("/proc")
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		cmdline, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			continue
		}
		if !strings.Contains(string(cmdline), "postgres") {
			continue
		}
		mapsData, err := os.ReadFile(fmt.Sprintf("/proc/%d/maps", pid))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(mapsData), "\n") {
			if strings.Contains(line, "libssl") && strings.Contains(line, ".so") {
				fields := strings.Fields(line)
				if len(fields) >= 6 {
					return fields[len(fields)-1], nil
				}
			}
		}
	}

	return "", fmt.Errorf("cannot find libssl.so; specify libssl_path in config")
}
