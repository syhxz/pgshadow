// This file implements the TCP stream Reassembler (R2). It is deliberately
// tag-free and pure Go: it operates on already-captured gopacket.Packet values
// and uses github.com/google/gopacket/reassembly, which has no cgo/libpcap
// dependency. As a result it is always compiled by the default `go build ./...`
// and is unit-testable with synthesized packets (no live NIC required).
//
// Responsibilities (Requirement 2):
//   - Reassemble per-connection TCP byte streams keyed by the ConnID four-tuple
//     (R2.1), ordering payload bytes by TCP sequence number (R2.2) and spanning
//     multiple packets (R2.3).
//   - Release reassembly state for connections idle past a timeout via
//     FlushOlderThan (R2.4).
//   - Release reassembly state when a connection is observed to close on a
//     FIN/RST (R2.5).
//   - Deliver ordered bytes to a StreamHandler with a direction flag indicating
//     client->server (fromClient=true) vs server->client.
package capture

import (
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"

	"pgshadow/pkg/core"
)

// streamReassembler is the concrete Reassembler backed by gopacket/reassembly.
//
// gopacket/reassembly's Assembler is not safe for concurrent use: callers must
// serialize Assemble/FlushOlderThan/Close on a single goroutine. The pgshadow
// pipeline drives the reassembler from the single capture-consumer goroutine,
// so no internal locking is required and closedCount can be a plain int.
type streamReassembler struct {
	handler    StreamHandler
	serverPort uint16

	pool      *reassembly.StreamPool
	assembler *reassembly.Assembler

	// closedCount counts connections whose reassembly state has been released
	// (ReassemblyComplete). FlushOlderThan uses the delta to report how many
	// connections it released. It uses atomic access as a safety measure even
	// though the current pipeline guarantees single-goroutine driving.
	closedCount int64
}

// NewReassembler constructs a Reassembler that delivers ordered, per-direction
// byte streams to handler. The capture port (cfg.Port, defaulting to the
// PostgreSQL port) is used to determine packet direction robustly: a segment
// whose destination port is the server port is client->server (fromClient).
func NewReassembler(cfg Config, handler StreamHandler) Reassembler {
	if handler == nil {
		panic("capture.NewReassembler: handler must not be nil")
	}
	port := cfg.Port
	if port == 0 {
		port = DefaultPort
	}
	r := &streamReassembler{
		handler:    handler,
		serverPort: uint16(port),
	}
	r.pool = reassembly.NewStreamPool(&streamFactory{r: r})
	r.assembler = reassembly.NewAssembler(r.pool)
	return r
}

// Assemble processes one captured packet. It extracts the network and TCP
// layers, builds an AssemblerContext carrying the four-tuple and capture
// timestamp, and feeds the segment to the underlying assembler, which invokes
// the StreamHandler with ordered bytes (R2.1, R2.2, R2.3).
func (r *streamReassembler) Assemble(pkt gopacket.Packet) {
	if pkt == nil {
		return
	}
	netLayer := pkt.NetworkLayer()
	if netLayer == nil {
		return
	}
	tcpLayer := pkt.Layer(layers.LayerTypeTCP)
	if tcpLayer == nil {
		return
	}
	tcp, ok := tcpLayer.(*layers.TCP)
	if !ok {
		return
	}

	netFlow := netLayer.NetworkFlow()
	srcEP, dstEP := netFlow.Endpoints()
	srcIP, ok1 := netip.AddrFromSlice(srcEP.Raw())
	dstIP, ok2 := netip.AddrFromSlice(dstEP.Raw())
	if !ok1 || !ok2 {
		return
	}

	ci := pkt.Metadata().CaptureInfo
	if ci.Timestamp.IsZero() {
		ci.Timestamp = time.Now()
	}

	ctx := &captureContext{
		ci:      ci,
		srcIP:   srcIP.Unmap(),
		dstIP:   dstIP.Unmap(),
		srcPort: uint16(tcp.SrcPort),
		dstPort: uint16(tcp.DstPort),
	}
	r.assembler.AssembleWithContext(netFlow, tcp, ctx)
}

// FlushOlderThan releases reassembly state for connections idle longer than the
// given duration (R2.4). It returns the number of connections whose state was
// released by this call.
func (r *streamReassembler) FlushOlderThan(idle time.Duration) (flushed int) {
	before := atomic.LoadInt64(&r.closedCount)
	cutoff := time.Now().Add(-idle)
	r.assembler.FlushCloseOlderThan(cutoff)
	return int(atomic.LoadInt64(&r.closedCount) - before)
}

// Close flushes and releases all remaining reassembly state, delivering any
// buffered bytes and invoking OnClose for every still-open connection.
func (r *streamReassembler) Close() {
	r.assembler.FlushAll()
}

// captureContext implements reassembly.AssemblerContext while also carrying the
// parsed four-tuple of the packet that created the connection. The stream
// factory reads it to build the canonical (client->server oriented) ConnID and
// to determine the real packet direction relative to the configured port.
type captureContext struct {
	ci               gopacket.CaptureInfo
	srcIP, dstIP     netip.Addr
	srcPort, dstPort uint16
}

func (c *captureContext) GetCaptureInfo() gopacket.CaptureInfo { return c.ci }

// streamFactory creates one stream per TCP connection. gopacket/reassembly
// shares a single Stream across both half-connections of a connection, so the
// stream stores the canonical ConnID and the orientation of the first packet.
type streamFactory struct {
	r *streamReassembler
}

// New builds the canonical ConnID for the connection. The canonical ConnID is
// always client->server oriented (Src = client, Dst = server) so that both
// directions of one connection map to the same ConnID and are distinguished by
// the fromClient flag delivered to the StreamHandler (R2.1).
func (f *streamFactory) New(netFlow, tcpFlow gopacket.Flow, tcp *layers.TCP, ac reassembly.AssemblerContext) reassembly.Stream {
	cc, _ := ac.(*captureContext)
	serverPort := f.r.serverPort

	s := &stream{r: f.r, firstFromClient: true}
	if cc == nil {
		return s
	}

	// Determine whether the connection-creating packet was client->server.
	// Robust rule: the side whose port equals the configured server port is the
	// server. Prefer dst-port match; fall back to src-port match; otherwise
	// assume the first packet is client->server.
	switch {
	case cc.dstPort == serverPort:
		s.firstFromClient = true
	case cc.srcPort == serverPort:
		s.firstFromClient = false
	default:
		s.firstFromClient = true
	}

	if s.firstFromClient {
		s.conn = core.ConnID{SrcIP: cc.srcIP, SrcPort: cc.srcPort, DstIP: cc.dstIP, DstPort: cc.dstPort}
	} else {
		// First packet was server->client; flip to client->server orientation.
		s.conn = core.ConnID{SrcIP: cc.dstIP, SrcPort: cc.dstPort, DstIP: cc.srcIP, DstPort: cc.srcPort}
	}
	return s
}

// stream handles both half-connections of one TCP connection.
type stream struct {
	r    *streamReassembler
	conn core.ConnID
	// firstFromClient records whether the connection-creating packet (which
	// defines the assembler's TCPDirClientToServer orientation) was a real
	// client->server segment.
	firstFromClient bool
}

// Accept admits every segment. We force *start = true so that connections
// captured mid-stream (no observed SYN, e.g. a mirror that began after the
// handshake) still establish a sequence baseline and deliver data rather than
// waiting indefinitely for a SYN.
func (s *stream) Accept(tcp *layers.TCP, ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, nextSeq reassembly.Sequence, start *bool, ac reassembly.AssemblerContext) bool {
	*start = true
	return true
}

// ReassembledSG delivers contiguous, in-order bytes for one direction. The
// real client->server direction is derived from the chunk direction relative to
// the connection's first-packet orientation (R2.2).
func (s *stream) ReassembledSG(sg reassembly.ScatterGather, ac reassembly.AssemblerContext) {
	avail, _ := sg.Lengths()
	if avail == 0 {
		return
	}
	dir, _, _, _ := sg.Info()
	fromClient := s.firstFromClient
	if dir == reassembly.TCPDirServerToClient {
		fromClient = !s.firstFromClient
	}

	// sg bytes are reused after this call returns, so copy before delivering.
	src := sg.Fetch(avail)
	data := make([]byte, len(src))
	copy(data, src)

	s.r.handler.OnBytes(s.conn, fromClient, data)
}

// ReassemblyComplete is called when the connection closes (FIN/RST observed) or
// is flushed as idle. It releases the reassembly state for the ConnID (R2.5)
// and reports OnClose to the handler. Returning true removes the connection
// from the pool.
func (s *stream) ReassemblyComplete(ac reassembly.AssemblerContext) bool {
	atomic.AddInt64(&s.r.closedCount, 1)
	s.r.handler.OnClose(s.conn)
	return true
}
