package capture

import (
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"

	"pgshadow/pkg/core"
)

// These tests are tag-free and require no live NIC: they synthesize TCP
// segments with the gopacket layers package and drive them through the
// reassembler, asserting in-order delivery, reordering, multi-segment spanning,
// idle flush (R2.4), and FIN/RST close (R2.5).

const (
	testServerPort uint16 = 5432
	testClientPort uint16 = 50000
)

var (
	testClientIP = net.IPv4(10, 0, 0, 1)
	testServerIP = net.IPv4(10, 0, 0, 2)
)

// recordingHandler captures OnBytes/OnClose callbacks for assertions. It is
// safe for concurrent use although the reassembler drives it from one goroutine.
type recordingHandler struct {
	mu       sync.Mutex
	clientTo map[string][]byte // ConnID.Key -> concatenated client->server bytes
	serverTo map[string][]byte // ConnID.Key -> concatenated server->client bytes
	closed   map[string]int    // ConnID.Key -> OnClose count
	order    []record
}

type record struct {
	conn       core.ConnID
	fromClient bool
	data       []byte
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		clientTo: map[string][]byte{},
		serverTo: map[string][]byte{},
		closed:   map[string]int{},
	}
}

func (h *recordingHandler) OnBytes(conn core.ConnID, fromClient bool, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if fromClient {
		h.clientTo[conn.Key()] = append(h.clientTo[conn.Key()], data...)
	} else {
		h.serverTo[conn.Key()] = append(h.serverTo[conn.Key()], data...)
	}
	h.order = append(h.order, record{conn: conn, fromClient: fromClient, data: append([]byte(nil), data...)})
}

func (h *recordingHandler) OnClose(conn core.ConnID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed[conn.Key()]++
}

// segment describes one synthesized TCP segment.
type segment struct {
	fromClient bool
	seq        uint32
	ack        uint32
	syn        bool
	fin        bool
	rst        bool
	payload    string
	ts         time.Time
}

// buildPacket serializes one TCP segment into a gopacket.Packet. The capture
// timestamp is set so idle-flush tests can backdate packets.
func buildPacket(t *testing.T, seg segment) gopacket.Packet {
	t.Helper()

	var srcIP, dstIP net.IP
	var srcPort, dstPort uint16
	if seg.fromClient {
		srcIP, dstIP = testClientIP, testServerIP
		srcPort, dstPort = testClientPort, testServerPort
	} else {
		srcIP, dstIP = testServerIP, testClientIP
		srcPort, dstPort = testServerPort, testClientPort
	}

	eth := &layers.Ethernet{
		SrcMAC:       net.HardwareAddr{0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
		DstMAC:       net.HardwareAddr{0x00, 0x00, 0x00, 0x00, 0x00, 0x02},
		EthernetType: layers.EthernetTypeIPv4,
	}
	ip := &layers.IPv4{
		Version:  4,
		IHL:      5,
		TTL:      64,
		Protocol: layers.IPProtocolTCP,
		SrcIP:    srcIP,
		DstIP:    dstIP,
	}
	tcp := &layers.TCP{
		SrcPort: layers.TCPPort(srcPort),
		DstPort: layers.TCPPort(dstPort),
		Seq:     seg.seq,
		Ack:     seg.ack,
		SYN:     seg.syn,
		FIN:     seg.fin,
		RST:     seg.rst,
		ACK:     !seg.syn || seg.ack != 0,
		Window:  65535,
	}
	if err := tcp.SetNetworkLayerForChecksum(ip); err != nil {
		t.Fatalf("SetNetworkLayerForChecksum: %v", err)
	}

	buf := gopacket.NewSerializeBuffer()
	opts := gopacket.SerializeOptions{FixLengths: true, ComputeChecksums: true}
	layersToSerialize := []gopacket.SerializableLayer{eth, ip, tcp}
	if seg.payload != "" {
		layersToSerialize = append(layersToSerialize, gopacket.Payload([]byte(seg.payload)))
	}
	if err := gopacket.SerializeLayers(buf, opts, layersToSerialize...); err != nil {
		t.Fatalf("SerializeLayers: %v", err)
	}

	pkt := gopacket.NewPacket(buf.Bytes(), layers.LayerTypeEthernet, gopacket.Default)
	ts := seg.ts
	if ts.IsZero() {
		ts = time.Now()
	}
	m := pkt.Metadata()
	m.Timestamp = ts
	m.CaptureInfo.Timestamp = ts
	m.CaptureLength = len(buf.Bytes())
	m.Length = len(buf.Bytes())
	return pkt
}

// canonicalConn is the client->server oriented ConnID the reassembler reports.
func canonicalConn() core.ConnID {
	cip, _ := netip.AddrFromSlice(testClientIP.To4())
	sip, _ := netip.AddrFromSlice(testServerIP.To4())
	return core.ConnID{SrcIP: cip, SrcPort: testClientPort, DstIP: sip, DstPort: testServerPort}
}

func TestReassembler_InOrderDelivery(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	segs := []segment{
		{fromClient: true, seq: 1000, syn: true, ts: base},                                // handshake
		{fromClient: true, seq: 1001, payload: "SELECT ", ts: base.Add(time.Millisecond)}, // seg 1
		{fromClient: true, seq: 1008, payload: "1;", ts: base.Add(2 * time.Millisecond)},  // seg 2
	}
	for _, s := range segs {
		r.Assemble(buildPacket(t, s))
	}
	r.Close()

	key := canonicalConn().Key()
	if got := string(h.clientTo[key]); got != "SELECT 1;" {
		t.Errorf("client->server reassembled = %q, want %q", got, "SELECT 1;")
	}
}

func TestReassembler_OutOfOrderReordering(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	// SYN first to fix the sequence baseline, then deliver the *later* segment
	// before the earlier one.
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: base}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1009, payload: "ROM t;", ts: base.Add(2 * time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, payload: "SELECT F", ts: base.Add(time.Millisecond)}))
	r.Close()

	key := canonicalConn().Key()
	if got := string(h.clientTo[key]); got != "SELECT FROM t;" {
		t.Errorf("out-of-order reassembled = %q, want %q (R2.2)", got, "SELECT FROM t;")
	}
}

func TestReassembler_MultiSegmentSpanningPackets(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	full := "INSERT INTO orders (id, amount) VALUES (42, 99.95);"
	base := time.Now()
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 5000, syn: true, ts: base}))

	// Split the statement across three contiguous segments.
	chunks := []string{full[:10], full[10:30], full[30:]}
	seq := uint32(5001)
	for i, c := range chunks {
		r.Assemble(buildPacket(t, segment{
			fromClient: true,
			seq:        seq,
			payload:    c,
			ts:         base.Add(time.Duration(i+1) * time.Millisecond),
		}))
		seq += uint32(len(c))
	}
	r.Close()

	key := canonicalConn().Key()
	if got := string(h.clientTo[key]); got != full {
		t.Errorf("multi-segment reassembled = %q, want %q (R2.3)", got, full)
	}
}

func TestReassembler_DirectionFlag(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	// Client SYN establishes orientation, then traffic in both directions.
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 100, syn: true, ts: base}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 200, syn: true, ack: 101, ts: base.Add(time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 101, payload: "SELECT 1;", ts: base.Add(2 * time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 201, payload: "READY", ts: base.Add(3 * time.Millisecond)}))
	r.Close()

	key := canonicalConn().Key()
	if got := string(h.clientTo[key]); got != "SELECT 1;" {
		t.Errorf("client->server bytes = %q, want %q", got, "SELECT 1;")
	}
	if got := string(h.serverTo[key]); got != "READY" {
		t.Errorf("server->client bytes = %q, want %q", got, "READY")
	}
}

func TestReassembler_DirectionFlag_ServerFirstPacket(t *testing.T) {
	// If the first packet observed is server->client (mirror started after the
	// handshake, server speaks first), direction must still be derived from the
	// configured server port, not from arrival order.
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 900, payload: "HELLO", ts: base}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 50, payload: "Q1", ts: base.Add(time.Millisecond)}))
	r.Close()

	key := canonicalConn().Key()
	if got := string(h.serverTo[key]); got != "HELLO" {
		t.Errorf("server->client bytes = %q, want %q (direction by port)", got, "HELLO")
	}
	if got := string(h.clientTo[key]); got != "Q1" {
		t.Errorf("client->server bytes = %q, want %q (direction by port)", got, "Q1")
	}
}

func TestReassembler_IdleFlushReleasesState(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	// Backdate the packets 10 minutes so they are older than the idle cutoff.
	old := time.Now().Add(-10 * time.Minute)
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: old}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, payload: "SELECT 1;", ts: old}))

	key := canonicalConn().Key()
	if h.closed[key] != 0 {
		t.Fatalf("connection closed before flush, closed=%d", h.closed[key])
	}

	flushed := r.FlushOlderThan(5 * time.Minute)
	if flushed < 1 {
		t.Errorf("FlushOlderThan released %d connections, want >= 1 (R2.4)", flushed)
	}
	if h.closed[key] == 0 {
		t.Errorf("OnClose not invoked on idle flush (R2.4)")
	}
}

func TestReassembler_IdleFlushSkipsActiveConnections(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	// Recent packet: must NOT be flushed by an idle cutoff in the past.
	now := time.Now()
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: now}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, payload: "SELECT 1;", ts: now}))

	flushed := r.FlushOlderThan(5 * time.Minute)
	if flushed != 0 {
		t.Errorf("FlushOlderThan released %d recent connections, want 0", flushed)
	}
	key := canonicalConn().Key()
	if h.closed[key] != 0 {
		t.Errorf("active connection state released prematurely (R2.4)")
	}
}

func TestReassembler_FINClosesConnection(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: base}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 2000, syn: true, ack: 1001, ts: base.Add(time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, payload: "SELECT 1;", ts: base.Add(2 * time.Millisecond)}))
	// Both halves send FIN -> connection fully closed -> state released (R2.5).
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1010, fin: true, ts: base.Add(3 * time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 2001, fin: true, ts: base.Add(4 * time.Millisecond)}))

	key := canonicalConn().Key()
	if h.closed[key] == 0 {
		t.Errorf("OnClose not invoked on observed FIN (R2.5)")
	}
}

func TestReassembler_RSTClosesConnection(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: base}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 2000, syn: true, ack: 1001, ts: base.Add(time.Millisecond)}))
	// RST on both halves tears the connection down.
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, rst: true, ts: base.Add(2 * time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 2001, rst: true, ts: base.Add(3 * time.Millisecond)}))

	key := canonicalConn().Key()
	if h.closed[key] == 0 {
		t.Errorf("OnClose not invoked on observed RST (R2.5)")
	}
}
