package capture

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"

	"pgshadow/pkg/core"
)

// These tests complement reassembler_test.go. They focus specifically on
// reassembly *state release* (task 8.5): that OnClose fires exactly once per
// connection, that released state is truly gone (a later flush reports nothing
// to release), that idle flush selects connections by age, and that distinct
// ConnIDs are released independently of one another (R2.4, R2.5, R1.7).
//
// They reuse the helpers declared in reassembler_test.go (segment,
// newRecordingHandler, canonicalConn, testServerPort) and add a small
// parameterized packet builder so that several distinct connections can be
// synthesized within one test.

// buildPacketForClient serializes one TCP segment for a connection identified by
// the given client IP/port (the server side is always testServerIP/
// testServerPort). It mirrors buildPacket but allows multiple distinct ConnIDs
// in a single test without redeclaring the single-connection helper.
func buildPacketForClient(t *testing.T, clientIP net.IP, clientPort uint16, seg segment) gopacket.Packet {
	t.Helper()

	var srcIP, dstIP net.IP
	var srcPort, dstPort uint16
	if seg.fromClient {
		srcIP, dstIP = clientIP, testServerIP
		srcPort, dstPort = clientPort, testServerPort
	} else {
		srcIP, dstIP = testServerIP, clientIP
		srcPort, dstPort = testServerPort, clientPort
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

// canonicalConnForClient returns the client->server oriented ConnID the
// reassembler reports for the given client IP/port.
func canonicalConnForClient(clientIP net.IP, clientPort uint16) core.ConnID {
	cip, _ := netip.AddrFromSlice(clientIP.To4())
	sip, _ := netip.AddrFromSlice(testServerIP.To4())
	return core.ConnID{SrcIP: cip, SrcPort: clientPort, DstIP: sip, DstPort: testServerPort}
}

// TestReassembler_OnCloseCalledOncePerConnection verifies that a connection
// torn down by an observed FIN handshake invokes OnClose exactly once, and that
// a subsequent Close()/FlushAll does not re-close it — i.e. the state was truly
// released, not merely flushed (R2.5).
func TestReassembler_OnCloseCalledOncePerConnection(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	base := time.Now()
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: base}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 2000, syn: true, ack: 1001, ts: base.Add(time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, payload: "SELECT 1;", ts: base.Add(2 * time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1010, fin: true, ts: base.Add(3 * time.Millisecond)}))
	r.Assemble(buildPacket(t, segment{fromClient: false, seq: 2001, fin: true, ts: base.Add(4 * time.Millisecond)}))

	key := canonicalConn().Key()
	if got := h.closed[key]; got != 1 {
		t.Fatalf("OnClose called %d times after FIN handshake, want exactly 1 (R2.5)", got)
	}

	// Releasing already-released state must not produce a second OnClose.
	r.Close()
	if got := h.closed[key]; got != 1 {
		t.Errorf("OnClose called %d times after Close(); state was not truly released (R2.5)", got)
	}
}

// TestReassembler_IdleFlushSecondPassReportsZero verifies that once idle state
// is released by FlushOlderThan, a second flush over the same cutoff reports 0
// connections released and does not invoke OnClose again — the state is gone
// (R2.4).
func TestReassembler_IdleFlushSecondPassReportsZero(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	old := time.Now().Add(-10 * time.Minute)
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: old}))
	r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1001, payload: "SELECT 1;", ts: old}))

	first := r.FlushOlderThan(5 * time.Minute)
	if first < 1 {
		t.Fatalf("first FlushOlderThan released %d, want >= 1 (R2.4)", first)
	}
	key := canonicalConn().Key()
	closedAfterFirst := h.closed[key]
	if closedAfterFirst == 0 {
		t.Fatalf("OnClose not invoked on first idle flush (R2.4)")
	}

	second := r.FlushOlderThan(5 * time.Minute)
	if second != 0 {
		t.Errorf("second FlushOlderThan released %d, want 0 (state already released) (R2.4)", second)
	}
	if h.closed[key] != closedAfterFirst {
		t.Errorf("OnClose count changed on second flush: got %d, want %d (state not released)", h.closed[key], closedAfterFirst)
	}
}

// TestReassembler_IdleFlushSelectsByAge verifies that FlushOlderThan releases
// only the connections idle past the cutoff and leaves more-recent connections
// intact. Two distinct connections are synthesized: one backdated well past the
// cutoff, one recent (R2.4).
func TestReassembler_IdleFlushSelectsByAge(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	oldClientPort := uint16(50001)
	recentClientPort := uint16(50002)

	old := time.Now().Add(-10 * time.Minute)
	now := time.Now()

	// Old connection.
	r.Assemble(buildPacketForClient(t, testClientIP, oldClientPort, segment{fromClient: true, seq: 1000, syn: true, ts: old}))
	r.Assemble(buildPacketForClient(t, testClientIP, oldClientPort, segment{fromClient: true, seq: 1001, payload: "SELECT old;", ts: old}))
	// Recent connection.
	r.Assemble(buildPacketForClient(t, testClientIP, recentClientPort, segment{fromClient: true, seq: 3000, syn: true, ts: now}))
	r.Assemble(buildPacketForClient(t, testClientIP, recentClientPort, segment{fromClient: true, seq: 3001, payload: "SELECT recent;", ts: now}))

	flushed := r.FlushOlderThan(5 * time.Minute)
	if flushed != 1 {
		t.Fatalf("FlushOlderThan released %d connections, want exactly 1 (only the aged one) (R2.4)", flushed)
	}

	oldKey := canonicalConnForClient(testClientIP, oldClientPort).Key()
	recentKey := canonicalConnForClient(testClientIP, recentClientPort).Key()
	if h.closed[oldKey] == 0 {
		t.Errorf("aged connection was not released by idle flush (R2.4)")
	}
	if h.closed[recentKey] != 0 {
		t.Errorf("recent connection was released prematurely (R2.4)")
	}
}

// TestReassembler_DistinctConnIDsReleasedIndependently verifies that closing one
// connection (FIN) releases only that connection's state, leaving a second,
// concurrently-tracked connection open until it is itself flushed (R2.1, R2.5).
func TestReassembler_DistinctConnIDsReleasedIndependently(t *testing.T) {
	h := newRecordingHandler()
	r := NewReassembler(Config{Port: int(testServerPort)}, h)

	portA := uint16(50010)
	portB := uint16(50011)
	base := time.Now()

	// Establish both connections.
	r.Assemble(buildPacketForClient(t, testClientIP, portA, segment{fromClient: true, seq: 100, syn: true, ts: base}))
	r.Assemble(buildPacketForClient(t, testClientIP, portA, segment{fromClient: false, seq: 900, syn: true, ack: 101, ts: base.Add(time.Millisecond)}))
	r.Assemble(buildPacketForClient(t, testClientIP, portB, segment{fromClient: true, seq: 200, syn: true, ts: base.Add(2 * time.Millisecond)}))
	r.Assemble(buildPacketForClient(t, testClientIP, portB, segment{fromClient: false, seq: 800, syn: true, ack: 201, ts: base.Add(3 * time.Millisecond)}))

	keyA := canonicalConnForClient(testClientIP, portA).Key()
	keyB := canonicalConnForClient(testClientIP, portB).Key()

	// Close connection A via a FIN handshake.
	r.Assemble(buildPacketForClient(t, testClientIP, portA, segment{fromClient: true, seq: 101, fin: true, ts: base.Add(4 * time.Millisecond)}))
	r.Assemble(buildPacketForClient(t, testClientIP, portA, segment{fromClient: false, seq: 901, fin: true, ts: base.Add(5 * time.Millisecond)}))

	if h.closed[keyA] == 0 {
		t.Errorf("connection A not released on FIN (R2.5)")
	}
	if h.closed[keyB] != 0 {
		t.Errorf("connection B released by connection A's close; state not independent (R2.1, R2.5)")
	}

	// B is still tracked: closing the whole reassembler must now release it.
	r.Close()
	if h.closed[keyB] == 0 {
		t.Errorf("connection B not released by Close(); state was lost prematurely (R2.5)")
	}
	if h.closed[keyA] != 1 {
		t.Errorf("connection A released %d times; expected exactly 1 (no double release on Close) (R2.5)", h.closed[keyA])
	}
}
