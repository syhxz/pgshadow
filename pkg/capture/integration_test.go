package capture

// Task 14.1 — End-to-end integration tests wiring multiple pipeline stages
// together: capture → TCP reassembly → protocol parser → transaction-state
// machine → SQL filter. These tests are tag-free and self-contained: they run
// under the default `go test ./...` with NO libpcap, NO live NIC, and NO
// database.
//
// Two complementary fixtures are exercised:
//
//  1. pcap path (TestIntegration_PcapFile...): a client PostgreSQL session is
//     synthesized as Ethernet/IPv4/TCP packets (reusing the buildPacket helper
//     from reassembler_test.go), written to a REAL .pcap file via the pure-Go
//     gopacket/pcapgo writer (no libpcap), read back with the pcapgo reader,
//     and replayed through NewReassembler → a StreamHandler that frames PG
//     messages and extracts SQL. This exercises the genuine .pcap file path
//     end to end without any cgo/libpcap dependency, asserting that a Simple
//     Query split across multiple TCP segments reassembles and the SQL is
//     extracted correctly (R2, R3).
//
//  2. pgproto3/byte fixtures (TestIntegration_ByteFixtures...): hand-built
//     Q/P/B/Z wire-byte sequences are driven through protocol.NewParser +
//     EventBuilder + filter.StateMachine + filter.Filter, asserting extracted
//     SQL, the Parse statement name / parameter OIDs, and that a bidirectional
//     backend ReadyForQuery ('Z') updates per-connection transaction state and
//     thereby flips the Plain_SELECT keep/drop decision (R3, R4.1, R5).
//
// Drop-count surfacing (R1.7): live kernel drop counters require the libpcap
// Source (build tag `pcap`) and a real NIC, so they cannot be exercised in the
// default environment. Instead TestIntegration_DropCountSurfacing asserts the
// Source.Stats() contract that the capture layer surfaces received/dropped
// counts to the metrics layer.

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcapgo"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/protocol"
)

// protoVersion30 is the PostgreSQL 3.0 startup protocol version code, written
// as the first 4 bytes of an untyped StartupMessage payload.
const protoVersion30 uint32 = 196608

// wireTyped builds a wire-format typed PG message: 1 type byte + Int32 length
// (inclusive of the 4 length bytes) + payload.
func wireTyped(typ byte, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = typ
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(payload)))
	copy(out[5:], payload)
	return out
}

// wireStartup builds a wire-format untyped startup-phase message: Int32 length
// (inclusive of the 4 length bytes) + payload, with code as the first 4 payload
// bytes (protocol version or request code).
func wireStartup(code uint32, rest []byte) []byte {
	payload := make([]byte, 4+len(rest))
	binary.BigEndian.PutUint32(payload[0:4], code)
	copy(payload[4:], rest)

	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(4+len(payload)))
	copy(out[4:], payload)
	return out
}

// wireParse builds a Parse ('P') message payload: NUL-terminated statement
// name, NUL-terminated query, Int16 OID count, then Int32 OIDs.
func wireParsePayload(stmtName, query string, oids []uint32) []byte {
	var b []byte
	b = append(b, []byte(stmtName)...)
	b = append(b, 0)
	b = append(b, []byte(query)...)
	b = append(b, 0)
	count := make([]byte, 2)
	binary.BigEndian.PutUint16(count, uint16(len(oids)))
	b = append(b, count...)
	for _, oid := range oids {
		o := make([]byte, 4)
		binary.BigEndian.PutUint32(o, oid)
		b = append(b, o...)
	}
	return b
}

// splitN splits b into n roughly-equal, contiguous chunks (the last chunk
// absorbs any remainder). It is used to spread a PG wire byte stream across
// several TCP segments so reassembly across packets (R2.3) is exercised.
func splitN(b []byte, n int) [][]byte {
	if n <= 1 || len(b) <= 1 {
		return [][]byte{b}
	}
	if n > len(b) {
		n = len(b)
	}
	size := len(b) / n
	chunks := make([][]byte, 0, n)
	for i := 0; i < n-1; i++ {
		chunks = append(chunks, b[i*size:(i+1)*size])
	}
	chunks = append(chunks, b[(n-1)*size:])
	return chunks
}

// pipelineHandler is a StreamHandler that wires reassembled bytes through the
// protocol parser (per ConnID + direction), the EventBuilder, the transaction
// state machine, and the SQL filter. It records every extracted event and the
// keep/drop decision so tests can assert end-to-end behavior.
type pipelineHandler struct {
	cfg     protocol.Config
	builder *protocol.EventBuilder
	sm      filter.StateMachine
	flt     filter.Filter
	cls     filter.Classifier

	clientParsers map[string]protocol.Parser
	serverParsers map[string]protocol.Parser

	events []*core.SQLEvent // every SQL_Event extracted, in delivery order
	kept   []*core.SQLEvent // events the filter chose to keep
	closed map[string]int   // ConnID.Key -> OnClose count
}

func newPipelineHandler(t *testing.T) *pipelineHandler {
	t.Helper()
	cfg := protocol.Config{MaxSQLLength: 1 << 20, ExtendedQuery: true}
	flt, err := filter.NewFilter(filter.Config{}) // default rule matrix
	if err != nil {
		t.Fatalf("filter.NewFilter: %v", err)
	}
	return &pipelineHandler{
		cfg:           cfg,
		builder:       protocol.NewEventBuilder(cfg),
		sm:            filter.NewStateMachine(),
		flt:           flt,
		cls:           filter.NewClassifier(nil),
		clientParsers: map[string]protocol.Parser{},
		serverParsers: map[string]protocol.Parser{},
		closed:        map[string]int{},
	}
}

func (h *pipelineHandler) OnBytes(conn core.ConnID, fromClient bool, data []byte) {
	key := conn.Key()
	if fromClient {
		p := h.clientParsers[key]
		if p == nil {
			p = protocol.NewParser(h.cfg, true)
			h.clientParsers[key] = p
		}
		msgs, err := p.Feed(data)
		if err != nil {
			return
		}
		for _, m := range msgs {
			ev, ok := h.builder.Build(conn, m, time.Now())
			if !ok {
				continue
			}
			state := h.sm.State(conn)
			action, _ := h.flt.Decide(ev, state)
			h.events = append(h.events, ev)
			if action == filter.Keep {
				h.kept = append(h.kept, ev)
			}
			// Unidirectional inference: advance state on BEGIN/COMMIT/ROLLBACK.
			h.sm.OnStatement(conn, h.cls.Classify(ev.SQL))
		}
		return
	}

	// Server→client direction: route ReadyForQuery ('Z') into the state machine
	// (bidirectional transaction-state, R4.1).
	p := h.serverParsers[key]
	if p == nil {
		p = protocol.NewParser(h.cfg, false)
		h.serverParsers[key] = p
	}
	msgs, err := p.Feed(data)
	if err != nil {
		return
	}
	for _, m := range msgs {
		if m.Type == 'Z' && len(m.Payload) > 0 {
			h.sm.OnReadyForQuery(conn, m.Payload[0])
		}
	}
}

func (h *pipelineHandler) OnClose(conn core.ConnID) {
	h.closed[conn.Key()]++
	h.sm.Forget(conn)
}

// writePcap writes packets to a real libpcap-format .pcap file using the pure-Go
// gopacket/pcapgo writer (no libpcap/cgo dependency).
func writePcap(t *testing.T, path string, pkts []gopacket.Packet) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create pcap: %v", err)
	}
	defer f.Close()

	w := pcapgo.NewWriter(f)
	if err := w.WriteFileHeader(uint32(DefaultSnapLen), layers.LinkTypeEthernet); err != nil {
		t.Fatalf("WriteFileHeader: %v", err)
	}
	for _, p := range pkts {
		data := p.Data()
		ci := p.Metadata().CaptureInfo
		if ci.CaptureLength == 0 {
			ci.CaptureLength = len(data)
		}
		if ci.Length == 0 {
			ci.Length = len(data)
		}
		if err := w.WritePacket(ci, data); err != nil {
			t.Fatalf("WritePacket: %v", err)
		}
	}
}

// readPcap reads a .pcap file back with the pure-Go pcapgo reader and rebuilds
// gopacket.Packet values suitable for the reassembler.
func readPcap(t *testing.T, path string) []gopacket.Packet {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open pcap: %v", err)
	}
	defer f.Close()

	r, err := pcapgo.NewReader(f)
	if err != nil {
		t.Fatalf("pcapgo.NewReader: %v", err)
	}

	var out []gopacket.Packet
	for {
		data, ci, err := r.ReadPacketData()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadPacketData: %v", err)
		}
		pkt := gopacket.NewPacket(data, layers.LayerTypeEthernet, gopacket.Default)
		m := pkt.Metadata()
		m.CaptureInfo = ci
		m.Timestamp = ci.Timestamp
		out = append(out, pkt)
	}
	return out
}

// TestIntegration_PcapFileThroughCaptureReassemblyParser replays a synthesized
// client session — written to and read back from a real .pcap file — through
// capture reassembly into the protocol parser, and asserts the SQL extracted
// from a Simple Query split across multiple TCP segments (R2.3, R3.1, R3.2).
func TestIntegration_PcapFileThroughCaptureReassemblyParser(t *testing.T) {
	const sql = "SELECT id, name FROM users WHERE id = 42 AND status = 'active';"

	// Build the client-side PG wire byte stream: StartupMessage then a Simple
	// Query carrying the SQL above (NUL-terminated).
	var clientStream []byte
	clientStream = append(clientStream, wireStartup(protoVersion30, []byte("user\x00postgres\x00database\x00app\x00\x00"))...)
	clientStream = append(clientStream, wireTyped('Q', append([]byte(sql), 0))...)

	// A backend ReadyForQuery(Idle) closes the request/response round-trip.
	serverStream := wireTyped('Z', []byte{'I'})

	base := time.Now()
	var pkts []gopacket.Packet
	// Handshake establishes per-direction sequence baselines.
	pkts = append(pkts, buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: base}))
	pkts = append(pkts, buildPacket(t, segment{fromClient: false, seq: 5000, syn: true, ack: 1001, ts: base.Add(time.Millisecond)}))

	// Spread the client stream across three contiguous TCP segments.
	seq := uint32(1001)
	for i, c := range splitN(clientStream, 3) {
		pkts = append(pkts, buildPacket(t, segment{
			fromClient: true,
			seq:        seq,
			payload:    string(c),
			ts:         base.Add(time.Duration(2+i) * time.Millisecond),
		}))
		seq += uint32(len(c))
	}
	// Backend response.
	pkts = append(pkts, buildPacket(t, segment{
		fromClient: false,
		seq:        5001,
		payload:    string(serverStream),
		ts:         base.Add(10 * time.Millisecond),
	}))

	// Round-trip through a real .pcap file (pure Go, no libpcap).
	path := filepath.Join(t.TempDir(), "session.pcap")
	writePcap(t, path, pkts)
	replayed := readPcap(t, path)
	if len(replayed) != len(pkts) {
		t.Fatalf("read back %d packets, wrote %d", len(replayed), len(pkts))
	}

	// Drive capture reassembly → parser → filter.
	h := newPipelineHandler(t)
	r := NewReassembler(Config{Port: int(testServerPort)}, h)
	for _, p := range replayed {
		r.Assemble(p)
	}
	r.Close()

	if len(h.events) != 1 {
		t.Fatalf("extracted %d SQL events, want 1: %+v", len(h.events), h.events)
	}
	if got := h.events[0].SQL; got != sql {
		t.Errorf("extracted SQL = %q, want %q (R2.3, R3.2)", got, sql)
	}
	if h.events[0].Conn != canonicalConn() {
		t.Errorf("event ConnID = %v, want %v (R2.1)", h.events[0].Conn, canonicalConn())
	}
}

// TestIntegration_ByteFixtures_BidirectionalZ drives hand-built Q/P/Z wire
// fixtures through the parser + EventBuilder + StateMachine + Filter, asserting
// extracted SQL, Parse statement/params, and that a bidirectional backend 'Z'
// flips a Plain_SELECT keep/drop decision via per-connection transaction state
// (R3.2, R3.3, R4.1, R5.6, R5.7).
func TestIntegration_ByteFixtures_BidirectionalZ(t *testing.T) {
	cfg := protocol.Config{MaxSQLLength: 1 << 20, ExtendedQuery: true}
	conn := canonicalConn()

	clientP := protocol.NewParser(cfg, true)
	serverP := protocol.NewParser(cfg, false)
	builder := protocol.NewEventBuilder(cfg)
	sm := filter.NewStateMachine()
	cls := filter.NewClassifier(nil)
	flt, err := filter.NewFilter(filter.Config{}) // default matrix: SELECT keep in-tx, drop out-of-tx
	if err != nil {
		t.Fatalf("filter.NewFilter: %v", err)
	}

	type decision struct {
		ev     *core.SQLEvent
		action filter.Action
		class  filter.StmtClass
		state  core.TxStatus
	}
	feedClient := func(wire []byte) []decision {
		msgs, err := clientP.Feed(wire)
		if err != nil {
			t.Fatalf("client Feed: %v", err)
		}
		var ds []decision
		for _, m := range msgs {
			ev, ok := builder.Build(conn, m, time.Now())
			if !ok {
				continue
			}
			state := sm.State(conn)
			action, class := flt.Decide(ev, state)
			sm.OnStatement(conn, cls.Classify(ev.SQL))
			ds = append(ds, decision{ev: ev, action: action, class: class, state: state})
		}
		return ds
	}
	feedServerZ := func(status byte) {
		msgs, err := serverP.Feed(wireTyped('Z', []byte{status}))
		if err != nil {
			t.Fatalf("server Feed: %v", err)
		}
		for _, m := range msgs {
			if m.Type == 'Z' && len(m.Payload) > 0 {
				sm.OnReadyForQuery(conn, m.Payload[0])
			}
		}
	}

	// Frontend opens with the untyped StartupMessage (no SQL event).
	if ds := feedClient(wireStartup(protoVersion30, []byte("user\x00postgres\x00\x00"))); len(ds) != 0 {
		t.Fatalf("startup produced %d events, want 0", len(ds))
	}

	// 1) Plain SELECT while Idle (default state) → dropped out-of-transaction (R5.6).
	ds := feedClient(wireTyped('Q', []byte("SELECT 1\x00")))
	if len(ds) != 1 {
		t.Fatalf("expected 1 event for first query, got %d", len(ds))
	}
	if ds[0].ev.SQL != "SELECT 1" {
		t.Errorf("SQL = %q, want %q (R3.2)", ds[0].ev.SQL, "SELECT 1")
	}
	if ds[0].state != core.TxIdle {
		t.Errorf("state before first query = %v, want Idle", ds[0].state)
	}
	if ds[0].class != filter.ClassPlainSelect || ds[0].action != filter.Drop {
		t.Errorf("Idle Plain_SELECT = (%v, action=%v), want (PlainSelect, Drop) (R5.6)", ds[0].class, ds[0].action)
	}

	// 2) Backend ReadyForQuery 'T' moves the connection In-transaction (R4.1).
	feedServerZ('T')
	if got := sm.State(conn); got != core.TxInTx {
		t.Fatalf("after Z='T' state = %v, want InTx (R4.1)", got)
	}

	// 3) The SAME Plain SELECT is now kept in-transaction (R5.7) — proving the
	// bidirectional 'Z' changed the filter outcome.
	ds = feedClient(wireTyped('Q', []byte("SELECT 1\x00")))
	if len(ds) != 1 || ds[0].action != filter.Keep {
		t.Fatalf("in-tx Plain_SELECT not kept: %+v (R5.7)", ds)
	}

	// 4) Backend ReadyForQuery 'E' (failed transaction) maps to Failed, which
	// uses the out-of-transaction action → Plain SELECT dropped again.
	feedServerZ('E')
	if got := sm.State(conn); got != core.TxFailed {
		t.Fatalf("after Z='E' state = %v, want Failed (R4.1)", got)
	}
	ds = feedClient(wireTyped('Q', []byte("SELECT 1\x00")))
	if len(ds) != 1 || ds[0].action != filter.Drop {
		t.Fatalf("failed-tx Plain_SELECT should drop: %+v", ds)
	}

	// 5) Back to Idle.
	feedServerZ('I')
	if got := sm.State(conn); got != core.TxIdle {
		t.Fatalf("after Z='I' state = %v, want Idle (R4.1)", got)
	}

	// 6) Extended Query Parse: assert statement name, SQL, and parameter OIDs
	// are extracted (R3.3). A parameterized DML is kept regardless of state.
	parse := wireTyped('P', wireParsePayload("ins_stmt", "INSERT INTO t(id, v) VALUES ($1, $2)", []uint32{23, 25}))
	ds = feedClient(parse)
	if len(ds) != 1 {
		t.Fatalf("expected 1 event for Parse, got %d", len(ds))
	}
	pev := ds[0].ev
	if !pev.Extended {
		t.Errorf("Parse event Extended = false, want true (R3.3)")
	}
	if pev.StmtName != "ins_stmt" {
		t.Errorf("StmtName = %q, want %q (R3.3)", pev.StmtName, "ins_stmt")
	}
	if pev.SQL != "INSERT INTO t(id, v) VALUES ($1, $2)" {
		t.Errorf("Parse SQL = %q (R3.3)", pev.SQL)
	}
	if len(pev.Params) != 2 || pev.Params[0].OID != 23 || pev.Params[1].OID != 25 {
		t.Errorf("Parse params = %+v, want OIDs [23 25] (R3.3)", pev.Params)
	}
	if ds[0].class != filter.ClassDML || ds[0].action != filter.Keep {
		t.Errorf("parameterized INSERT = (%v, action=%v), want (DML, Keep) (R5.8)", ds[0].class, ds[0].action)
	}

	// Sequence numbers are strictly monotonic per connection across all kept
	// and dropped SQL-bearing messages (R3.7): three Q events + one P event = 4.
	if pev.Seq != 4 {
		t.Errorf("final per-connection Seq = %d, want 4 (R3.7)", pev.Seq)
	}
}

// TestIntegration_ByteFixtures_PerConnIndependentState verifies the state
// machine tracks two connections independently: a 'Z'='T' on one connection
// does not affect the other's Plain_SELECT decision (R4.2).
func TestIntegration_ByteFixtures_PerConnIndependentState(t *testing.T) {
	cfg := protocol.Config{MaxSQLLength: 1 << 20, ExtendedQuery: true}
	builder := protocol.NewEventBuilder(cfg)
	sm := filter.NewStateMachine()
	flt, err := filter.NewFilter(filter.Config{})
	if err != nil {
		t.Fatalf("filter.NewFilter: %v", err)
	}
	connA := canonicalConn()
	connB := core.ConnID{SrcIP: connA.SrcIP, SrcPort: connA.SrcPort + 1, DstIP: connA.DstIP, DstPort: connA.DstPort}

	// Move connA In-transaction via a backend 'Z'; connB stays Idle.
	sm.OnReadyForQuery(connA, 'T')

	decide := func(conn core.ConnID) filter.Action {
		ev, ok := builder.Build(conn, core.PGMessage{Type: 'Q', Payload: []byte("SELECT 1\x00")}, time.Now())
		if !ok {
			t.Fatalf("expected SQL event")
		}
		a, _ := flt.Decide(ev, sm.State(conn))
		return a
	}

	if a := decide(connA); a != filter.Keep {
		t.Errorf("connA in-tx Plain_SELECT = %v, want Keep (R4.2, R5.7)", a)
	}
	if a := decide(connB); a != filter.Drop {
		t.Errorf("connB idle Plain_SELECT = %v, want Drop (R4.2, R5.6)", a)
	}
}

// statsSource is a fake capture Source that surfaces fixed received/dropped
// counters. It documents the R1.7 contract that the capture layer reports a
// packet-drop count to the metrics layer; the live kernel drop counter requires
// the libpcap Source (build tag `pcap`) and a real NIC.
type statsSource struct {
	ch    chan gopacket.Packet
	stats CaptureStats
}

func (s *statsSource) Packets() <-chan gopacket.Packet { return s.ch }
func (s *statsSource) Stats() (CaptureStats, error)    { return s.stats, nil }
func (s *statsSource) Close() error                    { close(s.ch); return nil }

// TestIntegration_DropCountSurfacing asserts that a capture Source surfaces the
// received and dropped packet counters through the Source interface so the
// Metrics_Collector can record them (R1.7).
func TestIntegration_DropCountSurfacing(t *testing.T) {
	var src Source = &statsSource{
		ch:    make(chan gopacket.Packet),
		stats: CaptureStats{Received: 10_000, Dropped: 37},
	}
	defer src.Close()

	got, err := src.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Received != 10_000 || got.Dropped != 37 {
		t.Errorf("Stats = %+v, want Received=10000 Dropped=37 (R1.7)", got)
	}
}
