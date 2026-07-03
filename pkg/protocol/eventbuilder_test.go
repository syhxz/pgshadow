package protocol

import (
	"encoding/binary"
	"net/netip"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// connID builds a distinct ConnID from a source port for per-connection tests.
func connID(srcPort uint16) core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr("10.0.0.1"),
		SrcPort: srcPort,
		DstIP:   netip.MustParseAddr("10.0.0.2"),
		DstPort: 5432,
	}
}

// parseMsg builds a Parse ('P') message PAYLOAD: NUL-terminated statement name,
// NUL-terminated query, Int16 OID count, then Int32 OIDs.
func parsePayload(stmtName, query string, oids []uint32) []byte {
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

func msg(typ byte, payload []byte) core.PGMessage {
	return core.PGMessage{Type: typ, Length: uint32(4 + len(payload)), Payload: payload}
}

func TestBuild_SimpleQueryStripsTrailingNUL(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})
	conn := connID(1001)

	ev, ok := b.Build(conn, msg('Q', []byte("select 1\x00")), time.Unix(100, 0))
	if !ok {
		t.Fatalf("expected a SQL event for Simple_Query")
	}
	if ev.SQL != "select 1" {
		t.Errorf("SQL = %q, want %q", ev.SQL, "select 1")
	}
	if ev.Extended {
		t.Errorf("Extended = true, want false for Simple_Query")
	}
	if ev.Conn != conn {
		t.Errorf("Conn = %v, want %v", ev.Conn, conn)
	}
	if !ev.Timestamp.Equal(time.Unix(100, 0)) {
		t.Errorf("Timestamp = %v, want %v", ev.Timestamp, time.Unix(100, 0))
	}
}

func TestBuild_SimpleQueryWithoutTrailingNUL(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})

	ev, ok := b.Build(connID(1001), msg('Q', []byte("select 2")), time.Now())
	if !ok {
		t.Fatalf("expected a SQL event")
	}
	if ev.SQL != "select 2" {
		t.Errorf("SQL = %q, want %q", ev.SQL, "select 2")
	}
}

func TestBuild_ParseExtractsNameSQLAndOIDs(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})

	payload := parsePayload("stmt1", "select * from t where id = $1 and v = $2", []uint32{23, 25})
	ev, ok := b.Build(connID(2002), msg('P', payload), time.Now())
	if !ok {
		t.Fatalf("expected a SQL event for Parse")
	}
	if ev.StmtName != "stmt1" {
		t.Errorf("StmtName = %q, want %q", ev.StmtName, "stmt1")
	}
	if ev.SQL != "select * from t where id = $1 and v = $2" {
		t.Errorf("SQL = %q", ev.SQL)
	}
	if !ev.Extended {
		t.Errorf("Extended = false, want true for Parse")
	}
	if len(ev.Params) != 2 {
		t.Fatalf("len(Params) = %d, want 2", len(ev.Params))
	}
	if ev.Params[0].OID != 23 || ev.Params[1].OID != 25 {
		t.Errorf("Params OIDs = %d, %d, want 23, 25", ev.Params[0].OID, ev.Params[1].OID)
	}
	if ev.Params[0].Value != nil {
		t.Errorf("Param value should be nil until Bind correlation (task 3.5)")
	}
}

func TestBuild_ParseUnnamedStatementNoParams(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})

	payload := parsePayload("", "begin", nil)
	ev, ok := b.Build(connID(2002), msg('P', payload), time.Now())
	if !ok {
		t.Fatalf("expected a SQL event")
	}
	if ev.StmtName != "" {
		t.Errorf("StmtName = %q, want empty", ev.StmtName)
	}
	if ev.SQL != "begin" {
		t.Errorf("SQL = %q, want %q", ev.SQL, "begin")
	}
	if len(ev.Params) != 0 {
		t.Errorf("len(Params) = %d, want 0", len(ev.Params))
	}
}

func TestBuild_ParseIgnoredWhenExtendedQueryDisabled(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: false})

	payload := parsePayload("stmt1", "select 1", []uint32{23})
	ev, ok := b.Build(connID(2002), msg('P', payload), time.Now())
	if ok || ev != nil {
		t.Fatalf("expected no event when ExtendedQuery disabled, got %+v", ev)
	}
}

func TestBuild_NonSQLMessagesReturnNil(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})
	conn := connID(3003)

	for _, typ := range []byte{'B', 'S', 'E', 'D', 'C', 'H', 'X', 'd', 'c', 'f', startupMsgType} {
		ev, ok := b.Build(conn, msg(typ, []byte("ignored")), time.Now())
		if ok || ev != nil {
			t.Errorf("message type %q: expected (nil,false), got (%+v,%v)", typ, ev, ok)
		}
	}
}

func TestBuild_NonSQLMessagesDoNotAdvanceSeq(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})
	conn := connID(3003)

	// Interleave non-SQL messages; they must not consume sequence numbers.
	b.Build(conn, msg('B', nil), time.Now())
	first, _ := b.Build(conn, msg('Q', []byte("a\x00")), time.Now())
	b.Build(conn, msg('S', nil), time.Now())
	second, _ := b.Build(conn, msg('Q', []byte("b\x00")), time.Now())

	if first.Seq != 1 {
		t.Errorf("first Seq = %d, want 1", first.Seq)
	}
	if second.Seq != 2 {
		t.Errorf("second Seq = %d, want 2", second.Seq)
	}
}

func TestBuild_PerConnSeqStrictlyMonotonic(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})
	conn := connID(4004)

	var prev uint64
	for i := 0; i < 50; i++ {
		ev, ok := b.Build(conn, msg('Q', []byte("select 1\x00")), time.Now())
		if !ok {
			t.Fatalf("expected event at iteration %d", i)
		}
		if ev.Seq <= prev {
			t.Fatalf("Seq not strictly increasing: got %d after %d", ev.Seq, prev)
		}
		prev = ev.Seq
	}
}

func TestBuild_SeqIndependentPerConn(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})
	connA := connID(5005)
	connB := connID(6006)

	a1, _ := b.Build(connA, msg('Q', []byte("x\x00")), time.Now())
	b1, _ := b.Build(connB, msg('Q', []byte("y\x00")), time.Now())
	a2, _ := b.Build(connA, msg('Q', []byte("z\x00")), time.Now())
	b2, _ := b.Build(connB, msg('Q', []byte("w\x00")), time.Now())

	if a1.Seq != 1 || a2.Seq != 2 {
		t.Errorf("connA seqs = %d, %d, want 1, 2", a1.Seq, a2.Seq)
	}
	if b1.Seq != 1 || b2.Seq != 2 {
		t.Errorf("connB seqs = %d, %d, want 1, 2", b1.Seq, b2.Seq)
	}
}

func TestBuild_ParseMissingNULReturnsNil(t *testing.T) {
	b := NewEventBuilder(Config{ExtendedQuery: true})

	// Payload with no NUL terminators cannot be decoded as a Parse message.
	ev, ok := b.Build(connID(7007), msg('P', []byte("no terminators here")), time.Now())
	if ok || ev != nil {
		t.Fatalf("expected (nil,false) for undecodable Parse, got (%+v,%v)", ev, ok)
	}
}
