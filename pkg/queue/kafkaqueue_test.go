package queue

import (
	"net"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"

	"pgshadow/pkg/core"
)

// closedAddr returns a TCP address that is guaranteed to refuse connections: it
// binds an ephemeral port, captures its address, then closes the listener so a
// subsequent dial gets connection-refused (fast, deterministic, no live broker).
func closedAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

// shortDialTimeout shrinks the per-broker reachability probe for the duration
// of a test so unreachable-broker cases fail fast even if a dial would
// otherwise hang.
func shortDialTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := kafkaDialTimeout
	kafkaDialTimeout = d
	t.Cleanup(func() { kafkaDialTimeout = prev })
}

// --- Startup reachability check (R6.10) ---

func TestKafkaNewRequiresBrokers(t *testing.T) {
	if _, err := New(Config{Type: "kafka", Kafka: KafkaConfig{Topic: "t"}}, nil); err == nil {
		t.Fatalf("New(type=kafka) with no brokers expected error, got nil")
	}
}

func TestKafkaNewFailsFastOnUnreachableBrokers(t *testing.T) {
	shortDialTimeout(t, 500*time.Millisecond)
	b1 := closedAddr(t)
	b2 := closedAddr(t)

	q, err := New(Config{Type: "kafka", Kafka: KafkaConfig{
		Brokers: []string{b1, b2},
		Topic:   "pgshadow",
	}}, nil)
	if err == nil {
		if q != nil {
			_ = q.Close()
		}
		t.Fatalf("New(type=kafka) with unreachable brokers expected error, got nil")
	}
	// The error must name the unreachable brokers (R6.10).
	for _, b := range []string{b1, b2} {
		if !strings.Contains(err.Error(), b) {
			t.Fatalf("error %q does not name unreachable broker %q", err.Error(), b)
		}
	}
}

func TestKafkaDialBrokersReportsUnreachable(t *testing.T) {
	reachable := closedAddr(t) // closed -> reported as unreachable
	another := closedAddr(t)

	got := dialBrokers([]string{reachable, another}, 500*time.Millisecond)
	if len(got) != 2 {
		t.Fatalf("dialBrokers returned %v, want both brokers reported unreachable", got)
	}
}

// --- ConnID partition keying (R6.7) ---

func TestKafkaMessageKeyedByConnID(t *testing.T) {
	want := &core.SQLEvent{
		Conn: core.ConnID{
			SrcIP:   netip.MustParseAddr("10.0.0.7"),
			SrcPort: 51000,
			DstIP:   netip.MustParseAddr("10.0.0.1"),
			DstPort: 5432,
		},
		SQL:       "UPDATE accounts SET bal = bal + $1 WHERE id = $2",
		Timestamp: time.Date(2024, 1, 2, 3, 4, 5, 678000000, time.UTC),
		TxID:      77,
		Seq:       9,
		Extended:  true,
		StmtName:  "upd",
		Params:    []core.ParamInfo{{OID: 23, Value: []byte("100")}, {OID: 23, Value: []byte("42")}},
	}

	msg, err := kafkaMessage(want)
	if err != nil {
		t.Fatalf("kafkaMessage error: %v", err)
	}

	// Key must be the stable ConnID key so same-connection events hash to one
	// partition and preserve order (R6.7).
	if string(msg.Key) != want.Conn.Key() {
		t.Fatalf("message key = %q, want ConnID key %q", string(msg.Key), want.Conn.Key())
	}

	// Value must round-trip back to an identical event (every field preserved).
	got, err := decodeEvent(msg.Value)
	if err != nil {
		t.Fatalf("decodeEvent error: %v", err)
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Fatalf("Timestamp = %v, want %v", got.Timestamp, want.Timestamp)
	}
	got.Timestamp = want.Timestamp // normalize for deep compare
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-trip mismatch:\n got = %+v\nwant = %+v", got, want)
	}
}

// Events from the same ConnID must produce identical keys (so they land in the
// same partition), while distinct ConnIDs generally produce distinct keys.
func TestKafkaMessageKeyStablePerConn(t *testing.T) {
	conn := core.ConnID{
		SrcIP:   netip.MustParseAddr("192.168.1.5"),
		SrcPort: 40000,
		DstIP:   netip.MustParseAddr("192.168.1.1"),
		DstPort: 5432,
	}
	a, err := kafkaMessage(&core.SQLEvent{Conn: conn, Seq: 1, SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("kafkaMessage a: %v", err)
	}
	b, err := kafkaMessage(&core.SQLEvent{Conn: conn, Seq: 2, SQL: "SELECT 2"})
	if err != nil {
		t.Fatalf("kafkaMessage b: %v", err)
	}
	if string(a.Key) != string(b.Key) {
		t.Fatalf("same-ConnID keys differ: %q vs %q", string(a.Key), string(b.Key))
	}

	other := conn
	other.SrcPort = 40001
	c, err := kafkaMessage(&core.SQLEvent{Conn: other, Seq: 1, SQL: "SELECT 1"})
	if err != nil {
		t.Fatalf("kafkaMessage c: %v", err)
	}
	if string(a.Key) == string(c.Key) {
		t.Fatalf("distinct-ConnID keys unexpectedly equal: %q", string(a.Key))
	}
}
