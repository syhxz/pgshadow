package main

import (
	"context"
	"encoding/binary"
	"net/netip"
	"testing"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/pipeline"
	"pgshadow/pkg/protocol"
)

// --- test doubles ------------------------------------------------------------

// recordingQueue is a queue.Queue that captures every enqueued event so a test
// can assert the producer side wired capture→parser→filter→queue correctly,
// without a live NIC or database.
type recordingQueue struct{ events []*core.SQLEvent }

func (q *recordingQueue) Enqueue(ev *core.SQLEvent) bool {
	q.events = append(q.events, ev)
	return false
}
func (q *recordingQueue) Dequeue(context.Context) (*core.SQLEvent, bool) { return nil, false }
func (q *recordingQueue) Depth() int                                     { return len(q.events) }
func (q *recordingQueue) Close() error                                   { return nil }

// --- wire-message builders ---------------------------------------------------

// startupMsg builds an untyped protocol-3.0 StartupMessage so the client-stream
// parser transitions from startup to typed framing (mirrors a real session).
func startupMsg() []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, 196608) // protocol 3.0
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(4+len(payload)))
	copy(buf[4:], payload)
	return buf
}

// simpleQuery builds a Simple_Query ('Q') message carrying NUL-terminated SQL.
func simpleQuery(sql string) []byte {
	payload := append([]byte(sql), 0)
	buf := make([]byte, 5+len(payload))
	buf[0] = 'Q'
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(payload)))
	copy(buf[5:], payload)
	return buf
}

// readyForQuery builds a backend ReadyForQuery ('Z') message with the given
// transaction status byte ('I'/'T'/'E').
func readyForQuery(status byte) []byte {
	buf := make([]byte, 6)
	buf[0] = 'Z'
	binary.BigEndian.PutUint32(buf[1:5], 5) // 4 length bytes + 1 payload byte
	buf[5] = status
	return buf
}

func testConn() core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr("10.0.0.1"),
		SrcPort: 51000,
		DstIP:   netip.MustParseAddr("10.0.0.2"),
		DstPort: 5432,
	}
}

func newTestProcessor(t *testing.T, fcfg filter.Config) (*pipeline.StreamProcessor, *recordingQueue) {
	t.Helper()
	q := &recordingQueue{}
	sp, err := pipeline.NewStreamProcessor(protocol.Config{ExtendedQuery: true}, fcfg, q, nil)
	if err != nil {
		t.Fatalf("NewStreamProcessor: %v", err)
	}
	return sp, q
}

// --- tests -------------------------------------------------------------------

// TestSimpleQueryIsEnqueued feeds the client-direction bytes for a startup
// message followed by an INSERT Simple_Query and asserts a single kept event
// reaches the queue with the extracted SQL, a sequence number, and a TxID.
func TestSimpleQueryIsEnqueued(t *testing.T) {
	sp, q := newTestProcessor(t, filter.Config{}) // default rules keep DML
	conn := testConn()

	bytes := append(startupMsg(), simpleQuery("INSERT INTO t VALUES (1)")...)
	sp.OnBytes(conn, true, bytes)

	if len(q.events) != 1 {
		t.Fatalf("enqueued %d events, want 1", len(q.events))
	}
	ev := q.events[0]
	if ev.SQL != "INSERT INTO t VALUES (1)" {
		t.Errorf("SQL = %q, want the INSERT statement", ev.SQL)
	}
	if ev.Seq != 1 {
		t.Errorf("Seq = %d, want 1", ev.Seq)
	}
	if ev.TxID == 0 {
		t.Errorf("TxID = 0, want a non-zero implicit transaction id")
	}
	if ev.Conn != conn {
		t.Errorf("Conn = %v, want %v", ev.Conn, conn)
	}
}

// TestReadyForQueryUpdatesState feeds backend ReadyForQuery messages on the
// server direction and asserts they drive the transaction state machine
// (R12.1/R4.1): 'T' → InTx, 'I' → Idle, 'E' → Failed.
func TestReadyForQueryUpdatesState(t *testing.T) {
	sp, _ := newTestProcessor(t, filter.Config{})
	conn := testConn()

	sp.OnBytes(conn, false, readyForQuery('T'))
	if got := sp.StateMachine().State(conn); got != core.TxInTx {
		t.Fatalf("after Z='T' state = %v, want TxInTx", got)
	}

	sp.OnBytes(conn, false, readyForQuery('I'))
	if got := sp.StateMachine().State(conn); got != core.TxIdle {
		t.Fatalf("after Z='I' state = %v, want TxIdle", got)
	}

	sp.OnBytes(conn, false, readyForQuery('E'))
	if got := sp.StateMachine().State(conn); got != core.TxFailed {
		t.Fatalf("after Z='E' state = %v, want TxFailed", got)
	}
}

// TestTransactionStateGatesPlainSelect demonstrates the state-machine→filter
// integration: under the default rule matrix a Plain_SELECT is dropped out of a
// transaction (R5.6) but kept inside one (R5.7). The in-transaction state is
// established here unidirectionally by a preceding BEGIN.
func TestTransactionStateGatesPlainSelect(t *testing.T) {
	conn := testConn()

	t.Run("plain select dropped out of transaction", func(t *testing.T) {
		sp, q := newTestProcessor(t, filter.Config{})
		sp.OnBytes(conn, true, append(startupMsg(), simpleQuery("SELECT * FROM t")...))
		if len(q.events) != 0 {
			t.Fatalf("enqueued %d events, want 0 (plain SELECT dropped out of tx)", len(q.events))
		}
	})

	t.Run("plain select kept inside transaction", func(t *testing.T) {
		sp, q := newTestProcessor(t, filter.Config{})
		buf := startupMsg()
		buf = append(buf, simpleQuery("BEGIN")...)
		buf = append(buf, simpleQuery("SELECT * FROM t")...)
		sp.OnBytes(conn, true, buf)

		// BEGIN (always kept, R5.10) + the in-transaction SELECT (R5.7).
		if len(q.events) != 2 {
			t.Fatalf("enqueued %d events, want 2 (BEGIN + in-tx SELECT)", len(q.events))
		}
		if got := sp.StateMachine().State(conn); got != core.TxInTx {
			t.Errorf("state after BEGIN = %v, want TxInTx", got)
		}
		if q.events[1].SQL != "SELECT * FROM t" {
			t.Errorf("second event SQL = %q, want the SELECT", q.events[1].SQL)
		}
		// Per-connection sequence numbers are strictly monotonic (R3.7).
		if q.events[0].Seq != 1 || q.events[1].Seq != 2 {
			t.Errorf("sequence numbers = %d,%d, want 1,2", q.events[0].Seq, q.events[1].Seq)
		}
	})
}

// TestOnCloseReleasesState asserts per-connection state is released on close so
// nothing leaks for dead connections (R2.5).
func TestOnCloseReleasesState(t *testing.T) {
	sp, _ := newTestProcessor(t, filter.Config{})
	conn := testConn()

	sp.OnBytes(conn, true, append(startupMsg(), simpleQuery("INSERT INTO t VALUES (1)")...))
	if !sp.HasParser(conn) || sp.SeqFor(conn) == 0 {
		t.Fatal("expected per-connection state after processing")
	}

	sp.OnClose(conn)
	if sp.HasParser(conn) {
		t.Error("parser state not released on close")
	}
	if sp.HasSeq(conn) {
		t.Error("sequence state not released on close")
	}
	if got := sp.StateMachine().State(conn); got != core.TxIdle {
		t.Errorf("state after close = %v, want TxIdle (forgotten)", got)
	}
}
