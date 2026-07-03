package filter

import (
	"net/netip"
	"sync"
	"testing"

	"pgshadow/pkg/core"
)

func conn(srcPort uint16) core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr("10.0.0.1"),
		SrcPort: srcPort,
		DstIP:   netip.MustParseAddr("10.0.0.2"),
		DstPort: 5432,
	}
}

// Bidirectional 'Z'-byte mapping: I/T/E → Idle/InTx/Failed, in both directions.
func TestOnReadyForQuery_StatusMapping(t *testing.T) {
	m := NewStateMachine()
	c := conn(1000)

	cases := []struct {
		status byte
		want   core.TxStatus
	}{
		{'I', core.TxIdle},
		{'T', core.TxInTx},
		{'E', core.TxFailed},
		// Bidirectional: transitions can go any direction.
		{'I', core.TxIdle},
		{'E', core.TxFailed},
		{'T', core.TxInTx},
		{'I', core.TxIdle},
	}
	for _, tc := range cases {
		m.OnReadyForQuery(c, tc.status)
		if got := m.State(c); got != tc.want {
			t.Fatalf("OnReadyForQuery(%q): got state %v, want %v", tc.status, got, tc.want)
		}
	}
}

// An unknown status byte must not corrupt the previously recorded state.
func TestOnReadyForQuery_UnknownStatusByteLeavesStateUnchanged(t *testing.T) {
	m := NewStateMachine()
	c := conn(1001)

	m.OnReadyForQuery(c, 'T')
	m.OnReadyForQuery(c, 'X') // unrecognized
	if got := m.State(c); got != core.TxInTx {
		t.Fatalf("unknown status byte changed state: got %v, want %v", got, core.TxInTx)
	}
}

// A connection with no recorded state defaults to Idle.
func TestState_DefaultsToIdle(t *testing.T) {
	m := NewStateMachine()
	if got := m.State(conn(1002)); got != core.TxIdle {
		t.Fatalf("default state: got %v, want %v", got, core.TxIdle)
	}
}

// Unidirectional inference: BEGIN → InTx, COMMIT/ROLLBACK → Idle. The mapping
// is one-directional through control statements only.
func TestOnStatement_UnidirectionalInference(t *testing.T) {
	m := NewStateMachine()
	c := conn(2000)

	m.OnStatement(c, ClassBegin)
	if got := m.State(c); got != core.TxInTx {
		t.Fatalf("after BEGIN: got %v, want %v", got, core.TxInTx)
	}

	m.OnStatement(c, ClassCommitRollback)
	if got := m.State(c); got != core.TxIdle {
		t.Fatalf("after COMMIT/ROLLBACK: got %v, want %v", got, core.TxIdle)
	}

	// BEGIN again then ROLLBACK (also ClassCommitRollback) returns to Idle.
	m.OnStatement(c, ClassBegin)
	if got := m.State(c); got != core.TxInTx {
		t.Fatalf("after second BEGIN: got %v, want %v", got, core.TxInTx)
	}
	m.OnStatement(c, ClassCommitRollback)
	if got := m.State(c); got != core.TxIdle {
		t.Fatalf("after ROLLBACK: got %v, want %v", got, core.TxIdle)
	}
}

// Non-control statement classes do not change the inferred transaction state.
func TestOnStatement_NonControlStatementsDoNotChangeState(t *testing.T) {
	m := NewStateMachine()
	c := conn(2001)

	m.OnStatement(c, ClassBegin)
	for _, st := range []StmtType{
		ClassPlainSelect, ClassProcSelect, ClassCall, ClassDML,
		ClassDDL, ClassUtility, ClassCopyFrom, ClassUnknown,
	} {
		m.OnStatement(c, st)
		if got := m.State(c); got != core.TxInTx {
			t.Fatalf("statement class %v changed state: got %v, want %v", st, got, core.TxInTx)
		}
	}
}

// State is maintained independently per ConnID.
func TestState_PerConnIDIndependence(t *testing.T) {
	m := NewStateMachine()
	a := conn(3000)
	b := conn(3001)

	m.OnReadyForQuery(a, 'T')
	m.OnReadyForQuery(b, 'E')
	m.OnStatement(a, ClassCommitRollback) // a → Idle

	if got := m.State(a); got != core.TxIdle {
		t.Fatalf("conn a: got %v, want %v", got, core.TxIdle)
	}
	if got := m.State(b); got != core.TxFailed {
		t.Fatalf("conn b changed by conn a updates: got %v, want %v", got, core.TxFailed)
	}
}

// Forget releases per-connection state; afterwards the connection reads back as
// the default Idle and does not affect other connections.
func TestForget(t *testing.T) {
	m := NewStateMachine()
	a := conn(4000)
	b := conn(4001)

	m.OnReadyForQuery(a, 'T')
	m.OnReadyForQuery(b, 'T')

	m.Forget(a)
	if got := m.State(a); got != core.TxIdle {
		t.Fatalf("after Forget(a): got %v, want %v (default Idle)", got, core.TxIdle)
	}
	if got := m.State(b); got != core.TxInTx {
		t.Fatalf("Forget(a) affected b: got %v, want %v", got, core.TxInTx)
	}

	// Forget on an unknown connection is a no-op.
	m.Forget(conn(9999))
}

// The state machine must be safe for concurrent use across distinct ConnIDs.
func TestStateMachine_ConcurrentAccess(t *testing.T) {
	m := NewStateMachine()
	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			c := conn(uint16(5000 + id))
			for i := 0; i < iterations; i++ {
				m.OnReadyForQuery(c, 'T')
				m.OnStatement(c, ClassCommitRollback)
				_ = m.State(c)
				m.Forget(c)
			}
		}(g)
	}
	wg.Wait()
}
