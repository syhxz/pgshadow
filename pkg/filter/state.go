package filter

import (
	"sync"

	"pgshadow/pkg/core"
)

// txStateMachine is the concrete StateMachine implementation. It tracks
// transaction state independently per ConnID (R4.2) behind a mutex-guarded map
// so it is safe for concurrent use.
//
// In bidirectional mode the authoritative source of state is the backend
// ReadyForQuery ('Z') status byte (R4.1). In unidirectional request-only mode
// the state is inferred from BEGIN/COMMIT/ROLLBACK control statements observed
// in the request stream (R4.3-R4.5).
type txStateMachine struct {
	mu     sync.Mutex
	states map[core.ConnID]core.TxStatus
}

// NewStateMachine constructs an empty StateMachine. A connection with no
// recorded state is treated as Idle (R4: a fresh connection is not in a
// transaction).
func NewStateMachine() StateMachine {
	return &txStateMachine{
		states: make(map[core.ConnID]core.TxStatus),
	}
}

// OnReadyForQuery applies a backend 'Z' status byte for the given connection,
// mapping 'I' → Idle, 'T' → In transaction, and 'E' → Failed transaction
// (R4.1). This is the authoritative bidirectional-mode update. An unrecognized
// status byte leaves the recorded state unchanged.
func (m *txStateMachine) OnReadyForQuery(conn core.ConnID, status byte) {
	var next core.TxStatus
	switch status {
	case 'I':
		next = core.TxIdle
	case 'T':
		next = core.TxInTx
	case 'E':
		next = core.TxFailed
	default:
		// Unknown status byte: do not corrupt existing state.
		return
	}

	m.mu.Lock()
	m.states[conn] = next
	m.mu.Unlock()
}

// OnStatement infers transaction state from a control statement observed in the
// request stream for unidirectional mode: BEGIN → In transaction (R4.4),
// COMMIT/ROLLBACK → Idle (R4.5). All other statement classes leave the recorded
// state unchanged, since they neither open nor close a transaction.
func (m *txStateMachine) OnStatement(conn core.ConnID, stmtType StmtType) {
	switch stmtType {
	case ClassBegin:
		m.mu.Lock()
		m.states[conn] = core.TxInTx
		m.mu.Unlock()
	case ClassCommitRollback:
		m.mu.Lock()
		m.states[conn] = core.TxIdle
		m.mu.Unlock()
	default:
		// Non-control statements do not change transaction state.
	}
}

// State returns the current transaction state for the connection. A connection
// with no recorded state is reported as Idle (R4.2).
func (m *txStateMachine) State(conn core.ConnID) core.TxStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.states[conn]
}

// Forget releases the transaction state associated with a connection, intended
// to be called when the connection is observed to close.
func (m *txStateMachine) Forget(conn core.ConnID) {
	m.mu.Lock()
	delete(m.states, conn)
	m.mu.Unlock()
}
