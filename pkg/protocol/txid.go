// This file implements TxID generation (R3.8) — task 3.4.
//
// TxIDGenerator allocates the transaction identifiers carried by SQLEvent.TxID
// (see pkg/core). Its purpose is to group a connection's statements that belong
// to the same transaction under a single, stable identifier, so that the buffer
// and replay layers can route and serialize a transaction's statements together
// (R3.8, supporting session-affinity replay in R7.5/R7.10).
//
// Layering: TxIDGenerator provides the allocation primitives only. The decision
// of *when* a new transaction group begins or ends is made by the EventBuilder
// (task 3.2), which has the transaction-state context. The contract is:
//
//   - Advance(conn) — begin a new transaction group for conn and obtain its
//     TxID. The EventBuilder calls this when a BEGIN is observed, or when the
//     first DML/DDL is observed on a connection that is outside an explicit
//     transaction (an implicit single-statement transaction).
//   - Current(conn) — obtain the TxID for the connection's active transaction
//     group. Every subsequent SQLEvent on the connection carries this same TxID
//     until the group ends.
//   - Reset(conn)   — end the connection's transaction group. The EventBuilder
//     calls this when a COMMIT or ROLLBACK is observed, so the next qualifying
//     statement starts a fresh group via Advance.
//
// TxIDs are drawn from a single global counter, so every allocated TxID is
// unique across all connections and strictly increasing in allocation order.
// Per-connection state is independent: allocating or resetting one connection's
// TxID never affects another's.
package protocol

import (
	"sync"

	"pgshadow/pkg/core"
)

// TxIDGenerator allocates monotonically-increasing TxIDs and tracks the current
// TxID per ConnID (R3.8).
//
// The zero value is ready to use; NewTxIDGenerator is provided for explicit
// construction. All methods are safe for concurrent use by multiple goroutines.
type TxIDGenerator struct {
	mu      sync.Mutex
	current map[core.ConnID]uint64 // per-connection active TxID
	counter uint64                 // global monotonic allocation counter
}

// NewTxIDGenerator returns an initialized TxIDGenerator.
func NewTxIDGenerator() *TxIDGenerator {
	return &TxIDGenerator{current: make(map[core.ConnID]uint64)}
}

// Current returns the TxID of the connection's active transaction group, or 0
// if no group is active (nothing has been allocated yet for conn, or the last
// group was Reset). The returned value is stable across repeated calls until
// the next Advance or Reset for the same connection, so all statements within a
// transaction share one TxID.
func (g *TxIDGenerator) Current(conn core.ConnID) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.current[conn]
}

// Advance allocates a new, globally-unique, monotonically-increasing TxID,
// associates it with conn as the active transaction group, and returns it.
// Allocation begins at 1, reserving 0 to mean "no active transaction group".
func (g *TxIDGenerator) Advance(conn core.ConnID) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.current == nil {
		g.current = make(map[core.ConnID]uint64)
	}
	g.counter++
	g.current[conn] = g.counter
	return g.counter
}

// Reset ends the connection's active transaction group so that Current reports
// 0 until the next Advance. It models the observation of COMMIT or ROLLBACK.
// Reset on a connection with no active group is a no-op.
func (g *TxIDGenerator) Reset(conn core.ConnID) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.current, conn)
}
