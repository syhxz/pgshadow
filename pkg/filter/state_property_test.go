package filter

import (
	"testing"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p6Event is a single ReadyForQuery status update applied to one connection.
// connIdx selects a connection out of a small fixed pool so that events
// naturally interleave across several ConnIDs.
type p6Event struct {
	connIdx int
	status  byte
}

// p6StatusToTxStatus mirrors the authoritative bidirectional mapping under test
// (R4.1): 'I' → Idle, 'T' → In transaction, 'E' → Failed.
func p6StatusToTxStatus(status byte) core.TxStatus {
	switch status {
	case 'I':
		return core.TxIdle
	case 'T':
		return core.TxInTx
	case 'E':
		return core.TxFailed
	default:
		return core.TxIdle
	}
}

// p6StatusGen generates one of the three valid ReadyForQuery status bytes.
func p6StatusGen() gopter.Gen {
	return gen.OneConstOf(byte('I'), byte('T'), byte('E'))
}

// p6EventGen generates a single (connIdx, status) event over numConns
// connections.
func p6EventGen(numConns int) gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(0, numConns-1),
		p6StatusGen(),
	).Map(func(vals []interface{}) p6Event {
		return p6Event{
			connIdx: vals[0].(int),
			status:  vals[1].(byte),
		}
	})
}

// Feature: pgshadow, Property 6: Bidirectional transaction-status mapping is correct and per-connection independent
//
// Validates: Requirements 4.1, 4.2
//
// For any set of ConnIDs receiving arbitrarily interleaved ReadyForQuery status
// bytes, each ConnID's resulting transaction state equals the mapping of its own
// most recent status byte (I→Idle, T→In transaction, E→Failed), independent of
// other ConnIDs.
func TestProperty6_BidirectionalStatusMappingPerConnIndependent(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100
	properties := gopter.NewProperties(params)

	const numConns = 5

	properties.Property("each ConnID maps to its most recent status byte, independent of others",
		prop.ForAll(
			func(events []p6Event) bool {
				m := NewStateMachine()

				// Build a stable pool of distinct ConnIDs via the reused conn() helper.
				conns := make([]core.ConnID, numConns)
				for i := range conns {
					conns[i] = conn(uint16(7000 + i))
				}

				// Apply the interleaved sequence and track each connection's last
				// status byte independently as the expectation oracle.
				lastStatus := make(map[int]byte)
				for _, e := range events {
					m.OnReadyForQuery(conns[e.connIdx], e.status)
					lastStatus[e.connIdx] = e.status
				}

				// Every connection's state must equal the mapping of its own most
				// recent status byte; connections that received no event remain Idle.
				for i := 0; i < numConns; i++ {
					want := core.TxIdle
					if s, ok := lastStatus[i]; ok {
						want = p6StatusToTxStatus(s)
					}
					if got := m.State(conns[i]); got != want {
						return false
					}
				}
				return true
			},
			gen.SliceOf(p6EventGen(numConns)),
		))

	properties.TestingRun(t)
}
