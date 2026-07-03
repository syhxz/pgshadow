// This file implements ReadyForQuery extraction for the server→client
// direction (R4.1) — task 3.3.
//
// In the backend (server→client) byte stream, the framing layer (framing.go)
// produces typed PGMessages just like any other direction. Among them is the
// ReadyForQuery ('Z') message, whose single payload byte is the transaction
// status indicator. The protocol parser's job for R4.1 is to detect that
// message and surface its status byte so the wiring layer can forward it to the
// Transaction_State_Machine (filter.StateMachine.OnReadyForQuery), which maps
// 'I'→Idle, 'T'→In transaction, 'E'→Failed transaction.
//
// This extractor is intentionally a pure, stateless function over a single
// framed message: it neither holds per-connection state nor depends on the
// EventBuilder, so the backend-direction parser can call it on every emitted
// message without coupling to SQL extraction.
package protocol

import "pgshadow/pkg/core"

// msgReadyForQuery is the backend ReadyForQuery message type byte. R4.1
const msgReadyForQuery byte = 'Z'

// ExtractReadyForQuery reports whether msg is a backend ReadyForQuery ('Z')
// message and, if so, returns its one-byte transaction status indicator for the
// server→client direction (R4.1). The returned byte is forwarded downstream to
// the Transaction_State_Machine, which maps 'I'/'T'/'E' to Idle/InTx/Failed; an
// unrecognized status byte is handled (ignored) by that state machine rather
// than here, so this function does not validate the byte's value.
//
// ok is false when msg is not a ReadyForQuery message, or when a 'Z' message
// carries no payload byte (a malformed ReadyForQuery): in that case callers
// must not update transaction state.
func ExtractReadyForQuery(msg core.PGMessage) (status byte, ok bool) {
	if msg.Type != msgReadyForQuery {
		return 0, false
	}
	if len(msg.Payload) < 1 {
		return 0, false
	}
	return msg.Payload[0], true
}
