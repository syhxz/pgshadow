// This file implements PG wire-protocol message framing (R3.1) — task 3.1.
//
// Framing is the lowest layer of the Protocol_Parser: it turns a per-connection
// byte stream into discrete PGMessages using the message type byte, the 4-byte
// length field, and the message payload. It is stateful per ConnID + direction:
// incomplete trailing bytes are retained across Feed calls until enough bytes
// arrive to complete the next message.
//
// The PostgreSQL wire protocol has two framings:
//   - Startup phase (frontend/client→server only): the very first message has
//     NO type byte. It is Int32 length (big-endian, inclusive of the 4 length
//     bytes) followed by the payload. This covers the StartupMessage as well as
//     the SSLRequest, GSSENCRequest, and CancelRequest negotiation messages.
//   - Typed phase: every message is a 1-byte type, an Int32 length (inclusive of
//     the 4 length bytes but NOT the type byte), and the payload.
//
// SQL extraction, EventBuilder, TxID generation, Bind correlation, COPY stream
// handling, and malformed/oversized skipping are implemented by later tasks
// (3.2–3.7); this file is intentionally scoped to framing only and structured
// so those layers can consume the emitted PGMessages.
package protocol

import (
	"encoding/binary"
	"fmt"

	"pgshadow/pkg/core"
)

// Startup-phase request codes carried in the first 4 bytes of an untyped
// startup message payload. These identify whether the connection is beginning
// a normal session (protocol 3.0) or issuing a negotiation/cancel request.
const (
	protocolVersion30 = 196608   // 0x00030000 — StartupMessage, protocol 3.0
	sslRequestCode    = 80877103 // SSLRequest
	gssEncRequestCode = 80877104 // GSSENCRequest
	cancelRequestCode = 80877102 // CancelRequest
)

// typedHeaderLen is the size of a typed message header: 1 type byte + 4 length.
const typedHeaderLen = 5

// startupHeaderLen is the size of a startup-phase header: 4 length bytes only.
const startupHeaderLen = 4

// startupMsgType is the synthetic PGMessage.Type used to represent an untyped
// startup-phase message (which has no type byte on the wire). Real typed
// messages always carry a non-zero ASCII type byte, so 0 is unambiguous.
const startupMsgType byte = 0

// streamParser frames PG wire-protocol messages from a single direction of a
// single connection. It implements the Parser interface. Instances are stateful
// and not safe for concurrent use; callers create one parser per
// (ConnID, direction) pair.
type streamParser struct {
	cfg        Config
	fromClient bool   // frontend direction: the first message is the untyped startup message
	inStartup  bool   // true while still expecting untyped startup-phase message(s)
	buf        []byte // retained bytes that did not yet form a complete message
}

// NewParser creates a stateful framing parser for one direction of one
// connection. fromClient must be true for the client→server (frontend)
// direction, where the first message is the untyped startup message; it must be
// false for the server→client (backend) direction, which uses typed framing
// from the first byte.
func NewParser(cfg Config, fromClient bool) Parser {
	return &streamParser{
		cfg:        cfg,
		fromClient: fromClient,
		inStartup:  fromClient,
	}
}

// Feed appends data to the parser's internal buffer and emits every complete
// message that can now be framed, in order. Bytes that form only a partial
// message are retained for the next Feed call (R3.1).
func (p *streamParser) Feed(data []byte) ([]core.PGMessage, error) {
	if len(data) > 0 {
		p.buf = append(p.buf, data...)
	}

	var msgs []core.PGMessage
	for {
		var (
			msg      core.PGMessage
			consumed int
			ok       bool
			err      error
		)
		if p.inStartup {
			msg, consumed, ok, err = frameStartup(p.buf)
		} else {
			msg, consumed, ok, err = frameTyped(p.buf)
		}
		if err != nil {
			return msgs, err
		}
		if !ok {
			// Not enough bytes for the next message; retain and wait.
			break
		}

		msgs = append(msgs, msg)
		p.buf = p.buf[consumed:]

		if p.inStartup && transitionToTyped(msg) {
			p.inStartup = false
		}
	}

	p.compact()
	return msgs, nil
}

// compact drops the consumed prefix's backing array so a fully drained stream
// does not retain a large buffer, and copies any leftover bytes into a fresh
// slice to avoid pinning an oversized backing array across Feed calls.
func (p *streamParser) compact() {
	if len(p.buf) == 0 {
		p.buf = nil
		return
	}
	leftover := make([]byte, len(p.buf))
	copy(leftover, p.buf)
	p.buf = leftover
}

// frameTyped attempts to frame one typed message (type byte + 4-byte length +
// payload) from the front of buf. It returns the framed message, the number of
// bytes consumed, ok=true when a complete message was framed, and ok=false when
// more bytes are needed.
func frameTyped(buf []byte) (core.PGMessage, int, bool, error) {
	if len(buf) < typedHeaderLen {
		return core.PGMessage{}, 0, false, nil
	}

	typ := buf[0]
	length := binary.BigEndian.Uint32(buf[1:5])
	// length covers the 4 length bytes plus the payload, so it must be >= 4.
	if length < 4 {
		return core.PGMessage{}, 0, false, fmt.Errorf("protocol: invalid length %d for message type %q", length, typ)
	}

	// Guard against malicious or corrupt large-length values: cap at 256MB to
	// prevent a single frame from allocating ~4GB and causing OOM (R3.6). Real
	// PG messages rarely exceed a few MB (even COPY payloads are chunked).
	const maxFrameLength = 256 * 1024 * 1024 // 256 MB
	if length > maxFrameLength {
		return core.PGMessage{}, 0, false, fmt.Errorf("protocol: frame length %d exceeds max %d for message type %q", length, maxFrameLength, typ)
	}

	total := 1 + int(length) // type byte is not included in length; safe on 64-bit (length <= 256MB)
	if len(buf) < total {
		return core.PGMessage{}, 0, false, nil
	}

	msg := core.PGMessage{
		Type:    typ,
		Length:  length,
		Payload: clonePayload(buf[typedHeaderLen:total]),
	}
	return msg, total, true, nil
}

// frameStartup attempts to frame one untyped startup-phase message (4-byte
// length + payload) from the front of buf.
func frameStartup(buf []byte) (core.PGMessage, int, bool, error) {
	if len(buf) < startupHeaderLen {
		return core.PGMessage{}, 0, false, nil
	}

	length := binary.BigEndian.Uint32(buf[0:4])
	// length is inclusive of the 4 length bytes, so it must be >= 4.
	if length < 4 {
		return core.PGMessage{}, 0, false, fmt.Errorf("protocol: invalid startup message length %d", length)
	}

	total := int(length)
	if len(buf) < total {
		return core.PGMessage{}, 0, false, nil
	}

	msg := core.PGMessage{
		Type:    startupMsgType,
		Length:  length,
		Payload: clonePayload(buf[startupHeaderLen:total]),
	}
	return msg, total, true, nil
}

// transitionToTyped decides, after an untyped startup-phase message has been
// framed, whether the connection now switches to typed message framing. A
// protocol-3.0 StartupMessage (or any unrecognized version) transitions to
// typed framing; SSLRequest/GSSENCRequest negotiations leave the connection in
// startup phase because the real StartupMessage still follows untyped, and a
// CancelRequest carries no further messages.
func transitionToTyped(msg core.PGMessage) bool {
	if len(msg.Payload) < 4 {
		// Cannot identify the request code; assume a normal startup and move on.
		return true
	}
	code := binary.BigEndian.Uint32(msg.Payload[0:4])
	switch code {
	case sslRequestCode, gssEncRequestCode, cancelRequestCode:
		return false
	default:
		// protocolVersion30 and any other protocol version use typed framing.
		return true
	}
}

// clonePayload returns an independent copy of the payload bytes so that emitted
// PGMessages never alias the parser's internal buffer or the caller's input.
func clonePayload(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
