package protocol

import (
	"encoding/binary"
	"testing"

	"pgshadow/pkg/core"
)

// typedMsg builds a wire-format typed message: type byte + Int32 length
// (inclusive of the 4 length bytes) + payload.
func typedMsg(typ byte, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = typ
	binary.BigEndian.PutUint32(out[1:5], uint32(4+len(payload)))
	copy(out[5:], payload)
	return out
}

// startupMsg builds a wire-format untyped startup-phase message: Int32 length
// (inclusive of the 4 length bytes) + payload. code is written as the first 4
// payload bytes (the protocol version or request code).
func startupMsg(code uint32, rest []byte) []byte {
	payload := make([]byte, 4+len(rest))
	binary.BigEndian.PutUint32(payload[0:4], code)
	copy(payload[4:], rest)

	out := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(4+len(payload)))
	copy(out[4:], payload)
	return out
}

func defaultConfig() Config {
	return Config{MaxSQLLength: 1 << 20, ExtendedQuery: true}
}

func TestFeed_SingleCompleteTypedMessage(t *testing.T) {
	// Backend direction uses typed framing immediately.
	p := NewParser(defaultConfig(), false)

	payload := []byte("select 1\x00")
	msgs, err := p.Feed(typedMsg('Q', payload))
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Type != 'Q' {
		t.Errorf("type = %q, want 'Q'", msgs[0].Type)
	}
	if int(msgs[0].Length) != 4+len(payload) {
		t.Errorf("length = %d, want %d", msgs[0].Length, 4+len(payload))
	}
	if string(msgs[0].Payload) != string(payload) {
		t.Errorf("payload = %q, want %q", msgs[0].Payload, payload)
	}
}

func TestFeed_MultipleMessagesInOneBuffer(t *testing.T) {
	p := NewParser(defaultConfig(), false)

	var buf []byte
	buf = append(buf, typedMsg('Q', []byte("a\x00"))...)
	buf = append(buf, typedMsg('Q', []byte("bb\x00"))...)
	buf = append(buf, typedMsg('S', nil)...) // Sync: length 4, no payload

	msgs, err := p.Feed(buf)
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "a\x00" || string(msgs[1].Payload) != "bb\x00" {
		t.Errorf("payloads = %q, %q", msgs[0].Payload, msgs[1].Payload)
	}
	if msgs[2].Type != 'S' || len(msgs[2].Payload) != 0 {
		t.Errorf("third message = type %q payload %q, want Sync with empty payload", msgs[2].Type, msgs[2].Payload)
	}
}

func TestFeed_MessageSplitAcrossFeedCalls(t *testing.T) {
	p := NewParser(defaultConfig(), false)

	full := typedMsg('Q', []byte("select * from t\x00"))

	// Split at every boundary and feed one byte at a time; the message must
	// only emerge once the final byte arrives.
	var got []core.PGMessage
	for i := 0; i < len(full); i++ {
		msgs, err := p.Feed(full[i : i+1])
		if err != nil {
			t.Fatalf("Feed returned error at byte %d: %v", i, err)
		}
		if i < len(full)-1 && len(msgs) != 0 {
			t.Fatalf("got %d messages before stream complete (byte %d)", len(msgs), i)
		}
		got = append(got, msgs...)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 message total, got %d", len(got))
	}
	if string(got[0].Payload) != "select * from t\x00" {
		t.Errorf("payload = %q", got[0].Payload)
	}
}

func TestFeed_HeaderSplitAcrossFeedCalls(t *testing.T) {
	p := NewParser(defaultConfig(), false)

	full := typedMsg('Q', []byte("xyz\x00"))

	// Feed first 3 bytes (less than the 5-byte header): nothing yet.
	msgs, err := p.Feed(full[:3])
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages with partial header, got %d", len(msgs))
	}

	msgs, err = p.Feed(full[3:])
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "xyz\x00" {
		t.Fatalf("expected the completed message, got %+v", msgs)
	}
}

func TestFeed_TwoMessagesSecondTrailingPartial(t *testing.T) {
	p := NewParser(defaultConfig(), false)

	first := typedMsg('Q', []byte("one\x00"))
	second := typedMsg('Q', []byte("two\x00"))

	// First message complete + only the first 2 bytes of the second.
	buf := append(append([]byte{}, first...), second[:2]...)
	msgs, err := p.Feed(buf)
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	// Deliver the remainder of the second message.
	msgs, err = p.Feed(second[2:])
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != "two\x00" {
		t.Fatalf("expected the second message, got %+v", msgs)
	}
}

func TestFeed_StartupThenTypedFraming(t *testing.T) {
	// Frontend direction: first message is the untyped StartupMessage, then the
	// connection transitions to typed framing.
	p := NewParser(defaultConfig(), true)

	startup := startupMsg(protocolVersion30, []byte("user\x00postgres\x00\x00"))
	query := typedMsg('Q', []byte("select 1\x00"))

	msgs, err := p.Feed(append(append([]byte{}, startup...), query...))
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Type != startupMsgType {
		t.Errorf("startup message type = %q, want 0 (untyped)", msgs[0].Type)
	}
	if binary.BigEndian.Uint32(msgs[0].Payload[0:4]) != protocolVersion30 {
		t.Errorf("startup payload did not carry protocol version 3.0")
	}
	if msgs[1].Type != 'Q' || string(msgs[1].Payload) != "select 1\x00" {
		t.Errorf("second message = type %q payload %q", msgs[1].Type, msgs[1].Payload)
	}
}

func TestFeed_SSLRequestStaysInStartupPhase(t *testing.T) {
	// SSLRequest is untyped and must NOT transition to typed framing: a real
	// untyped StartupMessage still follows.
	p := NewParser(defaultConfig(), true)

	ssl := startupMsg(sslRequestCode, nil) // length 8, just the code
	startup := startupMsg(protocolVersion30, []byte("user\x00bob\x00\x00"))
	query := typedMsg('Q', []byte("select 2\x00"))

	buf := append(append(append([]byte{}, ssl...), startup...), query...)
	msgs, err := p.Feed(buf)
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].Type != startupMsgType || msgs[1].Type != startupMsgType {
		t.Errorf("first two messages should be untyped startup-phase messages, got %q, %q", msgs[0].Type, msgs[1].Type)
	}
	if msgs[2].Type != 'Q' {
		t.Errorf("third message type = %q, want 'Q'", msgs[2].Type)
	}
}

func TestFeed_BackendNeverInStartupPhase(t *testing.T) {
	// Server→client direction frames typed messages from the first byte (e.g.
	// ReadyForQuery 'Z' with a single status byte payload).
	p := NewParser(defaultConfig(), false)

	z := typedMsg('Z', []byte{'I'})
	msgs, err := p.Feed(z)
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Type != 'Z' || string(msgs[0].Payload) != "I" {
		t.Fatalf("expected ReadyForQuery, got %+v", msgs)
	}
}

func TestFeed_EmptyInputRetainsState(t *testing.T) {
	p := NewParser(defaultConfig(), false)

	msgs, err := p.Feed(nil)
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no messages from empty feed, got %d", len(msgs))
	}
}

func TestFeed_InvalidLengthReturnsError(t *testing.T) {
	p := NewParser(defaultConfig(), false)

	// Type byte 'Q' with a declared length of 3 (< 4) is invalid framing.
	bad := []byte{'Q', 0, 0, 0, 3, 'x'}
	_, err := p.Feed(bad)
	if err == nil {
		t.Fatalf("expected an error for length < 4, got nil")
	}
}
