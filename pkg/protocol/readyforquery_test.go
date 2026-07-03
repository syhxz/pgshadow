package protocol

import (
	"testing"

	"pgshadow/pkg/core"
)

// TestExtractReadyForQuery_StatusBytes verifies that a backend ReadyForQuery
// ('Z') message yields its transaction status byte for each of the three
// PostgreSQL status indicators I/T/E (R4.1).
func TestExtractReadyForQuery_StatusBytes(t *testing.T) {
	for _, status := range []byte{'I', 'T', 'E'} {
		m := core.PGMessage{Type: 'Z', Length: 5, Payload: []byte{status}}
		got, ok := ExtractReadyForQuery(m)
		if !ok {
			t.Fatalf("status %q: expected ok=true", status)
		}
		if got != status {
			t.Errorf("status byte = %q, want %q", got, status)
		}
	}
}

// TestExtractReadyForQuery_NonZMessageIgnored verifies that non-'Z' messages
// are not treated as ReadyForQuery (R4.1).
func TestExtractReadyForQuery_NonZMessageIgnored(t *testing.T) {
	for _, typ := range []byte{'Q', 'P', 'C', 'B', startupMsgType} {
		m := core.PGMessage{Type: typ, Payload: []byte{'I'}}
		if got, ok := ExtractReadyForQuery(m); ok {
			t.Errorf("type %q: expected ok=false, got status %q", typ, got)
		}
	}
}

// TestExtractReadyForQuery_EmptyPayloadIgnored verifies that a malformed 'Z'
// message carrying no status byte is reported as not-extractable so callers do
// not corrupt transaction state (R4.1).
func TestExtractReadyForQuery_EmptyPayloadIgnored(t *testing.T) {
	m := core.PGMessage{Type: 'Z', Length: 4, Payload: nil}
	if got, ok := ExtractReadyForQuery(m); ok {
		t.Errorf("empty 'Z' payload: expected ok=false, got status %q", got)
	}
}

// TestExtractReadyForQuery_FromFramedBackendStream verifies the extractor works
// end-to-end on a 'Z' message produced by the backend-direction framing parser
// (R3.1 + R4.1).
func TestExtractReadyForQuery_FromFramedBackendStream(t *testing.T) {
	p := NewParser(defaultConfig(), false) // backend direction: typed from the first byte

	msgs, err := p.Feed(typedMsg('Z', []byte{'T'}))
	if err != nil {
		t.Fatalf("Feed returned error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 framed message, got %d", len(msgs))
	}
	status, ok := ExtractReadyForQuery(msgs[0])
	if !ok || status != 'T' {
		t.Fatalf("ExtractReadyForQuery = (%q,%v), want ('T',true)", status, ok)
	}
}
