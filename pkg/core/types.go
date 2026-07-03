// Package core defines the shared data types that flow through the pgshadow
// pipeline (capture → protocol → filter → queue → replayer). These types are
// referenced by every pipeline stage, so they live in a dependency-free
// package to avoid import cycles.
package core

import (
	"net/netip"
	"strconv"
	"time"
)

// ConnID is the four-tuple identifying one captured TCP connection. R2.1
type ConnID struct {
	SrcIP   netip.Addr
	SrcPort uint16
	DstIP   netip.Addr
	DstPort uint16
}

// Key returns a stable, canonical string for the connection four-tuple. It is
// suitable for use as a Go map key and as a Kafka partition key (R6.7), so that
// all events from the same source connection hash to the same partition and
// retain per-connection ordering. The encoding is deterministic for a given
// ConnID value.
func (c ConnID) Key() string {
	return c.SrcIP.String() + ":" + strconv.FormatUint(uint64(c.SrcPort), 10) +
		"-" + c.DstIP.String() + ":" + strconv.FormatUint(uint64(c.DstPort), 10)
}

// String implements fmt.Stringer using the same stable encoding as Key.
func (c ConnID) String() string {
	return c.Key()
}

// ParamInfo carries a single Extended_Query parameter: the type OID declared in
// the Parse message and the actual value extracted from the Bind message (R3.9).
type ParamInfo struct {
	OID    uint32 // parameter type OID from Parse message
	Value  []byte // actual parameter value from Bind message (R3.9); nil means SQL NULL
	Format int16  // wire format code: 0 = text, 1 = binary (R3.9)
}

// SQLEvent is the unit that flows through filter → queue → replayer. R3.7
type SQLEvent struct {
	Conn           ConnID        // source connection key
	SQL            string        // extracted statement text
	Timestamp      time.Time     // capture timestamp (drives pacing)
	TxID           uint64        // transaction identifier, generated per R3.8
	Seq            uint64        // per-connection monotonic sequence number
	Extended       bool          // true if from Extended_Query (Parse+Bind)
	StmtName       string        // prepared-statement name, if any (R3.3)
	Params         []ParamInfo   // parameter values from Bind correlation (R3.9)
	CopyData       [][]byte      // COPY FROM data payload, if any (R3.10)
	SourceExecTime time.Duration // source-side exec time from bidirectional capture (R10.7), zero if unavailable
	User           string        // source database user who issued the statement (R18)
}

// PGMessage is a framed wire-protocol message. R3.1
type PGMessage struct {
	Type    byte   // 'Q', 'P', 'B', 'E', 'Z', ...
	Length  uint32 // declared 4-byte length
	Payload []byte
}

// TxStatus mirrors the PG ReadyForQuery status byte. R4.1
type TxStatus uint8

const (
	TxIdle   TxStatus = iota // 'I'
	TxInTx                   // 'T'
	TxFailed                 // 'E'
)
