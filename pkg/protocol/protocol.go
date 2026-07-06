// Package protocol implements Module ② — Protocol Parser. From a per-connection
// byte stream it frames PostgreSQL wire-protocol messages, extracts SQL text
// into SQL_Events (R3), and drives the transaction state machine from backend
// ReadyForQuery messages (R4.1).
//
// This file contains compiling stubs only; method bodies are placeholders.
package protocol

import (
	"sync"

	"pgshadow/pkg/core"
)

// Parser is stateful per ConnID + direction. Derived from FrenzyPG framing.
type Parser interface {
	// Feed appends bytes and emits zero or more parsed messages. Incomplete
	// trailing bytes are retained until the next Feed. Unrecognized message
	// types (including GP-specific extensions) are skipped using the length
	// field without raising a parse error (R3.11).
	Feed(data []byte) ([]core.PGMessage, error)
}

// Config configures the protocol parser.
type Config struct {
	MaxSQLLength     int  `yaml:"max_sql_length"`      // bytes; default 1<<20 R3.5
	TimeoutIdle      int  `yaml:"timeout_idle_conn"`   // seconds; default 300 R2.4
	ExtendedQuery    bool `yaml:"extended_query"`      // default true R3.4
	ConnectionPooler bool `yaml:"connection_pooler"`   // true when a pooler (PgBouncer/Odyssey) sits between capture and PG
}

// TxIDGenerator (R3.8) is defined in txid.go; it allocates monotonic TxIDs per
// ConnID and tracks the current TxID for each connection.

// BindCorrelator tracks the most recent Parse per statement name per ConnID,
// correlating Bind messages with their Parse to produce complete parameterized
// SQL_Events (R3.9). All methods are safe for concurrent use; the instance-level
// mutex avoids cross-instance contention when multiple correlators exist.
type BindCorrelator struct {
	mu     sync.Mutex
	parsed map[core.ConnID]map[string]*ParseInfo
}

// ParseInfo holds the SQL and parameter OIDs declared by a Parse message.
type ParseInfo struct {
	SQL       string
	ParamOIDs []uint32
}

// CopyBuffer accumulates CopyData payloads for an in-progress COPY FROM. R3.10
type CopyBuffer struct {
	SQL  string
	Data [][]byte
}

// EventBuilder is implemented in eventbuilder.go (task 3.2): it converts
// client-side PGMessages into SQL_Events with a strictly-monotonic per-ConnID
// sequence number and routes server-side ReadyForQuery into transaction-state
// updates.

// SourceExecTimer (R10.7) is implemented in sourceexectimer.go (task 3.7): it
// measures source-side statement execution time from bidirectionally-captured
// traffic by tracking request→response intervals per ConnID.
