// This file implements Bind→Parse parameter-value correlation (R3.9) — task 3.5.
//
// In the Extended_Query protocol a prepared statement is defined once by a
// Parse ('P') message (statement name + SQL + declared parameter type OIDs,
// extracted by the EventBuilder, task 3.2) and then executed any number of
// times. Each execution begins with a Bind ('B') message that names a source
// prepared statement and supplies the *actual* parameter values for that run.
// To replay a parameterized Extended_Query faithfully (R3.3/R3.9) the bound
// values must be reunited with the SQL and parameter OIDs declared by the
// matching Parse.
//
// BindCorrelator is the component that performs this correlation. It is a
// standalone, fully unit-tested collaborator of the EventBuilder; the actual
// wiring into EventBuilder.Build is performed during pipeline assembly
// (task 12.2). The contract is:
//
//   - OnParse(conn, stmtName, sql, oids) — record a prepared statement for the
//     connection. A later Parse that reuses the same statement name replaces
//     the previous definition (PostgreSQL forbids redefining a live named
//     statement, but the unnamed statement "" is routinely reused).
//   - OnBind(conn, payload) — decode a Bind message's wire format, look up the
//     prepared statement it references on the same connection, and return the
//     ordered parameter values zipped with the declared OIDs as []core.ParamInfo
//     together with the statement's SQL. Returns ok=false for an orphaned Bind
//     whose source statement was never Parsed on the connection (R3.9 error
//     handling: skip the Bind, continue).
//   - Forget(conn) — drop all prepared statements for a connection, called on
//     connection close/timeout so per-connection state does not leak.
//
// Per-connection isolation is total: a Parse on one connection is never visible
// to a Bind on another, mirroring how PostgreSQL scopes prepared statements to
// a session. All methods are safe for concurrent use.
package protocol

import (
	"encoding/binary"

	"pgshadow/pkg/core"
)

// NewBindCorrelator returns an initialized BindCorrelator with no recorded
// prepared statements.
func NewBindCorrelator() *BindCorrelator {
	return &BindCorrelator{parsed: make(map[core.ConnID]map[string]*ParseInfo)}
}

// BindResult is the outcome of correlating a Bind message with its Parse. It
// carries everything needed to populate a parameterized SQL_Event: the source
// statement name, the SQL declared by the Parse, and the parameter values from
// the Bind zipped with the parameter type OIDs from the Parse (R3.9).
type BindResult struct {
	StmtName string           // source prepared-statement name from the Bind
	SQL      string           // SQL text from the matching Parse
	Params   []core.ParamInfo // OID (from Parse) + Value (from Bind), in order
}

// OnParse records the prepared statement declared by a Parse message for conn.
// A statement name reused on the same connection replaces the prior definition.
// The supplied OID slice is copied so the caller may reuse its backing array.
func (c *BindCorrelator) OnParse(conn core.ConnID, stmtName, sql string, paramOIDs []uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.parsed == nil {
		c.parsed = make(map[core.ConnID]map[string]*ParseInfo)
	}
	stmts := c.parsed[conn]
	if stmts == nil {
		stmts = make(map[string]*ParseInfo)
		c.parsed[conn] = stmts
	}
	oids := append([]uint32(nil), paramOIDs...)
	stmts[stmtName] = &ParseInfo{SQL: sql, ParamOIDs: oids}
}

// OnBind decodes a Bind message payload and correlates it with the prepared
// statement it references on conn. On success it returns a BindResult whose
// Params combine the declared parameter OIDs (from the matching Parse) with the
// actual values (from the Bind), in parameter order, and ok=true.
//
// It returns ok=false when the Bind payload is malformed (cannot be decoded per
// the wire format) or when it references a statement that has not been Parsed on
// the connection (an orphaned Bind). In both cases the caller skips the Bind and
// continues parsing the connection (R3.9).
func (c *BindCorrelator) OnBind(conn core.ConnID, payload []byte) (*BindResult, bool) {
	srcStmt, formats, values, ok := parseBindMessage(payload)
	if !ok {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	stmts := c.parsed[conn]
	if stmts == nil {
		return nil, false
	}
	info := stmts[srcStmt]
	if info == nil {
		// Orphaned Bind: no matching Parse on this connection (R3.9).
		return nil, false
	}

	return &BindResult{
		StmtName: srcStmt,
		SQL:      info.SQL,
		Params:   zipParams(info.ParamOIDs, formats, values),
	}, true
}

// Forget drops all prepared statements recorded for conn. It models connection
// close or idle timeout so per-connection correlation state does not leak.
// Forget on an unknown connection is a no-op.
func (c *BindCorrelator) Forget(conn core.ConnID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.parsed, conn)
}

// zipParams pairs declared parameter type OIDs with bound parameter values in
// order. The two counts normally match, but the protocol does not guarantee it
// (e.g. a Bind may legitimately supply zero values to reuse server defaults, or
// a malformed peer may disagree): the result is sized to the larger of the two
// so neither an OID nor a value is silently dropped, with the missing side left
// at its zero value (OID 0 / nil value / format 0).
//
// formats contains the per-parameter format codes from the Bind message.
// PostgreSQL allows either zero format codes (all text), one code (applied to
// all), or exactly N codes (one per parameter). This function resolves the
// effective format per parameter according to those rules.
func zipParams(oids []uint32, formats []int16, values [][]byte) []core.ParamInfo {
	n := len(oids)
	if len(values) > n {
		n = len(values)
	}
	if n == 0 {
		return nil
	}
	params := make([]core.ParamInfo, n)
	for i := 0; i < n; i++ {
		if i < len(oids) {
			params[i].OID = oids[i]
		}
		if i < len(values) {
			params[i].Value = values[i]
		}
		// Resolve format code per PostgreSQL protocol rules:
		// 0 formats → all text (0); 1 format → applies to all; N formats → per-param.
		switch len(formats) {
		case 0:
			params[i].Format = 0 // text
		case 1:
			params[i].Format = formats[0]
		default:
			if i < len(formats) {
				params[i].Format = formats[i]
			}
		}
	}
	return params
}

// parseBindMessage decodes a Bind ('B') message payload, returning the source
// prepared-statement name, the per-parameter format codes, and the ordered
// parameter values (R3.9). The wire format of the payload is:
//
//	String  destination portal name      (NUL-terminated; "" = unnamed portal)
//	String  source prepared-statement name (NUL-terminated; "" = unnamed stmt)
//	Int16   number of parameter format codes (C)
//	Int16   parameter format code × C        (0 = text, 1 = binary)
//	Int16   number of parameter values (V)
//	  per value:
//	    Int32  value length in bytes (-1 = SQL NULL, no bytes follow)
//	    Byte   value data × length
//	Int16   number of result-column format codes (R)
//	Int16   result format code × R              (ignored for value extraction)
//
// A NULL parameter (length -1) is returned as a nil slice; a present value is
// returned as a copy of its bytes, independent of the input buffer. Format
// codes are preserved so the replayer can faithfully replay parameters in their
// original wire format (text or binary). The result-format trailer is
// irrelevant to value extraction and is not parsed.
//
// ok is false if any field runs past the end of the payload.
func parseBindMessage(payload []byte) (srcStmt string, formats []int16, values [][]byte, ok bool) {
	p := payload

	// Destination portal name (consumed and discarded).
	_, p, ok = readCString(p)
	if !ok {
		return "", nil, nil, false
	}

	// Source prepared-statement name.
	srcStmt, p, ok = readCString(p)
	if !ok {
		return "", nil, nil, false
	}

	// Parameter format codes: read the count, then collect each Int16 code.
	formatCount, p, ok := readInt16(p)
	if !ok {
		return "", nil, nil, false
	}
	if formatCount > 0 {
		formats = make([]int16, 0, formatCount)
		for i := 0; i < formatCount; i++ {
			var fc int
			if fc, p, ok = readInt16(p); !ok {
				return "", nil, nil, false
			}
			formats = append(formats, int16(fc))
		}
	}

	// Parameter values.
	valueCount, p, ok := readInt16(p)
	if !ok {
		return "", nil, nil, false
	}
	// Guard against a malformed message with a huge parameter count that
	// would cause an excessive allocation. PostgreSQL itself limits functions
	// to 100 parameters; 10000 is a generous upper bound.
	if valueCount > 10000 {
		return "", nil, nil, false
	}
	values = make([][]byte, 0, valueCount)
	for i := 0; i < valueCount; i++ {
		var length int32
		length, p, ok = readInt32(p)
		if !ok {
			return "", nil, nil, false
		}
		if length < 0 {
			// SQL NULL: no bytes follow.
			values = append(values, nil)
			continue
		}
		if int64(length) > int64(len(p)) {
			return "", nil, nil, false
		}
		val := make([]byte, length)
		copy(val, p[:length])
		values = append(values, val)
		p = p[length:]
	}

	// Result-column format codes follow but are irrelevant to value extraction.
	return srcStmt, formats, values, true
}

// readCString reads a NUL-terminated string from the front of buf, returning
// the string (without the terminator) and the remaining bytes after it. ok is
// false when no NUL terminator is present.
func readCString(buf []byte) (s string, rest []byte, ok bool) {
	for i := 0; i < len(buf); i++ {
		if buf[i] == 0 {
			return string(buf[:i]), buf[i+1:], true
		}
	}
	return "", buf, false
}

// readInt16 reads a big-endian Int16 from the front of buf, returning its value
// and the remaining bytes. ok is false when fewer than 2 bytes remain.
func readInt16(buf []byte) (v int, rest []byte, ok bool) {
	if len(buf) < 2 {
		return 0, buf, false
	}
	return int(binary.BigEndian.Uint16(buf[:2])), buf[2:], true
}

// readInt32 reads a big-endian signed Int32 from the front of buf, returning its
// value and the remaining bytes. ok is false when fewer than 4 bytes remain.
// The value is signed so the -1 NULL sentinel decodes correctly.
func readInt32(buf []byte) (v int32, rest []byte, ok bool) {
	if len(buf) < 4 {
		return 0, buf, false
	}
	return int32(binary.BigEndian.Uint32(buf[:4])), buf[4:], true
}
