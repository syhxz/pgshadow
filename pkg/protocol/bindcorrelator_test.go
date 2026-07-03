// Unit tests for Bind→Parse parameter-value correlation (R3.9) — task 3.5.
//
// These tests verify that a Parse followed by a matching Bind reunites the
// declared parameter type OIDs (from Parse) with the actual parameter values
// (from Bind) in order, including NULL and binary values; that correlation is
// isolated per ConnID; and that an orphaned Bind (no matching Parse) is handled
// gracefully. They also exercise the Bind wire-format decoder directly for
// edge cases (truncation, format codes, the unnamed statement).
//
// mkConn is defined in txid_test.go (same package); tests here use distinct
// source ports for isolation.
package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	"pgshadow/pkg/core"
)

// bindParam describes one parameter value for buildBind: nil means SQL NULL.
type bindParam struct {
	value []byte
}

// buildBind assembles a Bind ('B') message payload (the bytes after the type
// byte and length prefix) from a portal name, source statement name, parameter
// format codes, parameter values, and result format codes. It mirrors the
// PostgreSQL wire format so tests feed the correlator realistic bytes.
func buildBind(portal, srcStmt string, formatCodes []int16, params []bindParam, resultCodes []int16) []byte {
	var b bytes.Buffer
	b.WriteString(portal)
	b.WriteByte(0)
	b.WriteString(srcStmt)
	b.WriteByte(0)

	writeInt16(&b, len(formatCodes))
	for _, c := range formatCodes {
		writeInt16(&b, int(c))
	}

	writeInt16(&b, len(params))
	for _, p := range params {
		if p.value == nil {
			writeInt32(&b, -1)
			continue
		}
		writeInt32(&b, len(p.value))
		b.Write(p.value)
	}

	writeInt16(&b, len(resultCodes))
	for _, c := range resultCodes {
		writeInt16(&b, int(c))
	}
	return b.Bytes()
}

func writeInt16(b *bytes.Buffer, v int) {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(v))
	b.Write(buf[:])
}

func writeInt32(b *bytes.Buffer, v int) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(int32(v)))
	b.Write(buf[:])
}

// A Parse followed by a matching Bind must yield the parameter values in order,
// each paired with the declared OID from the Parse.
func TestParseThenBindYieldsOrderedParams(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5001)

	c.OnParse(conn, "stmt1", "SELECT $1, $2", []uint32{23, 25}) // int4, text

	payload := buildBind("", "stmt1", nil, []bindParam{
		{value: []byte("42")},
		{value: []byte("hello")},
	}, nil)

	res, ok := c.OnBind(conn, payload)
	if !ok {
		t.Fatalf("OnBind ok = false, want true for matching Parse")
	}
	if res.SQL != "SELECT $1, $2" {
		t.Fatalf("SQL = %q, want %q", res.SQL, "SELECT $1, $2")
	}
	if res.StmtName != "stmt1" {
		t.Fatalf("StmtName = %q, want %q", res.StmtName, "stmt1")
	}
	if len(res.Params) != 2 {
		t.Fatalf("len(Params) = %d, want 2", len(res.Params))
	}
	if res.Params[0].OID != 23 || string(res.Params[0].Value) != "42" {
		t.Fatalf("Params[0] = {OID:%d Value:%q}, want {23 \"42\"}", res.Params[0].OID, res.Params[0].Value)
	}
	if res.Params[1].OID != 25 || string(res.Params[1].Value) != "hello" {
		t.Fatalf("Params[1] = {OID:%d Value:%q}, want {25 \"hello\"}", res.Params[1].OID, res.Params[1].Value)
	}
}

// NULL parameters (length -1) must be represented as a nil value, and binary
// values must be carried through byte-for-byte.
func TestBindNullAndBinaryValues(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5002)

	c.OnParse(conn, "s", "INSERT INTO t VALUES ($1, $2, $3)", []uint32{23, 17, 16}) // int4, bytea, bool

	binVal := []byte{0x00, 0x01, 0xFF, 0x7F} // arbitrary binary payload
	payload := buildBind("p", "s",
		[]int16{1, 1, 1}, // all-binary format codes (must be skipped, not consumed as values)
		[]bindParam{
			{value: nil},       // SQL NULL
			{value: binVal},    // binary bytea
			{value: []byte{1}}, // binary bool true
		}, []int16{0})

	res, ok := c.OnBind(conn, payload)
	if !ok {
		t.Fatalf("OnBind ok = false, want true")
	}
	if len(res.Params) != 3 {
		t.Fatalf("len(Params) = %d, want 3", len(res.Params))
	}
	if res.Params[0].Value != nil {
		t.Fatalf("Params[0].Value = %v, want nil (NULL)", res.Params[0].Value)
	}
	if !bytes.Equal(res.Params[1].Value, binVal) {
		t.Fatalf("Params[1].Value = %v, want %v", res.Params[1].Value, binVal)
	}
	if !bytes.Equal(res.Params[2].Value, []byte{1}) {
		t.Fatalf("Params[2].Value = %v, want [1]", res.Params[2].Value)
	}
}

// The extracted value must be independent of the input buffer: mutating the
// source payload after OnBind must not change the returned value.
func TestBindValueIsCopied(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5003)
	c.OnParse(conn, "", "SELECT $1", []uint32{25})

	payload := buildBind("", "", nil, []bindParam{{value: []byte("abc")}}, nil)
	res, ok := c.OnBind(conn, payload)
	if !ok {
		t.Fatalf("OnBind ok = false, want true")
	}
	// Corrupt the entire payload buffer in place.
	for i := range payload {
		payload[i] = 0xEE
	}
	if string(res.Params[0].Value) != "abc" {
		t.Fatalf("Params[0].Value = %q after buffer mutation, want %q", res.Params[0].Value, "abc")
	}
}

// Correlation must be isolated per ConnID: a Bind on a connection that never
// Parsed the statement must not match a Parse on a different connection.
func TestPerConnIsolation(t *testing.T) {
	c := NewBindCorrelator()
	connA := mkConn(5004)
	connB := mkConn(5005)

	c.OnParse(connA, "shared", "SELECT $1 FROM a", []uint32{23})

	payload := buildBind("", "shared", nil, []bindParam{{value: []byte("1")}}, nil)

	// Same statement name, different connection: must not resolve.
	if _, ok := c.OnBind(connB, payload); ok {
		t.Fatalf("OnBind on connB resolved a statement Parsed only on connA")
	}
	// The original connection still resolves correctly.
	if res, ok := c.OnBind(connA, payload); !ok {
		t.Fatalf("OnBind on connA ok = false, want true")
	} else if res.SQL != "SELECT $1 FROM a" {
		t.Fatalf("connA SQL = %q, want %q", res.SQL, "SELECT $1 FROM a")
	}
}

// An orphaned Bind (no matching Parse on the connection) must be reported as
// not-ok and not panic, on both an entirely unknown connection and a known
// connection that Parsed a different statement name (R3.9).
func TestOrphanBindHandledGracefully(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5006)

	// Unknown connection entirely.
	if _, ok := c.OnBind(conn, buildBind("", "ghost", nil, nil, nil)); ok {
		t.Fatalf("OnBind for unknown connection resolved, want not-ok")
	}

	// Known connection, but a different statement name was Parsed.
	c.OnParse(conn, "known", "SELECT 1", nil)
	if _, ok := c.OnBind(conn, buildBind("", "ghost", nil, nil, nil)); ok {
		t.Fatalf("OnBind for unparsed statement name resolved, want not-ok")
	}
}

// Re-Parsing the same statement name (common for the unnamed statement "")
// replaces the prior definition; a later Bind sees the newest SQL and OIDs.
func TestReParseReplacesDefinition(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5007)

	c.OnParse(conn, "", "SELECT $1", []uint32{23})
	c.OnParse(conn, "", "SELECT $1, $2", []uint32{25, 16}) // redefine unnamed statement

	payload := buildBind("", "", nil, []bindParam{
		{value: []byte("x")},
		{value: []byte("y")},
	}, nil)
	res, ok := c.OnBind(conn, payload)
	if !ok {
		t.Fatalf("OnBind ok = false, want true")
	}
	if res.SQL != "SELECT $1, $2" {
		t.Fatalf("SQL = %q, want redefined %q", res.SQL, "SELECT $1, $2")
	}
	if len(res.Params) != 2 || res.Params[0].OID != 25 || res.Params[1].OID != 16 {
		t.Fatalf("Params = %+v, want OIDs [25 16]", res.Params)
	}
}

// A statement with no parameters resolves to a result with no Params.
func TestBindNoParams(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5008)
	c.OnParse(conn, "np", "SELECT now()", nil)

	res, ok := c.OnBind(conn, buildBind("", "np", nil, nil, nil))
	if !ok {
		t.Fatalf("OnBind ok = false, want true")
	}
	if len(res.Params) != 0 {
		t.Fatalf("len(Params) = %d, want 0", len(res.Params))
	}
}

// Forget drops a connection's prepared statements so subsequent Binds no longer
// resolve, and leaves other connections untouched.
func TestForgetClearsConnState(t *testing.T) {
	c := NewBindCorrelator()
	connA := mkConn(5009)
	connB := mkConn(5010)
	c.OnParse(connA, "s", "SELECT $1", []uint32{23})
	c.OnParse(connB, "s", "SELECT $2", []uint32{25})

	c.Forget(connA)

	if _, ok := c.OnBind(connA, buildBind("", "s", nil, []bindParam{{value: []byte("1")}}, nil)); ok {
		t.Fatalf("OnBind on forgotten connection resolved, want not-ok")
	}
	if _, ok := c.OnBind(connB, buildBind("", "s", nil, []bindParam{{value: []byte("1")}}, nil)); !ok {
		t.Fatalf("Forget(connA) disturbed connB; OnBind(connB) ok = false, want true")
	}
}

// The decoder must reject malformed payloads (missing terminators / truncated
// fields) by returning ok=false rather than panicking.
func TestParseBindMessageMalformed(t *testing.T) {
	cases := map[string][]byte{
		"empty":                  {},
		"no portal terminator":   []byte("portal-with-no-nul"),
		"only portal":            append([]byte("p"), 0),             // missing source stmt string
		"truncated format count": append(append([]byte{0}, 0), 0x00), // portal "" stmt "" then 1 byte of Int16
		"value length truncated": func() []byte {
			var b bytes.Buffer
			b.WriteByte(0)        // portal ""
			b.WriteByte(0)        // stmt ""
			writeInt16(&b, 0)     // 0 format codes
			writeInt16(&b, 1)     // 1 value
			b.Write([]byte{0, 0}) // only 2 of 4 length bytes
			return b.Bytes()
		}(),
		"value bytes truncated": func() []byte {
			var b bytes.Buffer
			b.WriteByte(0)         // portal ""
			b.WriteByte(0)         // stmt ""
			writeInt16(&b, 0)      // 0 format codes
			writeInt16(&b, 1)      // 1 value
			writeInt32(&b, 8)      // claims 8 bytes
			b.Write([]byte("abc")) // only 3 provided
			return b.Bytes()
		}(),
	}
	for name, payload := range cases {
		if _, _, _, ok := parseBindMessage(payload); ok {
			t.Fatalf("%s: parseBindMessage ok = true, want false", name)
		}
	}
}

// A well-formed payload with format and result codes decodes to the correct
// source statement and values, confirming format/result codes are parsed correctly.
func TestParseBindMessageWellFormed(t *testing.T) {
	payload := buildBind("portal", "mystmt",
		[]int16{0, 1},
		[]bindParam{{value: []byte("v1")}, {value: nil}},
		[]int16{0, 0, 1})

	srcStmt, formats, values, ok := parseBindMessage(payload)
	if !ok {
		t.Fatalf("parseBindMessage ok = false, want true")
	}
	if srcStmt != "mystmt" {
		t.Fatalf("srcStmt = %q, want %q", srcStmt, "mystmt")
	}
	if len(formats) != 2 {
		t.Fatalf("len(formats) = %d, want 2", len(formats))
	}
	if formats[0] != 0 {
		t.Fatalf("formats[0] = %d, want 0", formats[0])
	}
	if formats[1] != 1 {
		t.Fatalf("formats[1] = %d, want 1", formats[1])
	}
	if len(values) != 2 {
		t.Fatalf("len(values) = %d, want 2", len(values))
	}
	if string(values[0]) != "v1" {
		t.Fatalf("values[0] = %q, want %q", values[0], "v1")
	}
	if values[1] != nil {
		t.Fatalf("values[1] = %v, want nil (NULL)", values[1])
	}
}

// Ensure the result type integrates with core.ParamInfo as the pipeline expects.
func TestBindResultParamInfoType(t *testing.T) {
	c := NewBindCorrelator()
	conn := mkConn(5011)
	c.OnParse(conn, "s", "SELECT $1", []uint32{23})
	res, ok := c.OnBind(conn, buildBind("", "s", nil, []bindParam{{value: []byte("7")}}, nil))
	if !ok {
		t.Fatalf("OnBind ok = false, want true")
	}
	var _ []core.ParamInfo = res.Params // compile-time type assertion
}
