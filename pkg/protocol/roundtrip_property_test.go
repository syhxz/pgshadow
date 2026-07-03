package protocol

import (
	"strings"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p3StripNUL removes NUL bytes from a generated string. The PG wire protocol
// uses NUL as the string terminator for Simple_Query payloads and for the
// statement-name/query fields of a Parse message, so generated SQL and
// statement names must be NUL-free for the framing to be unambiguous. Unicode
// and other special characters are preserved.
func p3StripNUL(s string) string { return strings.ReplaceAll(s, "\x00", "") }

// p3GenSQL generates arbitrary SQL text, including unicode and embedded special
// characters, with NUL bytes removed.
func p3GenSQL() gopter.Gen {
	return gen.AnyString().Map(p3StripNUL)
}

// p3GenStmtName generates arbitrary prepared-statement names (including the
// empty unnamed statement), with NUL bytes removed.
func p3GenStmtName() gopter.Gen {
	return gen.AnyString().Map(p3StripNUL)
}

// p3GenOIDs generates a random list of declared parameter type OIDs.
func p3GenOIDs() gopter.Gen {
	return gen.SliceOf(gen.UInt32())
}

// Feature: pgshadow, Property 3: Protocol parsing round-trips SQL text
//
// Validates: Requirements 3.1, 3.2, 3.3
//
// For any valid SQL text, encoding it as a Simple_Query ('Q') message — and,
// for any statement name / SQL / parameter set, as an Extended_Query Parse ('P')
// message — then framing (NewParser) and parsing (NewEventBuilder) the bytes
// reproduces the original SQL text (trailing NUL removed) and, for Parse
// messages, the original statement name and declared parameter OIDs.
func TestProperty3_ProtocolRoundTripsSQLText(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100 // minimum 100 iterations

	properties := gopter.NewProperties(params)

	properties.Property("framing+EventBuilder round-trips Simple_Query and Parse", prop.ForAll(
		func(sql, stmtName string, oids []uint32) bool {
			cfg := defaultConfig()

			// --- Simple_Query ('Q') round-trip (R3.1, R3.2) ---
			// Payload is the SQL text followed by a single trailing NUL.
			qParser := NewParser(cfg, false)
			qBuilder := NewEventBuilder(cfg)

			qWire := typedMsg(msgSimpleQuery, append([]byte(sql), 0))
			qMsgs, err := qParser.Feed(qWire)
			if err != nil || len(qMsgs) != 1 {
				return false
			}
			qEv, ok := qBuilder.Build(connID(1), qMsgs[0], time.Now())
			if !ok || qEv == nil {
				return false
			}
			if qEv.SQL != sql || qEv.Extended {
				return false
			}

			// --- Extended_Query Parse ('P') round-trip (R3.3) ---
			pParser := NewParser(cfg, false)
			pBuilder := NewEventBuilder(cfg)

			pWire := typedMsg(msgParse, parsePayload(stmtName, sql, oids))
			pMsgs, err := pParser.Feed(pWire)
			if err != nil || len(pMsgs) != 1 {
				return false
			}
			pEv, ok := pBuilder.Build(connID(2), pMsgs[0], time.Now())
			if !ok || pEv == nil {
				return false
			}
			if pEv.SQL != sql || pEv.StmtName != stmtName || !pEv.Extended {
				return false
			}
			if len(pEv.Params) != len(oids) {
				return false
			}
			for i, oid := range oids {
				if pEv.Params[i].OID != oid {
					return false
				}
			}
			return true
		},
		p3GenSQL(),
		p3GenStmtName(),
		p3GenOIDs(),
	))

	properties.TestingRun(t)
}
