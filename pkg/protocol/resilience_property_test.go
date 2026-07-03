package protocol

import (
	"strings"
	"testing"
	"time"

	"pgshadow/pkg/core"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// p4MaxSQLLength is a deliberately small MaxSQLLength so that an oversized
// neighbor message is cheap to generate while valid messages stay well under
// the limit. Valid SQL strings are truncated to this many bytes; the oversized
// bad message carries strictly more than this many bytes.
const p4MaxSQLLength = 128

// p4Config returns the parser config used by Property 4. ExtendedQuery must be
// enabled so a malformed Parse ('P') message reaches the decode path and is
// recorded as a parse error (rather than being ignored as a disabled feature).
func p4Config() Config {
	return Config{MaxSQLLength: p4MaxSQLLength, ExtendedQuery: true}
}

// p4ValidSQLGen generates a NUL-free SQL string that is guaranteed valid (not
// oversized): NUL bytes are stripped so framing is unambiguous, and the text is
// truncated to MaxSQLLength bytes so it never trips the oversized check.
func p4ValidSQLGen() gopter.Gen {
	return gen.AnyString().Map(func(s string) string {
		s = p3StripNUL(s)
		if len(s) > p4MaxSQLLength {
			s = s[:p4MaxSQLLength]
		}
		return s
	})
}

// p4ValidSQLsGen generates the stream of valid Simple_Query SQL strings.
func p4ValidSQLsGen() gopter.Gen {
	return gen.SliceOf(p4ValidSQLGen())
}

// p4BadParseGen generates the payload for a malformed Parse ('P') message: a
// non-empty byte string with NO NUL terminators, so the Parse decoder cannot
// locate the required NUL-terminated statement-name and query strings and the
// message is recorded as a parse error.
func p4BadParseGen() gopter.Gen {
	return gen.AnyString().Map(func(s string) string {
		s = p3StripNUL(s)
		if s == "" {
			s = "x"
		}
		return s
	})
}

// Feature: pgshadow, Property 4: Valid messages survive a malformed or oversized neighbor
//
// Validates: Requirements 3.5, 3.6
//
// For any stream of valid PG Simple_Query messages with exactly one malformed
// or oversized message injected at an arbitrary position, framing (NewParser)
// followed by EventBuilder.Build (with a parse-error hook) emits every valid
// message in order with the correct SQL text, records exactly one parse error,
// skips exactly the bad message, and assigns per-ConnID sequence numbers that
// cover only the valid messages (contiguous 1..N) — all on the same connection.
func TestProperty4_ValidMessagesSurviveMalformedOrOversizedNeighbor(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100 // minimum 100 iterations

	properties := gopter.NewProperties(params)

	properties.Property("valid messages survive a malformed/oversized neighbor", prop.ForAll(
		func(sqls []string, oversized bool, badParse string, frac float64) bool {
			cfg := p4Config()

			// --- Build the valid Simple_Query wire messages, preserving order ---
			validWire := make([][]byte, len(sqls))
			for i, s := range sqls {
				validWire[i] = typedMsg(msgSimpleQuery, append([]byte(s), 0))
			}

			// --- Build the single bad neighbor message ---
			// Either an oversized Simple_Query (SQL strictly longer than
			// MaxSQLLength) or a malformed Parse (payload with no NUL
			// terminators). Both frame correctly but must be skipped by Build.
			var bad []byte
			if oversized {
				big := strings.Repeat("x", p4MaxSQLLength+1)
				bad = typedMsg(msgSimpleQuery, append([]byte(big), 0))
			} else {
				bad = typedMsg(msgParse, []byte(badParse))
			}

			// --- Inject the bad message at an arbitrary position ---
			pos := int(frac * float64(len(validWire)+1))
			if pos < 0 {
				pos = 0
			}
			if pos > len(validWire) {
				pos = len(validWire)
			}

			var stream []byte
			for i := 0; i < len(validWire); i++ {
				if i == pos {
					stream = append(stream, bad...)
				}
				stream = append(stream, validWire[i]...)
			}
			if pos == len(validWire) {
				stream = append(stream, bad...)
			}

			// --- Frame the whole stream on one connection ---
			parser := NewParser(cfg, false)
			msgs, err := parser.Feed(stream)
			if err != nil {
				return false
			}
			// Every message frames fine: the valid ones plus the bad one.
			if len(msgs) != len(validWire)+1 {
				return false
			}

			// --- Run each framed message through Build with a parse-error hook ---
			parseErrors := 0
			builder := NewEventBuilderWithHook(cfg, func() { parseErrors++ })
			conn := connID(9001)

			var events []*core.SQLEvent
			for _, m := range msgs {
				if ev, ok := builder.Build(conn, m, time.Now()); ok && ev != nil {
					events = append(events, ev)
				}
			}

			// Exactly one bad message was skipped and recorded (R3.6).
			if parseErrors != 1 {
				return false
			}

			// Every valid message produced an event, in order, with correct SQL.
			if len(events) != len(sqls) {
				return false
			}
			for i, ev := range events {
				if ev.SQL != sqls[i] {
					return false
				}
				// Per-ConnID Seq covers only the valid messages: the skipped
				// bad message never advances the sequence, so events remain a
				// contiguous, strictly-monotonic run 1..N (R3.6 continuation).
				if ev.Seq != uint64(i+1) {
					return false
				}
			}
			return true
		},
		p4ValidSQLsGen(),
		gen.Bool(),
		p4BadParseGen(),
		gen.Float64Range(0, 1),
	))

	properties.TestingRun(t)
}
