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

// p5ConnPool is the small pool of source ports from which generated statements
// draw their ConnID. Keeping the pool small forces many statements onto each
// connection so the per-ConnID sequence is exercised deeply, while having more
// than one entry exercises per-ConnID independence across an interleaved stream.
var p5ConnPool = []uint16{1001, 1002, 1003}

// p5StripNUL removes NUL bytes from generated SQL text. NUL is the Simple_Query
// payload terminator on the wire, so the SQL body itself must be NUL-free.
func p5StripNUL(s string) string { return strings.ReplaceAll(s, "\x00", "") }

// p5Stmt is one generated statement in the interleaved stream: the index into
// p5ConnPool selecting its connection, plus the Simple_Query SQL text.
type p5Stmt struct {
	connIdx int
	sql     string
}

// p5GenStmt generates a single statement: a connection chosen from the small
// pool and arbitrary (NUL-free) SQL text.
func p5GenStmt() gopter.Gen {
	return gopter.CombineGens(
		gen.IntRange(0, len(p5ConnPool)-1),
		gen.AnyString().Map(p5StripNUL),
	).Map(func(vals []interface{}) p5Stmt {
		return p5Stmt{connIdx: vals[0].(int), sql: vals[1].(string)}
	})
}

// p5GenStream generates an interleaved stream of statements across the
// connection pool.
func p5GenStream() gopter.Gen {
	return gen.SliceOf(p5GenStmt())
}

// Feature: pgshadow, Property 5: Per-connection sequence numbers are strictly monotonic
//
// Validates: Requirements 3.7
//
// For any interleaved stream of Simple_Query statements drawn from a small pool
// of connections, feeding each through a single EventBuilder yields, for every
// connection, Seq values that are exactly 1, 2, 3, ... in emission order — with
// no gaps and no duplicates relative to that connection's base. Because each
// connection is tracked independently, interleaving connections in the stream
// does not perturb any individual connection's 1..N progression.
func TestProperty5_PerConnectionSequenceMonotonic(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 100 // minimum 100 iterations

	properties := gopter.NewProperties(params)

	properties.Property("per-ConnID Seq is exactly 1..N in emission order, independent across ConnIDs", prop.ForAll(
		func(stream []p5Stmt) bool {
			b := NewEventBuilder(Config{ExtendedQuery: true})

			// expected[conn] is the next Seq we expect for that connection.
			expected := make(map[core.ConnID]uint64)

			for _, st := range stream {
				conn := connID(p5ConnPool[st.connIdx])

				ev, ok := b.Build(conn, msg('Q', append([]byte(st.sql), 0)), time.Now())
				if !ok || ev == nil {
					// Every Simple_Query carries SQL and must emit an event.
					return false
				}
				if ev.Conn != conn {
					return false
				}

				expected[conn]++
				// Strictly monotonic, gap-free, per-connection base of 1.
				if ev.Seq != expected[conn] {
					return false
				}
			}
			return true
		},
		p5GenStream(),
	))

	properties.TestingRun(t)
}
