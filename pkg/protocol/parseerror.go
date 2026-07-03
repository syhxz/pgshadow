// This file implements malformed/oversized message handling for the protocol
// parser (R3.5, R3.6) — task 3.3.
//
// The EventBuilder (eventbuilder.go) frames and decodes client-side messages
// into SQL_Events. Some framed messages must be skipped rather than turned into
// events:
//
//   - Oversized: a message whose extracted SQL text exceeds the configured
//     MaxSQLLength (R3.5). When MaxSQLLength is unset/non-positive the default
//     of 1 MiB applies.
//   - Malformed: a message that frames correctly but whose payload cannot be
//     decoded (e.g. a Parse message missing its NUL-terminated strings).
//
// In both cases the parser skips the message, records the event through the
// metrics hook, and continues parsing subsequent messages on the same
// connection (R3.6). Skipping never advances the per-connection sequence number
// and never aborts the stream.
//
// Unrecognized message *type bytes* (including Greenplum private protocol
// extensions, R3.11) are NOT handled here: the framing layer already skips them
// using the 4-byte length field, and the EventBuilder simply returns no event
// for them WITHOUT recording a parse error. R3.11 explicitly states that an
// unrecognized message type is not a parse error.
package protocol

// defaultMaxSQLLength is the fallback maximum SQL length applied when the parser
// config does not configure one (R3.5): 1 mebibyte.
const defaultMaxSQLLength = 1 << 20

// ParseErrorHook is an optional, nil-safe callback invoked once each time the
// EventBuilder skips a malformed or oversized message (R3.6). It is the
// decoupling seam for the metrics layer: pkg/metrics depends on pkg/protocol,
// so the dependency cannot be inverted by importing metrics here. Injecting a
// hook keeps pkg/protocol free of any metrics import while still allowing the
// Collector.ParseError counter to be incremented. This mirrors the
// filter.ClassifiedHook pattern.
type ParseErrorHook func()

// effectiveMaxSQLLength returns the configured maximum SQL length in bytes, or
// the 1 MiB default when MaxSQLLength is unset or non-positive (R3.5).
func effectiveMaxSQLLength(cfg Config) int {
	if cfg.MaxSQLLength > 0 {
		return cfg.MaxSQLLength
	}
	return defaultMaxSQLLength
}
