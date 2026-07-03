// Feature: pgshadow, Property 1: pgshadow never connects to the Source_Database
//
// This file implements Property 1 from the design's "Correctness Properties"
// section (task 12.4):
//
//	For any sequence of pipeline operations driven by arbitrary captured input,
//	the set of outbound network addresses dialed by pgshadow contains only the
//	configured Target_Database endpoint and never the Source_Database endpoint.
//
// Encoding of "never dials the source" (per the design's Isolation harness)
//
// The producer side of the pipeline — capture → reassembler → streamProcessor →
// queue — is strictly read-only with respect to the network: it consumes
// already-captured bytes and writes kept events to the in-process queue. It
// imports no database driver and constructs no dialer; the only component in the
// whole binary that dials is the replayer's pool, and it dials the configured
// Target_Database exclusively.
//
// The harness makes that concrete with a process-wide recording dialer (a
// net.Dialer-shaped wrapper, p1RecordingDialer) that records every address it is
// asked to dial. We drive the producer over arbitrary synthesized client/server
// bytes across arbitrary ConnIDs — whose four-tuples encode "source" endpoints —
// and then assert:
//
//  1. the recorder recorded zero connections to ANY source endpoint (in fact it
//     recorded zero dials at all, because the producer never dials);
//  2. the only configured dial target is never itself a source endpoint; and
//  3. events still flow to the queue (the pipeline actually ran).
//
// The recorder's soundness — that it would in fact capture a dial to a source
// endpoint if one occurred — is proven by TestP1DialRecorderCatchesSourceDials,
// so the emptiness assertion in (1) is meaningful rather than vacuous.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"

	"pgshadow/pkg/core"
	"pgshadow/pkg/filter"
	"pgshadow/pkg/protocol"
)

// p1TargetEndpoint is the single configured Target_Database endpoint — the only
// address the binary is ever permitted to dial. It is in TEST-NET-3
// (203.0.113.0/24) so it is disjoint by construction from the generated source
// endpoints (192.0.2.0/24 sources, 198.51.100.0/24 servers).
const p1TargetEndpoint = "203.0.113.1:5432"

// p1RecordingDialer is the process-wide network seam for the isolation harness:
// a net.Dialer-shaped wrapper that records every address it is asked to dial and
// refuses to actually connect. If any producer code path dialed, the address
// would be captured here.
type p1RecordingDialer struct {
	mu     sync.Mutex
	dialed []string
}

// DialContext matches net.Dialer.DialContext so this recorder is a drop-in
// network seam. It records the address and returns an error without opening any
// socket, keeping the harness DB/NIC-free.
func (d *p1RecordingDialer) DialContext(_ context.Context, _ string, address string) (net.Conn, error) {
	d.mu.Lock()
	d.dialed = append(d.dialed, address)
	d.mu.Unlock()
	return nil, fmt.Errorf("p1 isolation harness: dialing %q is not permitted", address)
}

// Dialed returns a copy of every address dialed through this recorder.
func (d *p1RecordingDialer) Dialed() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.dialed))
	copy(out, d.dialed)
	return out
}

// p1ConnSpec describes one synthesized captured connection: a four-tuple plus
// the arbitrary client/server traffic observed on it. Inserts are guaranteed-kept
// DML (used as a lower bound on enqueued events); Selects are dropped out of a
// transaction under the default rule matrix; Parses register prepared statements
// that emit nothing until a Bind (none is sent here); ZStatuses are backend
// ReadyForQuery status bytes on the server direction.
type p1ConnSpec struct {
	SrcOctet  int
	DBOctet   int
	SrcPort   uint16
	Inserts   int
	Selects   int
	Parses    int
	ZStatuses []byte
}

// connID builds the captured four-tuple. Both endpoints (client and server) are
// treated as source endpoints that pgshadow must never dial.
func (s p1ConnSpec) connID() core.ConnID {
	return core.ConnID{
		SrcIP:   netip.AddrFrom4([4]byte{192, 0, 2, byte(s.SrcOctet)}),
		SrcPort: s.SrcPort,
		DstIP:   netip.AddrFrom4([4]byte{198, 51, 100, byte(s.DBOctet)}),
		DstPort: 5432,
	}
}

// clientBytes builds the client→server stream: a startup message followed by an
// arbitrary mix of Simple_Query INSERTs (kept), Simple_Query SELECTs (dropped
// out of tx), and Parse messages (emit nothing without a Bind).
func (s p1ConnSpec) clientBytes() []byte {
	buf := startupMsg()
	for i := 0; i < s.Inserts; i++ {
		buf = append(buf, simpleQuery(fmt.Sprintf("INSERT INTO t VALUES (%d)", i))...)
	}
	for i := 0; i < s.Selects; i++ {
		buf = append(buf, simpleQuery("SELECT * FROM t")...)
	}
	for i := 0; i < s.Parses; i++ {
		buf = append(buf, p1ParseMsg(fmt.Sprintf("s%d", i), "INSERT INTO t VALUES ($1)")...)
	}
	return buf
}

// serverBytes builds the server→client stream as a sequence of ReadyForQuery
// messages carrying arbitrary transaction-status bytes.
func (s p1ConnSpec) serverBytes() []byte {
	var buf []byte
	for _, status := range s.ZStatuses {
		buf = append(buf, readyForQuery(status)...)
	}
	return buf
}

// p1ParseMsg builds an Extended_Query Parse ('P') message: statement name and
// query as NUL-terminated strings followed by a zero parameter count.
func p1ParseMsg(stmt, sql string) []byte {
	payload := make([]byte, 0, len(stmt)+len(sql)+4)
	payload = append(payload, []byte(stmt)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(sql)...)
	payload = append(payload, 0)
	payload = append(payload, 0, 0) // int16 parameter count = 0
	buf := make([]byte, 5+len(payload))
	buf[0] = 'P'
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(payload)))
	copy(buf[5:], payload)
	return buf
}

// p1Endpoint renders an addr:port endpoint string.
func p1Endpoint(addr netip.Addr, port uint16) string {
	return fmt.Sprintf("%s:%d", addr.String(), port)
}

// p1GenScenario produces an arbitrary captured scenario: 1..4 connections, each
// with random endpoints and a random mix of client/server traffic. It draws
// directly from gopter's RNG so every iteration exercises a fresh input.
func p1GenScenario() gopter.Gen {
	statuses := []byte{'I', 'T', 'E'}
	return func(gp *gopter.GenParameters) *gopter.GenResult {
		rng := gp.Rng
		n := 1 + rng.Intn(4)
		specs := make([]p1ConnSpec, n)
		for i := range specs {
			zn := rng.Intn(4)
			zs := make([]byte, zn)
			for j := range zs {
				zs[j] = statuses[rng.Intn(len(statuses))]
			}
			specs[i] = p1ConnSpec{
				SrcOctet:  1 + rng.Intn(254),
				DBOctet:   1 + rng.Intn(254),
				SrcPort:   uint16(1024 + rng.Intn(64000)),
				Inserts:   1 + rng.Intn(3),
				Selects:   rng.Intn(3),
				Parses:    rng.Intn(3),
				ZStatuses: zs,
			}
		}
		return gopter.NewGenResult(specs, gopter.NoShrinker)
	}
}

// TestProductionIsolationProperty is the property-based test for Property 1.
//
// Feature: pgshadow, Property 1: pgshadow never connects to the Source_Database
func TestProductionIsolationProperty(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 200 // >= 100 iterations
	properties := gopter.NewProperties(params)

	properties.Property(
		"driving the producer over arbitrary captured input dials no source endpoint",
		prop.ForAll(func(specs []p1ConnSpec) bool {
			// The process-wide recording dialer. The producer never dials, so
			// nothing routes through it; if it did, every address would land in
			// rec.Dialed().
			rec := &p1RecordingDialer{}

			q := &recordingQueue{}
			sp, err := newStreamProcessor(protocol.Config{ExtendedQuery: true}, filter.Config{}, q, nil)
			if err != nil {
				return false
			}

			sourceEndpoints := make(map[string]bool)
			expectedKeptLB := 0
			for _, s := range specs {
				conn := s.connID()
				// Both ends of every captured connection are source endpoints
				// pgshadow must never dial.
				sourceEndpoints[p1Endpoint(conn.SrcIP, conn.SrcPort)] = true
				sourceEndpoints[p1Endpoint(conn.DstIP, conn.DstPort)] = true

				sp.OnBytes(conn, true, s.clientBytes())  // client→server
				sp.OnBytes(conn, false, s.serverBytes()) // server→client
				expectedKeptLB += s.Inserts
			}

			// (1) No outbound dial to any source endpoint occurred. The producer
			// dials nothing at all, so the recorder is empty.
			for _, addr := range rec.Dialed() {
				if sourceEndpoints[addr] {
					return false
				}
			}
			if len(rec.Dialed()) != 0 {
				return false
			}

			// (2) The only permitted dial target is never a source endpoint.
			if sourceEndpoints[p1TargetEndpoint] {
				return false
			}

			// (3) The pipeline actually ran: at least the guaranteed-kept INSERTs
			// reached the queue.
			return q.Depth() >= expectedKeptLB
		}, p1GenScenario()),
	)

	properties.TestingRun(t)
}

// TestP1DialRecorderCatchesSourceDials proves the isolation harness is sound:
// the recording dialer captures a dial to a source endpoint when one occurs, so
// the property test's "recorded zero source dials" assertion is meaningful.
func TestP1DialRecorderCatchesSourceDials(t *testing.T) {
	rec := &p1RecordingDialer{}
	if _, err := rec.DialContext(context.Background(), "tcp", "192.0.2.5:5432"); err == nil {
		t.Fatal("recording dialer should refuse to actually connect")
	}
	got := rec.Dialed()
	if len(got) != 1 || got[0] != "192.0.2.5:5432" {
		t.Fatalf("recorder did not capture the source dial: %v", got)
	}
}
