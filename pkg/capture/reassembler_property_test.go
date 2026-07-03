package capture

import (
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Feature: pgshadow, Property 2: TCP reassembly preserves byte order across fragmentation and reordering
//
// Validates: Requirements 2.1, 2.2, 2.3
//
// For any byte stream split into TCP fragments with monotonic sequence numbers
// and then arbitrarily reordered, reassembling the fragments for a given ConnID
// yields a contiguous byte stream identical to the original concatenation in
// original order.
//
// This test is tag-free (no libpcap): it synthesizes packets with the gopacket
// layers package and drives them through NewReassembler, reusing the helpers in
// reassembler_test.go (buildPacket, segment, newRecordingHandler,
// canonicalConn, testServerPort).

// p2Fragment is one contiguous slice of the original stream with its TCP seq.
type p2Fragment struct {
	seq     uint32
	payload []byte
}

// p2lcg advances a 64-bit linear congruential generator; used for deterministic
// (shrinkable) pseudo-random choices derived from a generated integer seed.
func p2lcg(state uint64) uint64 {
	return state*6364136223846793005 + 1442695040888963407
}

// p2SplitOffsets derives a sorted, unique set of internal cut points for a
// stream of length n from a seed. The number of cuts ranges over [0, n-1], so
// the resulting fragment count spans single-segment through byte-per-segment
// cases (R2.3).
func p2SplitOffsets(n int, seed uint64) []int {
	if n < 2 {
		return nil
	}
	state := p2lcg(seed)
	numCuts := int(state >> 33 % uint64(n)) // 0..n-1
	if numCuts == 0 {
		return nil
	}
	seen := make(map[int]struct{}, numCuts)
	offsets := make([]int, 0, numCuts)
	for i := 0; i < numCuts; i++ {
		state = p2lcg(state)
		pos := 1 + int(state>>33%uint64(n-1)) // 1..n-1
		if _, ok := seen[pos]; ok {
			continue
		}
		seen[pos] = struct{}{}
		offsets = append(offsets, pos)
	}
	// insertion sort (small slices)
	for i := 1; i < len(offsets); i++ {
		for j := i; j > 0 && offsets[j-1] > offsets[j]; j-- {
			offsets[j-1], offsets[j] = offsets[j], offsets[j-1]
		}
	}
	return offsets
}

// p2BuildFragments cuts data at the derived offsets and assigns monotonic TCP
// sequence numbers starting from baseSeq.
func p2BuildFragments(data []byte, offsets []int, baseSeq uint32) []p2Fragment {
	bounds := make([]int, 0, len(offsets)+2)
	bounds = append(bounds, 0)
	bounds = append(bounds, offsets...)
	bounds = append(bounds, len(data))

	frags := make([]p2Fragment, 0, len(bounds)-1)
	for i := 0; i < len(bounds)-1; i++ {
		start, end := bounds[i], bounds[i+1]
		if start >= end {
			continue
		}
		frags = append(frags, p2Fragment{
			seq:     baseSeq + uint32(start),
			payload: data[start:end],
		})
	}
	return frags
}

// p2Permutation returns a deterministic permutation of [0, n) from a seed.
func p2Permutation(n int, seed uint64) []int {
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	state := p2lcg(seed)
	for i := n - 1; i > 0; i-- {
		state = p2lcg(state)
		j := int(state >> 33 % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}

func TestReassembler_Property2_ByteOrderPreserved(t *testing.T) {
	params := gopter.DefaultTestParameters()
	params.MinSuccessfulTests = 200
	properties := gopter.NewProperties(params)

	properties.Property("reassembly preserves byte order across fragmentation and reordering",
		prop.ForAll(func(data []byte, splitSeed uint32, permSeed uint32) bool {
			if len(data) == 0 {
				// Empty stream: nothing delivered; just assert no panic/no bytes.
				h := newRecordingHandler()
				r := NewReassembler(Config{Port: int(testServerPort)}, h)
				r.Assemble(buildPacket(t, segment{fromClient: true, seq: 1000, syn: true, ts: time.Now()}))
				r.Close()
				return len(h.clientTo[canonicalConn().Key()]) == 0
			}

			const baseSeq uint32 = 1000
			offsets := p2SplitOffsets(len(data), uint64(splitSeed))
			frags := p2BuildFragments(data, offsets, baseSeq+1)
			perm := p2Permutation(len(frags), uint64(permSeed))

			h := newRecordingHandler()
			r := NewReassembler(Config{Port: int(testServerPort)}, h)

			base := time.Now()
			// SYN establishes the client->server sequence baseline at baseSeq;
			// the first data byte is baseSeq+1.
			r.Assemble(buildPacket(t, segment{
				fromClient: true,
				seq:        baseSeq,
				syn:        true,
				ts:         base,
			}))

			// Feed fragments in shuffled order with monotonic arrival timestamps
			// so the idle machinery never flushes mid-stream.
			for k, idx := range perm {
				f := frags[idx]
				r.Assemble(buildPacket(t, segment{
					fromClient: true,
					seq:        f.seq,
					payload:    string(f.payload),
					ts:         base.Add(time.Duration(k+1) * time.Millisecond),
				}))
			}
			r.Close()

			got := h.clientTo[canonicalConn().Key()]
			if len(got) != len(data) {
				return false
			}
			for i := range data {
				if got[i] != data[i] {
					return false
				}
			}
			return true
		},
			gen.SliceOf(gen.UInt8Range(1, 255)),
			gen.UInt32(),
			gen.UInt32(),
		),
	)

	properties.TestingRun(t)
}
