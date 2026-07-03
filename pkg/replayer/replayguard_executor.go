package replayer

import (
	"context"
	"sync/atomic"

	"pgshadow/pkg/core"
	"pgshadow/pkg/replayguard"
)

// replayguardExecutor is an Executor decorator that applies safeguard rules
// before delegating to the next executor. Events that are blocked are silently
// dropped; events that are rewritten have their SQL modified in place before
// reaching the target database.
//
// This decorator sits outermost in the execution chain so that blocked events
// are dropped immediately without consuming pacing delay or rate-limit tokens.
//
// Execution chain (outermost → innermost):
//   safeguard → pacing → rate-limit → error-policy → dialect → poolExecutor
type replayguardExecutor struct {
	next      Executor
	sg        *replayguard.Guard
	onDecision func(replayguard.Decision) // optional callback for metrics

	// Atomic counters for observability
	blocked   uint64
	allowed   uint64
	rewritten uint64
}

// newReplayguardExecutor wraps next with safeguard filtering and rewriting.
// If sg is nil, it returns next unchanged (no-op passthrough).
// The optional onDecision callback is invoked for every event with its decision.
func newReplayguardExecutor(next Executor, sg *replayguard.Guard, onDecision ...func(replayguard.Decision)) Executor {
	if sg == nil {
		return next
	}
	var cb func(replayguard.Decision)
	if len(onDecision) > 0 {
		cb = onDecision[0]
	}
	return &replayguardExecutor{next: next, sg: sg, onDecision: cb}
}

// Exec applies the replayguard. Blocked events return nil (silently dropped).
// Rewritten events proceed with modified SQL. Allowed events pass through.
func (s *replayguardExecutor) Exec(ctx context.Context, conn core.ConnID, ev *core.SQLEvent) error {
	decision := s.sg.Apply(ev)

	if s.onDecision != nil {
		s.onDecision(decision)
	}

	switch decision {
	case replayguard.Block:
		atomic.AddUint64(&s.blocked, 1)
		return nil // silently drop
	case replayguard.Rewrite:
		atomic.AddUint64(&s.rewritten, 1)
		return s.next.Exec(ctx, conn, ev)
	default: // Allow
		atomic.AddUint64(&s.allowed, 1)
		return s.next.Exec(ctx, conn, ev)
	}
}

// ReplayguardStats returns the safeguard executor's counters.
type ReplayguardStats struct {
	Blocked   uint64
	Allowed   uint64
	Rewritten uint64
}

// Stats returns the safeguard executor's decision counters.
func (s *replayguardExecutor) Stats() ReplayguardStats {
	return ReplayguardStats{
		Blocked:   atomic.LoadUint64(&s.blocked),
		Allowed:   atomic.LoadUint64(&s.allowed),
		Rewritten: atomic.LoadUint64(&s.rewritten),
	}
}
