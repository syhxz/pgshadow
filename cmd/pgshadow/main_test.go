package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/gopacket"
	"github.com/jackc/pgx/v5/pgxpool"

	"pgshadow/pkg/capture"
	"pgshadow/pkg/config"
	"pgshadow/pkg/core"
	"pgshadow/pkg/metrics"
	"pgshadow/pkg/queue"
	"pgshadow/pkg/replayer"
	"pgshadow/pkg/replayguard"
)

// --- test doubles ------------------------------------------------------------

// fakeCapture is a no-op capture.Source recording whether it was closed.
type fakeCapture struct{ closed bool }

func (f *fakeCapture) Packets() <-chan gopacket.Packet { return nil }
func (f *fakeCapture) Stats() (capture.CaptureStats, error) {
	return capture.CaptureStats{}, nil
}
func (f *fakeCapture) Close() error { f.closed = true; return nil }

// fakeQueue is a no-op queue.Queue recording whether it was closed.
type fakeQueue struct{ closed bool }

func (q *fakeQueue) Enqueue(*core.SQLEvent) bool { return false }
func (q *fakeQueue) Dequeue(context.Context) (*core.SQLEvent, bool) {
	return nil, false
}
func (q *fakeQueue) Depth() int   { return 0 }
func (q *fakeQueue) Close() error { q.closed = true; return nil }

// fakePool is a no-op replayer.Pool recording whether it was closed.
type fakePool struct{ closed bool }

func (p *fakePool) AcquireFor(context.Context, core.ConnID) (*pgxpool.Conn, error) {
	return nil, nil
}
func (p *fakePool) Release(core.ConnID)       {}
func (p *fakePool) Stats() replayer.PoolStats { return replayer.PoolStats{} }
func (p *fakePool) Close()                    { p.closed = true }

// fakeReplayer is a no-op replayer.Replayer.
type fakeReplayer struct{}

func (fakeReplayer) Run(context.Context) error { return nil }

// recordingDeps builds a deps whose constructors all succeed and append their
// step name to *order as they run, so a test can assert the startup ordering
// and that a failing step short-circuits the rest.
func recordingDeps(order *[]string) deps {
	cfg := &config.Config{}
	return deps{
		loadConfig: func(string) (*config.Config, error) {
			*order = append(*order, "config")
			return cfg, nil
		},
		newMetrics: func(metrics.Config) metrics.Collector {
			*order = append(*order, "metrics")
			return metrics.New(metrics.Config{})
		},
		newErrorExporter: func(metrics.Config) (metrics.ErrorExporter, error) {
			*order = append(*order, "errorexporter")
			return metrics.NewErrorExporter(metrics.Config{})
		},
		openCapture: func(capture.Config) (capture.Source, error) {
			*order = append(*order, "capture")
			return &fakeCapture{}, nil
		},
		newQueue: func(queue.Config, *metrics.Collector) (queue.Queue, error) {
			*order = append(*order, "queue")
			return &fakeQueue{}, nil
		},
		newPool: func(context.Context, replayer.Config) (replayer.Pool, error) {
			*order = append(*order, "pool")
			return &fakePool{}, nil
		},
		newReplayer: func(_ replayer.Config, _ queue.Queue, _ replayer.Pool, _ metrics.Collector, _ *replayguard.Guard) replayer.Replayer {
			*order = append(*order, "replayer")
			return fakeReplayer{}
		},
	}
}

// --- tests -------------------------------------------------------------------

// TestStartupOrdering asserts the ordered fail-fast sequence runs every step
// exactly once and in the documented order on a fully-successful startup.
func TestStartupOrdering(t *testing.T) {
	var order []string
	c, err := startup(context.Background(), "ignored.yaml", recordingDeps(&order))
	if err != nil {
		t.Fatalf("startup: unexpected error: %v", err)
	}
	defer c.Close()

	want := []string{"config", "metrics", "errorexporter", "capture", "queue", "pool", "replayer"}
	if strings.Join(order, ",") != strings.Join(want, ",") {
		t.Fatalf("startup order = %v, want %v", order, want)
	}
}

// TestStartupFailFast asserts that when a given step fails, no later step runs
// and the error is descriptive (names the failing stage).
func TestStartupFailFast(t *testing.T) {
	cases := []struct {
		name      string
		fail      string // step that should fail
		wantAfter []string
		wantMsg   string
	}{
		{"config", "config", nil, "configuration"},
		{"metrics", "errorexporter", []string{"config", "metrics"}, "metrics"},
		{"capture", "capture", []string{"config", "metrics", "errorexporter"}, "capture"},
		{"queue", "queue", []string{"config", "metrics", "errorexporter", "capture"}, "queue"},
		{"pool", "pool", []string{"config", "metrics", "errorexporter", "capture", "queue"}, "target pool"},
	}

	sentinel := errors.New("boom")

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			d := recordingDeps(&order)
			injectFailure(&d, tc.fail, sentinel)

			c, err := startup(context.Background(), "ignored.yaml", d)
			if err == nil {
				if c != nil {
					c.Close()
				}
				t.Fatalf("expected startup to fail at %q", tc.fail)
			}
			if !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want it to wrap sentinel", err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention failing stage %q", err.Error(), tc.wantMsg)
			}
			// The failing step runs (and appends its name) but nothing after it.
			gotSteps := append([]string{}, order...)
			if tc.wantAfter != nil && strings.Join(gotSteps, ",") != strings.Join(tc.wantAfter, ",") {
				t.Fatalf("steps run = %v, want %v", gotSteps, tc.wantAfter)
			}
		})
	}
}

// injectFailure replaces the named step's constructor with one that records the
// step then returns err, leaving all other steps intact.
func injectFailure(d *deps, step string, err error) {
	switch step {
	case "config":
		d.loadConfig = func(string) (*config.Config, error) { return nil, err }
	case "errorexporter":
		d.newErrorExporter = func(metrics.Config) (metrics.ErrorExporter, error) {
			return nil, err
		}
	case "capture":
		d.openCapture = func(capture.Config) (capture.Source, error) { return nil, err }
	case "queue":
		d.newQueue = func(queue.Config, *metrics.Collector) (queue.Queue, error) {
			return nil, err
		}
	case "pool":
		d.newPool = func(context.Context, replayer.Config) (replayer.Pool, error) {
			return nil, err
		}
	}
}

// TestStartupClosesOpenedResourcesOnFailure asserts a step failing after
// earlier steps opened resources tears those resources back down (no leaks).
func TestStartupClosesOpenedResourcesOnFailure(t *testing.T) {
	var order []string
	d := recordingDeps(&order)

	capt := &fakeCapture{}
	q := &fakeQueue{}
	d.openCapture = func(capture.Config) (capture.Source, error) {
		order = append(order, "capture")
		return capt, nil
	}
	d.newQueue = func(queue.Config, *metrics.Collector) (queue.Queue, error) {
		order = append(order, "queue")
		return q, nil
	}
	// Fail at the pool step, after capture and queue are open.
	d.newPool = func(context.Context, replayer.Config) (replayer.Pool, error) {
		order = append(order, "pool")
		return nil, errors.New("pool down")
	}

	if _, err := startup(context.Background(), "ignored.yaml", d); err == nil {
		t.Fatal("expected pool failure")
	}
	if !capt.closed {
		t.Error("capture handle was not closed on partial-startup failure")
	}
	if !q.closed {
		t.Error("queue was not closed on partial-startup failure")
	}
}

// TestRunInvalidConfigPath exercises the production config loader: a missing
// config file must fail startup with a descriptive error (R11.9), without a
// live NIC or database.
func TestRunInvalidConfigPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")
	err := run(context.Background(), []string{"--config", missing}, io.Discard, productionDeps())
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
	if !strings.Contains(err.Error(), "configuration") {
		t.Fatalf("error %q should mention configuration stage", err.Error())
	}
}
