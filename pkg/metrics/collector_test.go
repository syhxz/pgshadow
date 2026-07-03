package metrics

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"pgshadow/pkg/filter"
)

// gather collects all metric families from the collector's registry, keyed by
// metric name, so tests can assert on registration and recorded values.
func gather(t *testing.T, c *collector) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := c.reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func newCollector(t *testing.T) *collector {
	t.Helper()
	c, ok := New(Config{}).(*collector)
	if !ok {
		t.Fatalf("New did not return *collector")
	}
	return c
}

// simpleValue returns the single counter/gauge value of a metric family with no
// labels (or the first series otherwise).
func simpleValue(t *testing.T, mf *dto.MetricFamily) float64 {
	t.Helper()
	if mf == nil || len(mf.Metric) == 0 {
		t.Fatalf("metric family missing or empty")
	}
	m := mf.Metric[0]
	switch {
	case m.Counter != nil:
		return m.Counter.GetValue()
	case m.Gauge != nil:
		return m.Gauge.GetValue()
	default:
		t.Fatalf("metric %q is neither counter nor gauge", mf.GetName())
		return 0
	}
}

// TestCatalogRegistered asserts the full metric catalog is registered.
func TestCatalogRegistered(t *testing.T) {
	c := newCollector(t)
	got := gather(t, c)

	// Gather only emits families that have at least one observation, so seed
	// every metric once.
	c.PacketDrops(1000, 1)
	c.ParseError()
	c.Classified(filter.ClassDML, true)
	c.QueueDepth(5)
	c.Overflow()
	c.ReplayLag(1)
	c.ReplayResult(nil)
	c.ExecTime("select 1", 1*time.Millisecond, 2*time.Millisecond)
	c.Throughput(10, 9)
	got = gather(t, c)

	want := []string{
		"pgshadow_packets_received_total",
		"pgshadow_packets_dropped_total",
		"pgshadow_parse_errors_total",
		"pgshadow_classified_total",
		"pgshadow_queue_overflow_total",
		"pgshadow_replay_total",
		"pgshadow_replay_errors_total",
		"pgshadow_queue_depth",
		"pgshadow_replay_lag_seconds",
		"pgshadow_replay_error_rate",
		"pgshadow_packet_drop_rate",
		"pgshadow_source_qps",
		"pgshadow_target_qps",
		"pgshadow_source_exec_p99_seconds",
		"pgshadow_target_exec_p99_seconds",
		"pgshadow_alert_replay_lag",
		"pgshadow_alert_queue_depth",
		"pgshadow_alert_error_rate",
		"pgshadow_alert_target_p99",
		"pgshadow_alert_packet_drop_rate",
		"pgshadow_exec_time_seconds",
	}
	for _, name := range want {
		if _, ok := got[name]; !ok {
			t.Errorf("metric %q not registered/exposed", name)
		}
	}
}

func TestCountersIncrement(t *testing.T) {
	c := newCollector(t)
	c.ParseError()
	c.ParseError()
	c.Overflow()

	got := gather(t, c)
	if v := simpleValue(t, got["pgshadow_parse_errors_total"]); v != 2 {
		t.Errorf("parse_errors_total = %v, want 2", v)
	}
	if v := simpleValue(t, got["pgshadow_queue_overflow_total"]); v != 1 {
		t.Errorf("queue_overflow_total = %v, want 1", v)
	}
}

func TestPacketDropsCumulativeDeltas(t *testing.T) {
	c := newCollector(t)
	c.PacketDrops(100, 2)
	c.PacketDrops(250, 5) // cumulative totals advance

	got := gather(t, c)
	if v := simpleValue(t, got["pgshadow_packets_received_total"]); v != 250 {
		t.Errorf("packets_received_total = %v, want 250", v)
	}
	if v := simpleValue(t, got["pgshadow_packets_dropped_total"]); v != 5 {
		t.Errorf("packets_dropped_total = %v, want 5", v)
	}
	if v := simpleValue(t, got["pgshadow_packet_drop_rate"]); v != 5.0/250.0 {
		t.Errorf("packet_drop_rate = %v, want %v", v, 5.0/250.0)
	}
}

func TestClassifiedLabels(t *testing.T) {
	c := newCollector(t)
	c.Classified(filter.ClassDML, true)
	c.Classified(filter.ClassDML, true)
	c.Classified(filter.ClassPlainSelect, false)

	mf := gather(t, c)["pgshadow_classified_total"]
	if mf == nil {
		t.Fatal("classified_total not registered")
	}
	values := map[string]float64{}
	for _, m := range mf.Metric {
		key := ""
		for _, l := range m.Label {
			key += l.GetName() + "=" + l.GetValue() + ";"
		}
		values[key] = m.Counter.GetValue()
	}
	if v := values["class=dml;kept=true;"]; v != 2 {
		t.Errorf("dml/kept=true count = %v, want 2", v)
	}
	if v := values["class=plain_select;kept=false;"]; v != 1 {
		t.Errorf("plain_select/kept=false count = %v, want 1", v)
	}
}

func TestGaugesAndAlerts(t *testing.T) {
	c := newCollector(t)

	// Below thresholds -> alerts clear.
	c.QueueDepth(10)
	c.ReplayLag(5)
	if v := simpleValue(t, gather(t, c)["pgshadow_alert_queue_depth"]); v != 0 {
		t.Errorf("alert_queue_depth = %v, want 0", v)
	}
	if v := simpleValue(t, gather(t, c)["pgshadow_alert_replay_lag"]); v != 0 {
		t.Errorf("alert_replay_lag = %v, want 0", v)
	}

	// Above thresholds -> alerts fire.
	c.QueueDepth(ThresholdQueueDepth + 1)
	c.ReplayLag(ThresholdLagSeconds + 1)
	if v := simpleValue(t, gather(t, c)["pgshadow_alert_queue_depth"]); v != 1 {
		t.Errorf("alert_queue_depth = %v, want 1", v)
	}
	if v := simpleValue(t, gather(t, c)["pgshadow_alert_replay_lag"]); v != 1 {
		t.Errorf("alert_replay_lag = %v, want 1", v)
	}
}

func TestReplayResultErrorRate(t *testing.T) {
	c := newCollector(t)
	// 1 error out of 4 = 25% > 1% -> alert fires.
	c.ReplayResult(nil)
	c.ReplayResult(nil)
	c.ReplayResult(nil)
	c.ReplayResult(errors.New("boom"))

	got := gather(t, c)
	if v := simpleValue(t, got["pgshadow_replay_total"]); v != 4 {
		t.Errorf("replay_total = %v, want 4", v)
	}
	if v := simpleValue(t, got["pgshadow_replay_errors_total"]); v != 1 {
		t.Errorf("replay_errors_total = %v, want 1", v)
	}
	if v := simpleValue(t, got["pgshadow_replay_error_rate"]); v != 0.25 {
		t.Errorf("replay_error_rate = %v, want 0.25", v)
	}
	if v := simpleValue(t, got["pgshadow_alert_error_rate"]); v != 1 {
		t.Errorf("alert_error_rate = %v, want 1", v)
	}
}

func TestExecTimeObservesBothSides(t *testing.T) {
	c := newCollector(t)
	c.ExecTime("q", 1*time.Millisecond, 3*time.Millisecond)

	mf := gather(t, c)["pgshadow_exec_time_seconds"]
	if mf == nil {
		t.Fatal("exec_time_seconds not registered")
	}
	counts := map[string]uint64{}
	for _, m := range mf.Metric {
		for _, l := range m.Label {
			if l.GetName() == "side" {
				counts[l.GetValue()] = m.Histogram.GetSampleCount()
			}
		}
	}
	if counts["source"] != 1 {
		t.Errorf("source observations = %d, want 1", counts["source"])
	}
	if counts["target"] != 1 {
		t.Errorf("target observations = %d, want 1", counts["target"])
	}
}

func TestThroughputGauges(t *testing.T) {
	c := newCollector(t)
	c.Throughput(123, 45)
	got := gather(t, c)
	if v := simpleValue(t, got["pgshadow_source_qps"]); v != 123 {
		t.Errorf("source_qps = %v, want 123", v)
	}
	if v := simpleValue(t, got["pgshadow_target_qps"]); v != 45 {
		t.Errorf("target_qps = %v, want 45", v)
	}
}

// TestMetricsEndpoint asserts the /metrics handler serves the exposition.
func TestMetricsEndpoint(t *testing.T) {
	c := newCollector(t)
	c.ParseError()

	srv := httptest.NewServer(c.handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "pgshadow_parse_errors_total") {
		t.Errorf("/metrics output missing pgshadow_parse_errors_total")
	}
}

func TestReservoirQuantile(t *testing.T) {
	r := newReservoir(10)
	if q := r.quantile(0.99); q != 0 {
		t.Errorf("empty reservoir quantile = %v, want 0", q)
	}
	for i := 1; i <= 10; i++ {
		r.add(float64(i))
	}
	if q := r.quantile(0.99); q != 10 {
		t.Errorf("p99 of 1..10 = %v, want 10", q)
	}
	if q := r.quantile(0); q != 1 {
		t.Errorf("min = %v, want 1", q)
	}
}
