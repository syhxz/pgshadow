package metrics

import (
	"errors"
	"testing"
	"time"
)

// threshold_test.go (task 11.3) asserts alert-condition gauges flip exactly at
// the documented boundaries. The collector fires alerts on strictly
// greater-than comparisons, so a metric sitting precisely at the threshold must
// NOT fire; only values above it fire. R10.2-R10.6.

// alertValue records a metric via fn, then reads the named alert gauge.
func alertValue(t *testing.T, c *collector, alertMetric string) float64 {
	t.Helper()
	return simpleValue(t, gather(t, c)[alertMetric])
}

// TestReplayLagThresholdBoundary: lag of exactly 60s does not fire; just above
// 60s fires (R10.2, ThresholdLagSeconds = 60).
func TestReplayLagThresholdBoundary(t *testing.T) {
	t.Run("at threshold not firing", func(t *testing.T) {
		c := newCollector(t)
		c.ReplayLag(ThresholdLagSeconds) // 60.0
		if v := alertValue(t, c, "pgshadow_alert_replay_lag"); v != 0 {
			t.Errorf("alert_replay_lag at %.3fs = %v, want 0", ThresholdLagSeconds, v)
		}
	})
	t.Run("just above threshold firing", func(t *testing.T) {
		c := newCollector(t)
		c.ReplayLag(ThresholdLagSeconds + 0.001) // 60.001
		if v := alertValue(t, c, "pgshadow_alert_replay_lag"); v != 1 {
			t.Errorf("alert_replay_lag at %.3fs = %v, want 1", ThresholdLagSeconds+0.001, v)
		}
	})
}

// TestQueueDepthThresholdBoundary: depth of exactly 100000 does not fire;
// 100001 fires (R10.3, ThresholdQueueDepth = 100000).
func TestQueueDepthThresholdBoundary(t *testing.T) {
	t.Run("at threshold not firing", func(t *testing.T) {
		c := newCollector(t)
		c.QueueDepth(ThresholdQueueDepth) // 100000
		if v := alertValue(t, c, "pgshadow_alert_queue_depth"); v != 0 {
			t.Errorf("alert_queue_depth at %d = %v, want 0", ThresholdQueueDepth, v)
		}
	})
	t.Run("just above threshold firing", func(t *testing.T) {
		c := newCollector(t)
		c.QueueDepth(ThresholdQueueDepth + 1) // 100001
		if v := alertValue(t, c, "pgshadow_alert_queue_depth"); v != 1 {
			t.Errorf("alert_queue_depth at %d = %v, want 1", ThresholdQueueDepth+1, v)
		}
	})
}

// TestErrorRateThresholdBoundary: a replay error rate of exactly 1% does not
// fire; a rate just above 1% fires (R10.4, ThresholdErrorRate = 0.01).
func TestErrorRateThresholdBoundary(t *testing.T) {
	// drive runs `total` replay attempts of which `errs` fail, yielding an
	// error rate of errs/total.
	drive := func(c *collector, errs, total int) {
		for i := 0; i < total; i++ {
			if i < errs {
				c.ReplayResult(errors.New("boom"))
			} else {
				c.ReplayResult(nil)
			}
		}
	}

	t.Run("at threshold not firing", func(t *testing.T) {
		c := newCollector(t)
		drive(c, 1, 100) // 1/100 = 0.01 == ThresholdErrorRate
		if v := simpleValue(t, gather(t, c)["pgshadow_replay_error_rate"]); v != ThresholdErrorRate {
			t.Fatalf("replay_error_rate = %v, want %v", v, ThresholdErrorRate)
		}
		if v := alertValue(t, c, "pgshadow_alert_error_rate"); v != 0 {
			t.Errorf("alert_error_rate at 1%% = %v, want 0", v)
		}
	})
	t.Run("just above threshold firing", func(t *testing.T) {
		c := newCollector(t)
		drive(c, 1, 99) // 1/99 ≈ 0.0101 > 0.01
		if v := alertValue(t, c, "pgshadow_alert_error_rate"); v != 1 {
			t.Errorf("alert_error_rate just above 1%% = %v, want 1", v)
		}
	})
}

// TestTargetP99ThresholdBoundary: target p99 exactly 2x the source p99 does not
// fire; target p99 above 2x fires (R10.5, ThresholdP99Ratio = 2.0). With a
// single sample per side the reservoir p99 equals that sample.
func TestTargetP99ThresholdBoundary(t *testing.T) {
	const source = 1 * time.Millisecond
	t.Run("at 2x not firing", func(t *testing.T) {
		c := newCollector(t)
		c.ExecTime("q", source, ThresholdP99Ratio*source) // target == 2x source
		if v := alertValue(t, c, "pgshadow_alert_target_p99"); v != 0 {
			t.Errorf("alert_target_p99 at exactly 2x = %v, want 0", v)
		}
	})
	t.Run("just above 2x firing", func(t *testing.T) {
		c := newCollector(t)
		c.ExecTime("q", source, ThresholdP99Ratio*source+time.Microsecond) // > 2x source
		if v := alertValue(t, c, "pgshadow_alert_target_p99"); v != 1 {
			t.Errorf("alert_target_p99 just above 2x = %v, want 1", v)
		}
	})
}

// TestPacketDropRateThresholdBoundary: a drop rate of exactly 0.1% does not
// fire; a rate just above 0.1% fires (R10.6, ThresholdDropRate = 0.001).
func TestPacketDropRateThresholdBoundary(t *testing.T) {
	t.Run("at threshold not firing", func(t *testing.T) {
		c := newCollector(t)
		c.PacketDrops(1000, 1) // 1/1000 = 0.001 == ThresholdDropRate
		if v := simpleValue(t, gather(t, c)["pgshadow_packet_drop_rate"]); v != ThresholdDropRate {
			t.Fatalf("packet_drop_rate = %v, want %v", v, ThresholdDropRate)
		}
		if v := alertValue(t, c, "pgshadow_alert_packet_drop_rate"); v != 0 {
			t.Errorf("alert_packet_drop_rate at 0.1%% = %v, want 0", v)
		}
	})
	t.Run("just above threshold firing", func(t *testing.T) {
		c := newCollector(t)
		c.PacketDrops(1000, 2) // 2/1000 = 0.002 > 0.001
		if v := alertValue(t, c, "pgshadow_alert_packet_drop_rate"); v != 1 {
			t.Errorf("alert_packet_drop_rate just above 0.1%% = %v, want 1", v)
		}
	})
}
