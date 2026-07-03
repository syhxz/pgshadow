package queue

// kafka_integration_test.go is the integration test for the external-Kafka
// backend (task 7.6). It exercises a *real* produce→consume round trip against
// a live broker to assert per-ConnID partition-key ordering (R6.7), and it
// re-confirms the startup fail-fast on unreachable brokers (R6.10).
//
// Gating: the produce→consume portion requires a reachable Kafka broker, which
// is not available in CI or the default `go test ./pkg/queue/...` environment.
// The test reads the broker list from the PGSHADOW_KAFKA_BROKERS environment
// variable (comma-separated) and calls t.Skip() when it is unset, so the
// default run stays green by skipping cleanly. To run it against a real broker:
//
//	PGSHADOW_KAFKA_BROKERS=localhost:9092 go test ./pkg/queue/... -run Integration
//
// The unreachable-broker assertion (TestKafkaIntegrationFailFastUnreachable)
// needs no broker and always runs: it points the backend at a closed address
// and asserts construction fails, naming the broker.
//
// This file reuses helpers already defined in kafkaqueue_test.go (same package):
// closedAddr and shortDialTimeout. They are NOT redeclared here.

import (
	"context"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"pgshadow/pkg/core"
)

// kafkaBrokersFromEnv returns the configured integration brokers, or skips the
// test when PGSHADOW_KAFKA_BROKERS is unset/empty. This is the gate that keeps
// the default `go test ./pkg/queue/...` green when no broker is available.
func kafkaBrokersFromEnv(t *testing.T) []string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("PGSHADOW_KAFKA_BROKERS"))
	if raw == "" {
		t.Skip("PGSHADOW_KAFKA_BROKERS not set; skipping Kafka integration test (no broker available)")
	}
	var brokers []string
	for _, b := range strings.Split(raw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			brokers = append(brokers, b)
		}
	}
	if len(brokers) == 0 {
		t.Skip("PGSHADOW_KAFKA_BROKERS contained no usable broker addresses; skipping")
	}
	return brokers
}

// createIntegrationTopic creates a multi-partition topic so that distinct
// ConnIDs can hash to different partitions, making the per-ConnID ordering
// assertion meaningful (with a single partition everything would be trivially
// ordered). It is best-effort: if the topic already exists the broker returns a
// benign error which we tolerate.
func createIntegrationTopic(t *testing.T, broker, topic string, partitions int) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("dial broker %q: %v", broker, err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("get controller: %v", err)
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("dial controller: %v", err)
	}
	defer controllerConn.Close()

	if err := controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	}); err != nil {
		// Topic-already-exists is fine; surface anything else for visibility but
		// don't fail — auto-creation may also have produced the topic.
		t.Logf("CreateTopics(%q) returned: %v (continuing)", topic, err)
	}
}

// TestKafkaIntegrationConnIDOrdering produces several events across multiple
// ConnIDs and asserts that, on consume, each ConnID's events arrive in the same
// order they were enqueued. Because the producer keys each message by the
// ConnID and uses a hash balancer, all events from one connection land in a
// single partition and therefore preserve order (R6.7).
func TestKafkaIntegrationConnIDOrdering(t *testing.T) {
	brokers := kafkaBrokersFromEnv(t)

	// A unique topic per run gives the fixed consumer group a partition with no
	// committed offset, so the reader starts at the first offset and sees every
	// message we produce in this test.
	topic := "pgshadow-it-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	// Ensure the topic exists before constructing the queue. On KRaft-mode
	// brokers, auto-create via the writer's produce retries handles this.
	// We use a temporary queue to produce a dummy event which triggers
	// topic auto-creation, then close it and create the real queue.
	tmpQ, err := New(Config{Type: "kafka", Kafka: KafkaConfig{
		Brokers: brokers,
		Topic:   topic,
	}}, nil)
	if err != nil {
		t.Fatalf("New(type=kafka) for topic bootstrap: %v", err)
	}
	sentinel := &core.SQLEvent{
		Conn: mkConn("0.0.0.0", 0),
		Seq:  0,
	}
	if dropped := tmpQ.Enqueue(sentinel); dropped {
		tmpQ.Close()
		t.Fatalf("failed to bootstrap topic %q: sentinel dropped", topic)
	}
	tmpQ.Close()
	// Wait for the topic metadata to fully propagate to all partitions.
	time.Sleep(2 * time.Second)

	q, err := New(Config{Type: "kafka", Kafka: KafkaConfig{
		Brokers: brokers,
		Topic:   topic,
	}}, nil)
	if err != nil {
		t.Fatalf("New(type=kafka) against live broker(s) %v: %v", brokers, err)
	}
	t.Cleanup(func() { _ = q.Close() })

	// Three distinct connections, each with several events carrying a strictly
	// increasing Seq so we can verify arrival order per connection.
	conns := []core.ConnID{
		mkConn("10.0.0.10", 50001),
		mkConn("10.0.0.11", 50002),
		mkConn("10.0.0.12", 50003),
	}
	const perConn = 5

	want := make(map[string][]uint64, len(conns))
	for _, c := range conns {
		for i := 0; i < perConn; i++ {
			seq := uint64(i + 1)
			ev := &core.SQLEvent{
				Conn: c,
				SQL:  "SELECT " + strconv.Itoa(i),
				Seq:  seq,
			}
			if dropped := q.Enqueue(ev); dropped {
				t.Fatalf("Enqueue reported dropped for conn %s seq %d", c.Key(), seq)
			}
			want[c.Key()] = append(want[c.Key()], seq)
		}
	}

	total := len(conns) * perConn
	got := make(map[string][]uint64, len(conns))

	// Bound the whole consume phase: the group reader needs a moment to join and
	// fetch, so allow generous time but never block forever.
	deadline := time.Now().Add(60 * time.Second)
	for received := 0; received < total; {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out: received %d of %d events; got=%v", received, total, got)
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		ev, ok := q.Dequeue(ctx)
		cancel()
		if !ok {
			t.Fatalf("Dequeue returned ok=false after %d of %d events; got=%v", received, total, got)
		}
		// Skip any internal/setup messages that may have been produced during
		// topic auto-creation.
		if ev.SQL == "" {
			continue
		}
		got[ev.Conn.Key()] = append(got[ev.Conn.Key()], ev.Seq)
		received++
	}

	// Per-ConnID ordering (R6.7): each connection's events must arrive in the
	// exact order they were enqueued. Ordering ACROSS connections is not
	// guaranteed (they may live in different partitions), so we assert per key.
	for _, c := range conns {
		key := c.Key()
		gotSeqs := got[key]
		wantSeqs := want[key]
		if len(gotSeqs) != len(wantSeqs) {
			t.Fatalf("conn %s: got %d events %v, want %d %v", key, len(gotSeqs), gotSeqs, len(wantSeqs), wantSeqs)
		}
		for i := range wantSeqs {
			if gotSeqs[i] != wantSeqs[i] {
				t.Fatalf("conn %s: out-of-order at index %d: got seq %d, want %d (got=%v want=%v)",
					key, i, gotSeqs[i], wantSeqs[i], gotSeqs, wantSeqs)
			}
		}
	}
}

// TestKafkaIntegrationFailFastUnreachable re-confirms the startup fail-fast
// behavior (R6.10) without requiring a live broker: it points the backend at an
// address that refuses connections and asserts construction fails with an error
// naming the unreachable broker. This complements the live-broker ordering test
// above and always runs in the default suite.
func TestKafkaIntegrationFailFastUnreachable(t *testing.T) {
	shortDialTimeout(t, 500*time.Millisecond)
	broker := closedAddr(t)

	q, err := New(Config{Type: "kafka", Kafka: KafkaConfig{
		Brokers: []string{broker},
		Topic:   "pgshadow",
	}}, nil)
	if err == nil {
		if q != nil {
			_ = q.Close()
		}
		t.Fatalf("New(type=kafka) with unreachable broker expected error, got nil")
	}
	if !strings.Contains(err.Error(), broker) {
		t.Fatalf("error %q does not name unreachable broker %q", err.Error(), broker)
	}
}

// mkConn builds a ConnID with a fixed target so only the source endpoint varies
// per connection in the ordering test.
func mkConn(srcIP string, srcPort uint16) core.ConnID {
	return core.ConnID{
		SrcIP:   netip.MustParseAddr(srcIP),
		SrcPort: srcPort,
		DstIP:   netip.MustParseAddr("10.0.0.1"),
		DstPort: 5432,
	}
}

// ensureTopicReady ensures a topic exists and is writable. On KRaft-mode
// brokers the Go kafka-go library's CreateTopics may not work (returns EOF).
// This helper triggers topic auto-creation by sending a Metadata request for
// the topic (which causes the broker to auto-create it when
// auto.create.topics.enable=true), then waits until a produce succeeds.
func ensureTopicReady(t *testing.T, broker, topic string) {
	t.Helper()

	// A Metadata request for a non-existent topic triggers auto-creation
	// when auto.create.topics.enable=true on the broker.
	conn, err := kafka.Dial("tcp", broker)
	if err == nil {
		// ReadPartitions internally sends a Metadata request which triggers
		// auto-creation of the topic on the broker.
		_, _ = conn.ReadPartitions(topic)
		conn.Close()
		time.Sleep(2 * time.Second) // wait for topic to be fully ready
	}

	// Verify the topic is writable with retries.
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
		MaxAttempts:            5,
	}
	defer w.Close()

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = w.WriteMessages(ctx, kafka.Message{
			Key:   []byte("__setup__"),
			Value: []byte("ready"),
		})
		cancel()
		if err == nil {
			time.Sleep(500 * time.Millisecond)
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("ensureTopicReady: topic %q not writable after retries: %v", topic, err)
}
