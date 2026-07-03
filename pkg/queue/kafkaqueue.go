// kafkaqueue.go implements the external-Kafka Queue backend (task 7.3).
//
// kafkaQueue produces SQL_Events to, and consumes them from, an external,
// operator-provided Kafka cluster via github.com/segmentio/kafka-go. Each
// message is keyed by the source ConnID (ev.Conn.Key()) and the producer uses a
// hash balancer, so all events from one source connection land in the same
// partition and are delivered in captured order, preserving per-ConnID ordering
// (R6.6, R6.7). The backend never bundles or runs a broker — it only connects to
// the configured external cluster (R6.9) — and fails fast at construction if the
// configured brokers are unreachable, naming the unreachable brokers (R6.10).
//
// Serialization reuses the package-local gob helpers encodeEvent/decodeEvent
// (defined in filequeue.go), so every SQL_Event field survives the round trip.
package queue

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"pgshadow/pkg/core"
	"pgshadow/pkg/metrics"
)

const (
	// defaultKafkaTopic is used when KafkaConfig.Topic is empty.
	defaultKafkaTopic = "pgshadow"
	// defaultKafkaGroupID is the consumer group used by the reader so a single
	// pgshadow process consumes every partition of the topic while Kafka tracks
	// offsets. Per-partition (hence per-ConnID) order is preserved within each
	// assigned partition.
	defaultKafkaGroupID = "pgshadow-replayer"
	// defaultKafkaDialTimeout bounds the per-broker startup reachability probe
	// (R6.10) so an unreachable broker fails fast rather than hanging startup.
	defaultKafkaDialTimeout = 10 * time.Second
)

// kafkaDialTimeout is the per-broker reachability probe timeout. It is a
// package var (rather than a const) so tests can shorten it; production uses
// the default.
var kafkaDialTimeout = defaultKafkaDialTimeout

// kafkaQueue is the external-Kafka backend. The Kafka cluster provides durable,
// effectively unbounded buffering, so this backend does not apply the in-memory
// overflow policy (R6.9): events are never discarded due to a local capacity
// bound. A produce failure is reported as dropped=true so the caller can account
// for it.
//
// Architecture: Enqueue writes to a local in-memory ring buffer (non-blocking,
// nanosecond-level) which decouples the capture pipeline from Kafka produce
// latency. A background goroutine batch-produces from the ring buffer to Kafka
// asynchronously. Dequeue reads from the Kafka consumer as before. This ensures
// the streamProcessor is never blocked by Kafka network round trips.
type kafkaQueue struct {
	writer *kafka.Writer
	reader *kafka.Reader

	brokers []string
	topic   string

	// Local async buffer: Enqueue writes here, background producer drains it.
	msgCh chan kafka.Message

	// produced/consumed are local counters used to approximate Depth (see
	// Depth for the rationale and limitations of this approximation).
	produced int64
	consumed int64

	collector *metrics.Collector

	mu       sync.Mutex
	closed   bool
	doneCh   chan struct{} // closed when the background producer exits
}

// kafkaLocalBufferSize is the capacity of the local async buffer between
// Enqueue and the Kafka producer goroutine. Sized to absorb ~30s of 5000 TPS
// burst without blocking the capture pipeline.
const kafkaLocalBufferSize = 200000

// newKafkaQueue verifies broker reachability and constructs the Kafka backend.
// It fails fast, naming the unreachable brokers, if any configured broker
// cannot be reached at startup (R6.10). It never starts an embedded broker
// (R6.9). Supports SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and TLS for
// connecting to managed Kafka services (AWS MSK, Confluent Cloud, etc.).
func newKafkaQueue(cfg KafkaConfig, m *metrics.Collector) (Queue, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("queue.New: kafka backend requires at least one broker address")
	}

	topic := cfg.Topic
	if strings.TrimSpace(topic) == "" {
		topic = defaultKafkaTopic
	}

	// Build TLS config if enabled
	var tlsCfg *tls.Config
	if cfg.TLSEnabled {
		var err error
		tlsCfg, err = buildTLSConfig(cfg)
		if err != nil {
			return nil, fmt.Errorf("queue.New: kafka TLS config: %w", err)
		}
	}

	// Build SASL mechanism if configured
	var saslMechanism sasl.Mechanism
	if cfg.SASLMechanism != "" {
		var err error
		saslMechanism, err = buildSASLMechanism(cfg)
		if err != nil {
			return nil, fmt.Errorf("queue.New: kafka SASL config: %w", err)
		}
	}

	// Build the shared transport for dialer, writer, reader
	transport := &kafka.Transport{
		TLS:  tlsCfg,
		SASL: saslMechanism,
	}

	// Build the dialer for reachability checks
	dialer := &kafka.Dialer{
		Timeout:       kafkaDialTimeout,
		TLS:           tlsCfg,
		SASLMechanism: saslMechanism,
	}

	// Startup reachability check (R6.10): probe every configured broker and
	// fail fast if any are unreachable, naming them.
	if unreachable := dialBrokersWithDialer(cfg.Brokers, dialer, kafkaDialTimeout); len(unreachable) > 0 {
		return nil, fmt.Errorf("queue.New: kafka backend: unreachable broker(s): %s",
			strings.Join(unreachable, ", "))
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(cfg.Brokers...),
		Topic:     topic,
		Transport: transport,
		// Hash the message key (ConnID) to a partition so same-connection
		// events stay in one partition and preserve order (R6.7).
		Balancer: &kafka.Hash{},
		// Batch up to 1000 messages or 10ms, whichever comes first.
		BatchSize:    1000,
		BatchTimeout: 10 * time.Millisecond,
		// Allow the broker to auto-create the topic on first produce.
		AllowAutoTopicCreation: true,
		// Retry transient errors.
		MaxAttempts:     10,
		WriteBackoffMin: 250 * time.Millisecond,
		WriteBackoffMax: 2 * time.Second,
		// Async mode: WriteMessages returns immediately, flushing in background.
		Async: true,
	}

	readerCfg := kafka.ReaderConfig{
		Brokers:     cfg.Brokers,
		Topic:       topic,
		GroupID:     defaultKafkaGroupID,
		StartOffset: kafka.FirstOffset,
	}
	if dialer.TLS != nil || dialer.SASLMechanism != nil {
		readerCfg.Dialer = dialer
	}
	reader := kafka.NewReader(readerCfg)

	q := &kafkaQueue{
		writer:    writer,
		reader:    reader,
		brokers:   append([]string(nil), cfg.Brokers...),
		topic:     topic,
		collector: m,
		msgCh:     make(chan kafka.Message, kafkaLocalBufferSize),
		doneCh:    make(chan struct{}),
	}

	// Start the background batch producer.
	go q.produceLoop()

	return q, nil
}

// produceLoop drains the local message channel and batch-writes to Kafka. It
// collects up to BatchSize messages or waits up to 5ms for more messages before
// flushing, balancing latency and throughput. It implements partial retry
// logic: on failure, individual messages are retried rather than discarding
// the entire batch. It exits when msgCh is closed (triggered by Close).
func (q *kafkaQueue) produceLoop() {
	defer close(q.doneCh)

	batch := make([]kafka.Message, 0, 1000)
	for {
		// Wait for at least one message.
		msg, ok := <-q.msgCh
		if !ok {
			// Channel closed — flush any remaining and exit.
			return
		}
		batch = append(batch, msg)

		// Drain as many as available without blocking (up to batch capacity).
	drain:
		for len(batch) < cap(batch) {
			select {
			case m, ok := <-q.msgCh:
				if !ok {
					break drain
				}
				batch = append(batch, m)
			default:
				break drain
			}
		}

		// Attempt batch produce with partial retry logic
		result := q.writeBatchWithRetry(batch)
		// Log the failed messages for overflow tracking
		if result.FailedCount > 0 {
			for i := 0; i < result.FailedCount; i++ {
				q.recordOverflow()
			}
		}

		batch = batch[:0]
	}
}

// writeResult holds the result of a batch write attempt.
type writeResult struct {
	Failed      []kafka.Message
	FailedCount int
}

// writeBatchWithRetry attempts to write a batch of messages to Kafka with
// partial retry logic. It first tries the full batch, then on failure it
// splits and retries smaller batches, and finally retries individual messages.
// This approach prevents losing all messages in a batch due to a single
// problematic message.
func (q *kafkaQueue) writeBatchWithRetry(batch []kafka.Message) writeResult {
	if len(batch) == 0 {
		return writeResult{}
	}

	// Try full batch first
	err := q.writer.WriteMessages(context.Background(), batch...)
	if err == nil {
		atomic.AddInt64(&q.produced, int64(len(batch)))
		return writeResult{}
	}

	// Full batch failed - try with exponential backoff retry
	err = q.retryWithBackoff(batch, 3)
	if err == nil {
		return writeResult{}
	}

	// Retry exhausted - try partial batch approach
	result := q.writePartialBatch(batch)
	
	// Update produced count for successful messages
	successCount := len(batch) - result.FailedCount
	atomic.AddInt64(&q.produced, int64(successCount))

	return result
}

// retryWithBackoff attempts to write the batch with retries and exponential backoff.
// It aborts early when the queue is closing so Close() is not blocked for 10+ seconds.
func (q *kafkaQueue) retryWithBackoff(batch []kafka.Message, maxRetries int) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 100ms, 200ms, 400ms
			backoff := time.Duration(100<<uint(attempt-1)) * time.Millisecond
			time.Sleep(backoff)

			// Abort if queue is closing — don't hold up Close() with retries.
			q.mu.Lock()
			closed := q.closed
			q.mu.Unlock()
			if closed {
				return lastErr
			}
		}

		err := q.writer.WriteMessages(context.Background(), batch...)
		if err == nil {
			return nil
		}
		lastErr = err

		// Check if error is retriable
		if !isRetriableKafkaError(err) {
			break
		}
	}
	return lastErr
}

// writePartialBatch attempts to write messages in smaller batches, then
// individually, to maximize successful writes while tracking failures.
func (q *kafkaQueue) writePartialBatch(batch []kafka.Message) writeResult {
	result := writeResult{
		Failed: make([]kafka.Message, 0),
	}

	// First, try smaller batches (split batch in half)
	if len(batch) > 1 {
		mid := len(batch) / 2
		left := batch[:mid]
		right := batch[mid:]

		// Try left half
		if err := q.writer.WriteMessages(context.Background(), left...); err != nil {
			// Left failed, add to failed list
			result.Failed = append(result.Failed, left...)
		} else {
			// Left succeeded, clear it from failed tracking
			left = nil
		}

		// Try right half
		if err := q.writer.WriteMessages(context.Background(), right...); err != nil {
			result.Failed = append(result.Failed, right...)
		} else {
			right = nil
		}

		// If partial success, retry the failed messages individually
		if len(result.Failed) > 0 && len(result.Failed) < len(batch) {
			remaining := result.Failed
			result.Failed = nil
			
			// Retry remaining messages one by one
			for _, msg := range remaining {
				if err := q.writer.WriteMessages(context.Background(), msg); err != nil {
					result.Failed = append(result.Failed, msg)
				}
			}
		}
	} else {
		// Single message - just retry once
		if err := q.writer.WriteMessages(context.Background(), batch[0]); err != nil {
			result.Failed = append(result.Failed, batch[0])
		}
	}

	result.FailedCount = len(result.Failed)
	return result
}

// dialBrokers probes each broker with a bounded timeout and returns the list of
// brokers that could not be reached. It dials only — it never produces, creates
// topics, or starts a broker (R6.9).
func dialBrokers(brokers []string, timeout time.Duration) []string {
	return dialBrokersWithDialer(brokers, &kafka.Dialer{Timeout: timeout}, timeout)
}

// dialBrokersWithDialer probes each broker using the provided dialer (which may
// carry TLS/SASL config for managed clusters) and returns unreachable brokers.
func dialBrokersWithDialer(brokers []string, dialer *kafka.Dialer, timeout time.Duration) []string {
	var unreachable []string
	for _, b := range brokers {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		conn, err := dialer.DialContext(ctx, "tcp", b)
		cancel()
		if err != nil {
			unreachable = append(unreachable, b)
			continue
		}
		_ = conn.Close()
	}
	return unreachable
}

// buildTLSConfig creates a *tls.Config from the Kafka TLS settings.
func buildTLSConfig(cfg KafkaConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: cfg.TLSSkipVerify,
	}

	// Load CA certificate if provided
	if cfg.TLSCAFile != "" {
		caCert, err := os.ReadFile(cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file %q: %w", cfg.TLSCAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("CA file %q contains no valid certificates", cfg.TLSCAFile)
		}
		tlsCfg.RootCAs = pool
	}

	// Load client certificate for mTLS if provided
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// buildSASLMechanism creates the appropriate SASL mechanism from config.
func buildSASLMechanism(cfg KafkaConfig) (sasl.Mechanism, error) {
	password := cfg.SASLPassword
	if cfg.SASLPasswordEnv != "" {
		password = os.Getenv(cfg.SASLPasswordEnv)
		if password == "" {
			return nil, fmt.Errorf("SASL password env %q is empty or unset", cfg.SASLPasswordEnv)
		}
	}
	if cfg.SASLUsername == "" {
		return nil, errors.New("sasl_username is required when sasl_mechanism is set")
	}
	if password == "" {
		return nil, errors.New("sasl_password or sasl_password_env is required when sasl_mechanism is set")
	}

	switch strings.ToLower(cfg.SASLMechanism) {
	case "plain":
		return &plain.Mechanism{
			Username: cfg.SASLUsername,
			Password: password,
		}, nil
	case "scram-sha-256":
		mechanism, err := scram.Mechanism(scram.SHA256, cfg.SASLUsername, password)
		if err != nil {
			return nil, fmt.Errorf("scram-sha-256: %w", err)
		}
		return mechanism, nil
	case "scram-sha-512":
		mechanism, err := scram.Mechanism(scram.SHA512, cfg.SASLUsername, password)
		if err != nil {
			return nil, fmt.Errorf("scram-sha-512: %w", err)
		}
		return mechanism, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q (supported: plain, scram-sha-256, scram-sha-512)", cfg.SASLMechanism)
	}
}

// kafkaMessage builds the Kafka message for a SQL_Event: the partition key is
// the stable ConnID key (R6.7) and the value is the binary-encoded event. It is
// extracted so the ConnID-keying logic can be unit-tested without a live broker.
func kafkaMessage(ev *core.SQLEvent) (kafka.Message, error) {
	payload, err := encodeEvent(ev)
	if err != nil {
		return kafka.Message{}, err
	}
	return kafka.Message{
		Key:   []byte(ev.Conn.Key()),
		Value: payload,
	}, nil
}

// recordOverflow notifies the metrics collector of a discarded event, if a
// collector is configured.
func (q *kafkaQueue) recordOverflow() {
	if q.collector != nil && *q.collector != nil {
		(*q.collector).Overflow()
	}
}

// isRetriableKafkaError returns true for transient Kafka errors that may resolve
// on retry, such as UnknownTopicOrPartition (topic being auto-created) or
// LeaderNotAvailable (partition leader election in progress).
func isRetriableKafkaError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Unknown Topic Or Partition") ||
		strings.Contains(msg, "Leader Not Available") ||
		strings.Contains(msg, "Not Leader") ||
		strings.Contains(msg, "Request Timed Out")
}

// Enqueue writes ev to the local async buffer (non-blocking under normal load).
// The background produceLoop batch-flushes the buffer to Kafka. This ensures
// the capture pipeline is never blocked by Kafka network latency. It returns
// dropped=true only when the local buffer is full (which means Kafka is
// unreachable for an extended period and the buffer has exhausted).
func (q *kafkaQueue) Enqueue(ev *core.SQLEvent) (dropped bool) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}

	msg, err := kafkaMessage(ev)
	if err != nil {
		q.mu.Unlock()
		q.recordOverflow()
		return true
	}

	// Non-blocking send to the local buffer while holding the lock, so Close
	// cannot close the channel between our closed-check and the send.
	select {
	case q.msgCh <- msg:
		q.mu.Unlock()
		return false
	default:
		q.mu.Unlock()
		q.recordOverflow()
		return true
	}
}

// Dequeue blocks until a message is available, the context is cancelled, or the
// queue is closed. It returns ok=false when the context is cancelled or the
// reader is closed. Per-ConnID order is preserved because each ConnID maps to a
// single partition (R6.6, R6.7).
func (q *kafkaQueue) Dequeue(ctx context.Context) (*core.SQLEvent, bool) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, false
	}
	q.mu.Unlock()

	// maxConsecutiveDecodeErrors is the circuit-breaker threshold: if this many
	// consecutive messages fail to decode, Dequeue returns (nil, false) to stop
	// the replay loop. This indicates a systemic issue (codec version mismatch,
	// topic corruption) rather than isolated bad messages, and continuing would
	// silently discard a large fraction of traffic.
	const maxConsecutiveDecodeErrors = 50

	consecutiveErrors := 0

	for {
		msg, err := q.reader.ReadMessage(ctx)
		if err != nil {
			// Context cancellation, reader closed, or a transient read error.
			return nil, false
		}

		ev, err := decodeEvent(msg.Value)
		if err != nil {
			consecutiveErrors++

			// Rate-limited logging: log first, every 10th, and the final one
			// before circuit-break to avoid log storms.
			if consecutiveErrors == 1 || consecutiveErrors%10 == 0 || consecutiveErrors >= maxConsecutiveDecodeErrors {
				fmt.Fprintf(os.Stderr,
					"pgshadow: ERROR [%s] kafka dequeue: corrupt message #%d "+
						"(partition=%d offset=%d size=%d): %v\n",
					time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
					consecutiveErrors, msg.Partition, msg.Offset, len(msg.Value), err)
			}

			if consecutiveErrors >= maxConsecutiveDecodeErrors {
				fmt.Fprintf(os.Stderr,
					"pgshadow: FATAL [%s] kafka dequeue: %d consecutive decode failures — "+
						"circuit breaker tripped, stopping consumer (likely codec version mismatch)\n",
					time.Now().Format("2006-01-02T15:04:05.000Z07:00"),
					consecutiveErrors)
				return nil, false
			}

			atomic.AddInt64(&q.consumed, 1)
			continue
		}

		// Successful decode resets the circuit breaker.
		consecutiveErrors = 0
		atomic.AddInt64(&q.consumed, 1)
		return ev, true
	}
}

// Depth approximates the number of buffered events as produced-minus-consumed
// observed by THIS process (R10.3). It is an approximation: it does not reflect
// the true broker-side backlog (messages produced by other processes, or
// consumer-group lag across restarts), and it can go negative if this process
// consumes events it did not itself produce. The authoritative backlog for a
// Kafka backend is the consumer-group lag exposed by Kafka itself. We use this
// cheap local counter to keep the Queue interface uniform across backends.
func (q *kafkaQueue) Depth() int {
	// Include the local buffer depth plus the Kafka lag approximation.
	localDepth := int64(len(q.msgCh))
	kafkaDepth := atomic.LoadInt64(&q.produced) - atomic.LoadInt64(&q.consumed)
	d := localDepth + kafkaDepth
	if d < 0 {
		return 0
	}
	return int(d)
}

// Close shuts down the background producer, then the writer and consumer. It is
// idempotent. It never stops an embedded broker because none is ever started
// (R6.9). Closing the msgCh signals the produce loop to flush remaining
// messages and exit.
func (q *kafkaQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()

	// Signal the produce loop to drain and exit.
	close(q.msgCh)
	// Wait for the produce loop to finish flushing.
	<-q.doneCh

	var errs []error
	if q.writer != nil {
		if err := q.writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("kafka writer close: %w", err))
		}
	}
	if q.reader != nil {
		if err := q.reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("kafka reader close: %w", err))
		}
	}
	return errors.Join(errs...)
}
