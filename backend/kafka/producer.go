package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

// InitProducer initializes the global Kafka producer client.
func InitProducer() {
	writer = &kafka.Writer{
		Addr:                   kafka.TCP(BrokerAddresses[0]),
		Balancer:               &kafka.Hash{}, // Routes identical keys to the same partition for ordering
		Async:                  false,         // Sync mode so we get partition/offset info immediately on write
		RequiredAcks:           kafka.RequireOne, // Wait for leader broker confirmation
		WriteTimeout:           5 * time.Second,
		AllowAutoTopicCreation: true,          // Automatically create DLQ or missing topics on publish
	}
}

// CloseProducer cleans up and flushes any buffered messages.
func CloseProducer() error {
	if writer != nil {
		return writer.Close()
	}
	return nil
}

// PublishMessage sends a record with key, value, and custom headers to a topic.
func PublishMessage(ctx context.Context, topic string, key string, value string, headers map[string]string) (int, int64, error) {
	kafkaHeaders := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   k,
			Value: []byte(v),
		})
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   []byte(value),
		Headers: kafkaHeaders,
	}

	// In segmentio/kafka-go, when Async is false, WriteMessages blocks until Kafka acknowledges
	// the write. We pass the message in a slice using the variadic slice operator (msgs...)
	// so the library updates the Partition and Offset fields directly in our slice.
	msgs := []kafka.Message{msg}
	err := writer.WriteMessages(ctx, msgs...)
	if err != nil {
		return 0, 0, err
	}

	return msgs[0].Partition, msgs[0].Offset, nil
}

// parseAcks maps a string to the required acks level.
func parseAcks(a string) kafka.RequiredAcks {
	switch a {
	case "0":
		return kafka.RequireNone
	case "all":
		return kafka.RequireAll
	default:
		return kafka.RequireOne
	}
}

// BatchProduceResult holds timing metrics from a batch produce run.
type BatchProduceResult struct {
	MessagesSent  int     `json:"messagesSent"`
	TotalDuration string  `json:"totalDuration"`
	MessagesPerSec float64 `json:"messagesPerSec"`
	AvgLatencyMs  float64 `json:"avgLatencyMs"`
}

// BatchProduce sends N messages to a topic and measures throughput/latency.
func BatchProduce(ctx context.Context, topic string, count int, batchSize int, compression string, acks string) (*BatchProduceResult, error) {
	requiredAcks := parseAcks(acks)

	bw := &kafka.Writer{
		Addr:                   kafka.TCP(BrokerAddresses[0]),
		Balancer:               &kafka.RoundRobin{},
		Async:                  batchSize > 1,
		RequiredAcks:           requiredAcks,
		BatchSize:              batchSize,
		BatchTimeout:           50 * time.Millisecond,
		AllowAutoTopicCreation: true,
	}

	// Only set compression if explicitly requested
	switch compression {
	case "gzip":
		bw.Compression = kafka.Gzip
	case "snappy":
		bw.Compression = kafka.Snappy
	case "lz4":
		bw.Compression = kafka.Lz4
	}
	defer bw.Close()

	start := time.Now()

	for i := 0; i < count; i++ {
		msg := kafka.Message{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("bench-%d", i%100)),
			Value: []byte(fmt.Sprintf(`{"seq":%d,"ts":"%s","payload":"Lorem ipsum benchmark data padding to make message larger"}`, i, time.Now().Format(time.RFC3339Nano))),
		}
		if err := bw.WriteMessages(ctx, msg); err != nil {
			return nil, fmt.Errorf("batch produce error at message %d: %w", i, err)
		}
	}

	elapsed := time.Since(start)
	msgsPerSec := float64(count) / elapsed.Seconds()
	avgLatency := float64(elapsed.Milliseconds()) / float64(count)

	return &BatchProduceResult{
		MessagesSent: count,
		TotalDuration: elapsed.String(),
		MessagesPerSec: msgsPerSec,
		AvgLatencyMs: avgLatency,
	}, nil
}
