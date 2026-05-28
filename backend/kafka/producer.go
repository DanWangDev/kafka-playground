package kafka

import (
	"context"
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
