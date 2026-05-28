package kafka

import (
	"context"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumedMessage represents a structured message sent to the frontend dashboard.
type ConsumedMessage struct {
	Partition int               `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"headers"`
}

// ConsumerEvent represents a processing event (success or failure with DLQ routing details).
type ConsumerEvent struct {
	Type          string          `json:"type"` // "success" or "dlq"
	Message       ConsumedMessage `json:"message"`
	DlqTopic      string          `json:"dlqTopic,omitempty"`
	DlqPartition  int             `json:"dlqPartition,omitempty"`
	DlqOffset     int64           `json:"dlqOffset,omitempty"`
	FailureReason string          `json:"failureReason,omitempty"`
}

// StreamMessages spins up a Kafka Reader for a given topic and consumer group,
// streaming received messages into msgChan until the context is cancelled.
func StreamMessages(ctx context.Context, topic string, groupID string, fromBeginning bool, eventChan chan<- ConsumerEvent) error {
	var startOffset int64
	if fromBeginning {
		startOffset = kafka.FirstOffset // Start from earliest message in the log
	} else {
		startOffset = kafka.LastOffset  // Start from latest (new messages only)
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:         BrokerAddresses,
		Topic:           topic,
		GroupID:         groupID,
		StartOffset:     startOffset,
		MinBytes:        1,
		MaxBytes:        10e6,
		ReadLagInterval: 1 * time.Second,
	}

	reader := kafka.NewReader(readerConfig)
	defer reader.Close()

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		headers := make(map[string]string)
		for _, h := range m.Headers {
			headers[h.Key] = string(h.Value)
		}

		consumedMsg := ConsumedMessage{
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       string(m.Key),
			Value:     string(m.Value),
			Timestamp: m.Time,
			Headers:   headers,
		}

		// Fail if header "simulate-failure" is "true" OR body contains "fail"
		isFailure := false
		failureReason := ""
		if headers["simulate-failure"] == "true" || strings.Contains(strings.ToLower(string(m.Value)), "fail") {
			isFailure = true
			failureReason = "Simulated processing error: Payload validation failed (triggers DLQ routing rule)"
		}

		if isFailure {
			dlqTopic := m.Topic + "-dlq"

			// Copy original headers and inject DLQ tracing metadata (very important in production!)
			dlqHeaders := make(map[string]string)
			for k, v := range headers {
				dlqHeaders[k] = v
			}
			dlqHeaders["x-dlq-original-topic"] = m.Topic
			dlqHeaders["x-dlq-original-partition"] = strconv.Itoa(m.Partition)
			dlqHeaders["x-dlq-original-offset"] = strconv.FormatInt(m.Offset, 10)
			dlqHeaders["x-dlq-failure-reason"] = failureReason
			dlqHeaders["x-dlq-failed-at"] = time.Now().Format(time.RFC3339)

			// Publish failed message to DLQ topic
			dlqPartition, dlqOffset, pubErr := PublishMessage(ctx, dlqTopic, string(m.Key), string(m.Value), dlqHeaders)
			if pubErr != nil {
				log.Printf("[DLQ] Failed to publish message to DLQ: %v", pubErr)
			} else {
				log.Printf("[DLQ] Forwarded failed message to %s (P: %d, O: %d)", dlqTopic, dlqPartition, dlqOffset)
			}

			eventChan <- ConsumerEvent{
				Type:          "dlq",
				Message:       consumedMsg,
				DlqTopic:      dlqTopic,
				DlqPartition:  dlqPartition,
				DlqOffset:     dlqOffset,
				FailureReason: failureReason,
			}
		} else {
			// Successfully processed event
			eventChan <- ConsumerEvent{
				Type:    "success",
				Message: consumedMsg,
			}
		}
	}
}
