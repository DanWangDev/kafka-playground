package kafka

import (
	"context"
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

// StreamMessages spins up a Kafka Reader for a given topic and consumer group,
// streaming received messages into msgChan until the context is cancelled.
func StreamMessages(ctx context.Context, topic string, groupID string, fromBeginning bool, msgChan chan<- ConsumedMessage) error {
	var startOffset int64
	if fromBeginning {
		startOffset = kafka.FirstOffset // Start from earliest message in the log
	} else {
		startOffset = kafka.LastOffset  // Start from latest (new messages only)
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:      []string{BrokerAddress},
		Topic:        topic,
		GroupID:      groupID,
		StartOffset:  startOffset,
		MinBytes:     1,               // 1 byte minimum to fetch messages immediately
		MaxBytes:     10e6,            // 10MB maximum batch size
		ReadLagInterval: 1 * time.Second, // Update lag stats periodically
	}

	reader := kafka.NewReader(readerConfig)
	defer reader.Close()

	for {
		// ReadMessage blocks until a message is available or context is cancelled.
		// It automatically handles heartbeat signals, joins the consumer group,
		// and commits offsets according to the ReaderConfig default (Auto-Commit).
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			// If context is cancelled (e.g., WebSocket disconnects), exit cleanly.
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		headers := make(map[string]string)
		for _, h := range m.Headers {
			headers[h.Key] = string(h.Value)
		}

		msgChan <- ConsumedMessage{
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       string(m.Key),
			Value:     string(m.Value),
			Timestamp: m.Time,
			Headers:   headers,
		}
	}
}
