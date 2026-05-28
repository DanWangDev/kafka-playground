package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// StressTestStatus holds the live state of a running stress test.
type StressTestStatus struct {
	Running       bool   `json:"running"`
	Topic         string `json:"topic"`
	MessagesSent  int64  `json:"messagesSent"`
	RatePerSec    int64  `json:"ratePerSec"`
	StartedAt     string `json:"startedAt,omitempty"`
}

var (
	stressActive    atomic.Bool
	stressMsgCount  atomic.Int64
	stressCancel    context.CancelFunc
	stressMu        sync.Mutex
	stressTopic     string
	stressStartedAt time.Time
)

// StartStressTest begins a background producer that fires messages at a target rate.
func StartStressTest(topic string, ratePerSec int) error {
	stressMu.Lock()
	defer stressMu.Unlock()

	if stressActive.Load() {
		return fmt.Errorf("stress test already running")
	}

	ctx, cancel := context.WithCancel(context.Background())
	stressCancel = cancel
	stressTopic = topic
	stressMsgCount.Store(0)
	stressStartedAt = time.Now()
	stressActive.Store(true)

	go func() {
		defer stressActive.Store(false)

		w := &kafka.Writer{
			Addr:         kafka.TCP(BrokerAddresses...),
			Balancer:     &kafka.RoundRobin{},
			Async:        true,
			RequiredAcks: kafka.RequireNone,
			BatchSize:    100,
			BatchTimeout: 5 * time.Millisecond,
		}
		defer w.Close()

		interval := time.Second / time.Duration(ratePerSec)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		seq := int64(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq++
				msg := kafka.Message{
					Topic: topic,
					Key:   []byte(fmt.Sprintf("stress-%d", seq%100)),
					Value: []byte(fmt.Sprintf(`{"seq":%d,"ts":"%s","type":"stress","data":"padding-%d"}`, seq, time.Now().Format(time.RFC3339), seq%1000)),
				}
				// Fire-and-forget in async mode
				_ = w.WriteMessages(context.Background(), msg)
				stressMsgCount.Store(seq)
			}
		}
	}()

	return nil
}

// StopStressTest halts the running stress test.
func StopStressTest() {
	stressMu.Lock()
	defer stressMu.Unlock()

	if stressCancel != nil {
		stressCancel()
		stressCancel = nil
	}
}

// GetStressTestStatus returns the current state of the stress test.
func GetStressTestStatus() StressTestStatus {
	running := stressActive.Load()
	var rate int64
	if running {
		elapsed := time.Since(stressStartedAt).Seconds()
		if elapsed > 0 {
			rate = int64(float64(stressMsgCount.Load()) / elapsed)
		}
	}

	result := StressTestStatus{
		Running:      running,
		Topic:        stressTopic,
		MessagesSent: stressMsgCount.Load(),
		RatePerSec:   rate,
	}
	if running {
		result.StartedAt = stressStartedAt.Format(time.RFC3339)
	}
	return result
}
