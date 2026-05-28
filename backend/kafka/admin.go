package kafka

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type BrokerInfo struct {
	ID   int    `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type PartitionInfo struct {
	ID       int   `json:"id"`
	Leader   int   `json:"leader"`
	Replicas []int `json:"replicas"`
	ISR      []int `json:"isr"`
}

type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

type ClusterMetadata struct {
	Brokers      []BrokerInfo `json:"brokers"`
	Topics       []TopicInfo  `json:"topics"`
	ControllerID int          `json:"controller_id"`
}

// GetClusterMetadata fetches active brokers, topics, partition leaders, and replicas.
func GetClusterMetadata(ctx context.Context) (*ClusterMetadata, error) {
	client := &kafka.Client{
		Addr:    kafka.TCP(BrokerAddresses...),
		Timeout: 5 * time.Second,
	}

	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, err
	}

	metadata := &ClusterMetadata{
		ControllerID: resp.Controller.ID,
		Brokers:      make([]BrokerInfo, 0, len(resp.Brokers)),
		Topics:       make([]TopicInfo, 0, len(resp.Topics)),
	}

	for _, broker := range resp.Brokers {
		metadata.Brokers = append(metadata.Brokers, BrokerInfo{
			ID:   broker.ID,
			Host: broker.Host,
			Port: broker.Port,
		})
	}

	for _, topic := range resp.Topics {
		// Hide the internal consumer offsets topic to keep the playground focused.
		if topic.Name == "__consumer_offsets" {
			continue
		}

		topicInfo := TopicInfo{
			Name:       topic.Name,
			Partitions: make([]PartitionInfo, 0, len(topic.Partitions)),
		}

		for _, partition := range topic.Partitions {
			replicas := make([]int, len(partition.Replicas))
			for i, r := range partition.Replicas {
				replicas[i] = r.ID
			}

			isr := make([]int, len(partition.Isr))
			for i, r := range partition.Isr {
				isr[i] = r.ID
			}

			topicInfo.Partitions = append(topicInfo.Partitions, PartitionInfo{
				ID:       partition.ID,
				Leader:   partition.Leader.ID,
				Replicas: replicas,
				ISR:      isr,
			})
		}

		metadata.Topics = append(metadata.Topics, topicInfo)
	}

	return metadata, nil
}

// CreateTopic creates a new Kafka topic with specified partition and replication configs.
func CreateTopic(ctx context.Context, name string, numPartitions int, replicationFactor int) error {
	client := &kafka.Client{
		Addr:    kafka.TCP(BrokerAddresses...),
		Timeout: 5 * time.Second,
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             name,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			},
		},
	})
	if err != nil {
		return err
	}

	if err, ok := resp.Errors[name]; ok && err != nil {
		return err
	}

	return nil
}

// DeleteTopic deletes an existing topic by name.
func DeleteTopic(ctx context.Context, name string) error {
	client := &kafka.Client{
		Addr:    kafka.TCP(BrokerAddresses...),
		Timeout: 5 * time.Second,
	}

	resp, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{
		Topics: []string{name},
	})
	if err != nil {
		return err
	}

	if err, ok := resp.Errors[name]; ok && err != nil {
		return err
	}

	return nil
}

// TopicConfigEntry represents a topic-level configuration.
type TopicConfigEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// SetTopicConfig applies topic-level configs via kafka-configs.sh.
func SetTopicConfig(ctx context.Context, name string, configs map[string]string) error {
	args := []string{
		"exec", "kafka-playground-broker-1",
		"/opt/kafka/bin/kafka-configs.sh",
		"--bootstrap-server", "localhost:9092",
		"--entity-type", "topics",
		"--entity-name", name,
		"--alter",
	}
	for k, v := range configs {
		args = append(args, fmt.Sprintf("--add-config=%s=%s", k, v))
	}

	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to set topic config: %w\nOutput: %s", err, string(out))
	}
	return nil
}

// DescribeTopicConfig returns the current config for a topic.
func DescribeTopicConfig(name string) ([]TopicConfigEntry, error) {
	cmd := exec.Command("docker", "exec", "kafka-playground-broker-1",
		"/opt/kafka/bin/kafka-configs.sh",
		"--bootstrap-server", "localhost:9092",
		"--entity-type", "topics",
		"--entity-name", name,
		"--describe",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to describe topic config: %w", err)
	}

	// Parse output to extract non-default overrides
	// Output has lines like "  retention.ms=30000 sensitive=false synonyms={...}"
	entries := make([]TopicConfigEntry, 0)
	for _, line := range splitLines(string(out)) {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) < 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		valPart := parts[1]
		// Value is before the first space
		value := strings.SplitN(valPart, " ", 2)[0]
		entries = append(entries, TopicConfigEntry{Key: key, Value: value})
	}
	return entries, nil
}

func splitLines(s string) []string {
	var lines []string
	for _, line := range strings.Split(strings.TrimSpace(s), "\n") {
		lines = append(lines, line)
	}
	return lines
}

