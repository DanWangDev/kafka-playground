package kafka

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConsumerMember represents one active consumer in a group.
type ConsumerMember struct {
	ID        string    `json:"id"`
	GroupID   string    `json:"groupId"`
	Topic     string    `json:"topic"`
	Since     time.Time `json:"since"`
}

// PartitionAssignment shows which member owns which partition.
type PartitionAssignment struct {
	Partition int    `json:"partition"`
	Owner     string `json:"owner"` // consumer member ID or "unassigned"
	Lag       int64  `json:"lag"`
}

// ConsumerGroupView is the broker-side view of a consumer group.
type ConsumerGroupView struct {
	GroupID    string               `json:"groupId"`
	Topic      string               `json:"topic"`
	State      string               `json:"state"` // Stable, PreparingRebalance, etc.
	Members    []ConsumerMember     `json:"members"`
	Partitions []PartitionAssignment `json:"partitions"`
}

var (
	consumerRegistry   = make(map[string]*ConsumerMember)
	consumerRegistryMu sync.RWMutex
)

// RegisterConsumer adds a consumer to the in-memory registry.
func RegisterConsumer(id, groupID, topic string) {
	consumerRegistryMu.Lock()
	defer consumerRegistryMu.Unlock()
	consumerRegistry[id] = &ConsumerMember{
		ID:      id,
		GroupID: groupID,
		Topic:   topic,
		Since:   time.Now(),
	}
}

// UnregisterConsumer removes a consumer from the registry.
func UnregisterConsumer(id string) {
	consumerRegistryMu.Lock()
	defer consumerRegistryMu.Unlock()
	delete(consumerRegistry, id)
}

// GetRegisteredConsumers returns a snapshot of active consumers.
func GetRegisteredConsumers() []ConsumerMember {
	consumerRegistryMu.RLock()
	defer consumerRegistryMu.RUnlock()
	members := make([]ConsumerMember, 0, len(consumerRegistry))
	for _, m := range consumerRegistry {
		members = append(members, *m)
	}
	return members
}

// DescribeConsumerGroup queries the broker for consumer group partition assignments.
// Uses docker exec to run kafka-consumer-groups.sh inside broker-1.
func DescribeConsumerGroup(groupID string) (*ConsumerGroupView, error) {
	cmd := exec.Command("docker", "exec", "kafka-playground-broker-1",
		"/opt/kafka/bin/kafka-consumer-groups.sh",
		"--bootstrap-server", "localhost:9092",
		"--group", groupID,
		"--describe",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to describe consumer group: %w", err)
	}

	view := &ConsumerGroupView{
		GroupID:    groupID,
		Partitions: make([]PartitionAssignment, 0),
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	for _, line := range lines {
		if line == "" || strings.HasPrefix(line, "GROUP") || strings.HasPrefix(line, "Consumer group") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 7 {
			continue
		}
		// Format: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
		partition, _ := strconv.Atoi(fields[2])
		lag, _ := strconv.ParseInt(fields[5], 10, 64)

		owner := "unassigned"
		if fields[6] != "-" {
			owner = strings.TrimSpace(fields[6])
		}

		view.Topic = fields[1]

		view.Partitions = append(view.Partitions, PartitionAssignment{
			Partition: partition,
			Owner:     owner,
			Lag:       lag,
		})
	}

	// Determine group state
	if len(view.Partitions) > 0 {
		view.State = "Stable"
	} else {
		view.State = "Empty"
	}

	// Merge registry members for this group
	consumerRegistryMu.RLock()
	defer consumerRegistryMu.RUnlock()
	for _, m := range consumerRegistry {
		if m.GroupID == groupID {
			view.Members = append(view.Members, *m)
		}
	}

	return view, nil
}

// ListConsumerGroups queries the broker for all consumer groups.
func ListConsumerGroups() ([]string, error) {
	cmd := exec.Command("docker", "exec", "kafka-playground-broker-1",
		"/opt/kafka/bin/kafka-consumer-groups.sh",
		"--bootstrap-server", "localhost:9092",
		"--list",
	)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	groups := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			groups = append(groups, line)
		}
	}
	return groups, nil
}
