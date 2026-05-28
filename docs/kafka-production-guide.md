# Apache Kafka: Local Playground to Production Architecture

This guide details the core concepts of Apache Kafka, explains the choices made in our local playground, and details how a high-availability, production-grade Kafka environment is structured.

---

## 1. Local Playground vs. Production Cluster

Our playground runs a single broker with KRaft mode. While excellent for debugging, a production cluster is designed for high availability, partition distribution, and failover:

| Attribute | Local Playground | Production Environment |
| :--- | :--- | :--- |
| **Broker Count** | `1` Broker | `3` or `5` Brokers (Odd number for quorum) |
| **Replication Factor** | `1` (No backup copies) | `3` (Standard) or `5` (Ultra-critical) |
| **KRaft Roles** | 1 Node acts as Controller & Broker | Controllers are separate, dedicated nodes (usually 3) |
| **Storage** | Ephemeral Docker Volume | Multi-disk RAID, SAN, or persistent cloud SSDs |
| **Data Partitioning** | Local routing | Distributed routing across physical network racks |

### Production Network Topology
In a standard production deployment, brokers are distributed across different **Availability Zones (AZs)** or hardware racks. If an entire data center zone goes offline, the remaining brokers in the other zones form a quorum and continue serving traffic without data loss.

---

## 2. Deep Dive: Partitioning & Ordering

Partitions are Kafka's unit of scale. A single topic with 12 partitions can distribute its write and read load across 12 different brokers.

### The Ordering Guarantees
* **Local Ordering**: Kafka only guarantees message ordering **within a single partition**.
* **Global Ordering**: If you need global ordering across an entire topic, that topic **must have exactly 1 partition**. However, this severely limits throughput because only one consumer thread can read it at a time.
* **Key-Based Routing**: By assigning a `Key` to a message (e.g. `UserID: 891`), the Kafka Producer hashes the key and routes all messages with that key to the same partition. This ensures all events for that specific user are processed sequentially by consumers, preserving chronological domain logic.

---

## 3. Reliability Semantics & Trade-offs

When designing message flows, you must choose between speed and safety:

### Write Acks (`acks` configuration)
1. **`acks=0` (Fire-and-forget)**:
   * **Behavior**: Producer sends the message and does not wait for any response from the broker.
   * **Use Case**: Non-critical high-volume metrics (e.g., website clickstream, server temp readings).
2. **`acks=1` (Leader Acknowledgement)**:
   * **Behavior**: The producer waits for the partition Leader to write the record to its local log.
   * **Use Case**: Default balance of speed and safety. If the leader crashes before replication, data is lost.
3. **`acks=all` (Cluster-wide Ack)**:
   * **Behavior**: The producer waits for the leader AND all in-sync replicas (ISR) to commit the message.
   * **Use Case**: Crucial financial records, audit logs, billing. Used in tandem with `min.insync.replicas=2` (guarantees at least two copies are committed before acknowledging).

### Delivery Guarantees
* **At-Most-Once**: Offsets are committed *before* message processing is complete. If the consumer crashes during processing, the message is skipped.
* **At-Least-Once**: Offsets are committed *after* message processing succeeds. If the consumer crashes, the replacement consumer reads the message again. **Requires consumers to be idempotent** (processing the same message twice has no side-effects).
* **Exactly-Once (EOS)**: Achieved using Kafka's Transaction Coordinator API. Messages are written and committed atomically across multiple topics. In Go, this requires a transaction-capable client (like `confluent-kafka-go` or `franz-go`).

---

## 4. Go Best Practices for Kafka in Production

Writing resilient Go services with Kafka requires handling concurrency and system interrupts gracefully.

### A. Graceful Shutdown (Signal Handling)
If a Go service is containerized (e.g., on Kubernetes) and redeployed, it will receive a `SIGTERM` signal. The app must stop consuming, complete processing active messages, commit offsets, flush the producer, and then exit. 

Here is a standard production Go pattern for handling this:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Create a context that is cancelled when we receive system interrupts
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"prod-broker:9092"},
		GroupID:  "payment-processors",
		Topic:    "orders",
		MaxBytes: 10e6,
	})
	defer reader.Close()

	log.Println("Consumer started...")

	for {
		// ReadMessage automatically respects context cancellation
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Context cancelled. Shutting down consumer gracefully...")
				break
			}
			log.Printf("Consumer read error: %v", err)
			continue
		}

		// Process your message here (ensure logic is idempotent!)
		processOrder(m.Value)
	}

	// Clean up any databases, connections, or buffered logs here
	log.Println("Graceful shutdown complete.")
}

func processOrder(payload []byte) {
	// Business logic
}
```

### B. Connection Reuse
Creating connection channels to Kafka has overhead. You should initialize the Kafka Producer/Writer once on startup (sharing a thread-safe instance) and close it on application shutdown, rather than opening and closing writers on every single REST API request.

---

## 5. Operations & Monitoring in Production

### The Golden Metric: Consumer Lag
**Consumer Lag** is the distance between the last written offset in a partition (Log End Offset) and the offset processed by the consumer group.
* If a producer has written up to offset `1000`, and a consumer group is at offset `950`, the **lag is 50 messages**.
* **Why it matters**: A growing lag means your consumers cannot keep up with write speeds. This leads to latency in your processing pipeline.
* **How to monitor**: Use tools like **Prometheus + Grafana** querying JMX metrics from the brokers, or **Burrow** (an open-source lag monitor created by LinkedIn).

### Data Retention Configurations
Kafka is not a database designed for infinite storage (though Tiered Storage makes this possible). Production topics configure retention policies:
* `retention.ms`: How long data is kept (default is 7 days).
* `retention.bytes`: Maximum log size per partition before deleting old segments.
* **Log Compaction**: Instead of deleting by time, Kafka keeps only the **latest value for each key**. Ideal for database changelog streaming (CDC).
