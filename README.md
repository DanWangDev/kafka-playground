# Kafka Playground

Hands-on Kafka learning environment with a Go backend, React dashboard, and 3-broker KRaft cluster — all containerized and ready in one command.

## Architecture

```
┌─────────────────┐     WebSocket      ┌─────────────────┐
│   React SPA     │ ◄──────────────► │   Go Backend     │
│   (Vite :5173)  │    REST API       │   (Gin :8081)    │
└─────────────────┘                   └────────┬────────┘
                                               │
                                        segmentio/kafka-go
                                               │
                          ┌────────────────────┼────────────────────┐
                          │                    │                    │
                    ┌─────▼─────┐      ┌──────▼──────┐      ┌─────▼─────┐
                    │ Broker 1  │      │  Broker 2   │      │ Broker 3  │
                    │  :9092    │      │   :9093     │      │  :9094    │
                    │ Controller│      │  Controller │      │ Controller│
                    └───────────┘      └─────────────┘      └───────────┘
                          │                    │                    │
                          └────────────────────┼────────────────────┘
                                               │
                                    ┌──────────▼──────────┐
                                    │   Kafka UI :8080     │
                                    │   (cluster inspector)│
                                    └──────────────────────┘
```

## Quick Start

```bash
# Start Kafka cluster (3 brokers) + Kafka UI
docker compose up -d

# Start the Go backend
cd backend && go run main.go

# Start the React frontend (separate terminal)
cd frontend && npm install && npm run dev
```

Then open:
- **Frontend dashboard** — http://localhost:5173
- **Kafka UI** — http://localhost:8080
- **Go API** — http://localhost:8081/api

## Feature Tour

### Dashboard

| Section | What it teaches |
|---------|----------------|
| **Topic Admin** | Create topics with configurable partitions, replication factor, and retention/compaction policies |
| **Producer Studio** | Publish messages with keys (determine partition placement) and custom headers. Templates for common payloads and DLQ trigger |
| **Producer Benchmark** | Compare throughput/latency across compression codecs (gzip/snappy/lz4/none), acks levels (0/1/all), and batch sizes |
| **Lag Simulator** | Flood a topic at a configurable rate, watch consumer lag climb, then stop and see it drain |
| **Consumer Group Rebalancing** | Live partition ownership grid. Open multiple tabs in the same group — partitions auto-distribute. Close one — rebalance |
| **Offset Management** | Rewind offsets to earliest/latest. Inspect per-partition lag. Understand that offsets are just consumer-position pointers |
| **Topology Visualizer** | Real-time broker list, KRaft controller ID, partition leaders, and ISR (in-sync replicas) per topic |
| **Consumer Console** | WebSocket stream of messages with partition, offset, key, headers. Filter in real time. DLQ events highlighted in red |

### CLI Quick Reference

```bash
docker exec -it kafka-playground-broker-1 bash

# List topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Create a topic (RF=3 now possible with 3 brokers)
/opt/kafka/bin/kafka-topics.sh --create \
  --topic orders --partitions 6 --replication-factor 3 \
  --bootstrap-server localhost:9092

# Describe — see leaders, replicas, ISR across brokers
/opt/kafka/bin/kafka-topics.sh --describe \
  --topic orders --bootstrap-server localhost:9092

# Produce/consume interactively
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders --from-beginning --bootstrap-server localhost:9092

# Check consumer group lag
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my-service --describe

# Simulate broker failure
docker stop kafka-playground-broker-3
# Watch Kafka UI — ISR shrinks, leader fails over if needed
docker start kafka-playground-broker-3
```

## API Reference

All endpoints under `http://localhost:8081/api`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metadata` | Cluster brokers, topics, partition leaders, ISR |
| `POST` | `/topics` | Create a topic `{name, partitions, replicationFactor, configs?}` |
| `DELETE` | `/topics/:name` | Delete a topic |
| `POST` | `/topics/:name/config` | Set topic configs (retention.ms, cleanup.policy, etc.) |
| `GET` | `/topics/:name/config` | Get topic configs |
| `POST` | `/produce` | Publish a single message `{topic, key?, value, headers?}` |
| `POST` | `/produce/batch` | Batch produce N messages — returns throughput/latency |
| `POST` | `/stress/start` | Start background stress test at target rate |
| `POST` | `/stress/stop` | Stop stress test |
| `GET` | `/stress` | Live stress test status (messages sent, rate) |
| `GET` | `/consume/ws?topic=&groupId=&offset=` | WebSocket stream of consumed messages |
| `GET` | `/consumers` | List registered consumers |
| `GET` | `/consumers/groups` | List consumer groups (from broker) |
| `GET` | `/consumers/groups/:groupId` | Consumer group partition assignments + lag |
| `POST` | `/consumers/groups/:groupId/reset` | Reset offsets `{topic, partition?, offset?, target}` |

### Example: Batch produce (benchmark)

```bash
curl -X POST http://localhost:8081/api/produce/batch \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","count":5000,"batchSize":100,"compression":"lz4","acks":"1"}'
```

```json
{
  "messagesSent": 5000,
  "totalDuration": "1.234s",
  "messagesPerSec": 4051.87,
  "avgLatencyMs": 0.247
}
```

### Example: Create a compacted topic

```bash
curl -X POST http://localhost:8081/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"user-state","partitions":3,"replicationFactor":3,"configs":{"cleanup.policy":"compact"}}'
```

### Example: Rewind a consumer group

```bash
curl -X POST http://localhost:8081/api/consumers/groups/playground-group/reset \
  -H "Content-Type: application/json" \
  -d '{"target":"earliest"}'
```

### Consumer WebSocket event format

```json
// Success
{ "type": "success", "message": { "partition": 0, "offset": 4, "key": "user-42", "value": "...", ... } }

// Dead-lettered
{ "type": "dlq", "message": { ... }, "dlqTopic": "orders-dlq", "dlqPartition": 0, "dlqOffset": 0, "failureReason": "..." }
```

## Project Structure

```
.
├── docker-compose.yml              # 3-broker KRaft cluster + Kafka UI
├── backend/
│   ├── main.go                     # Gin HTTP server, API handlers, WebSocket
│   └── kafka/
│       ├── client.go               # Bootstrap broker address list
│       ├── producer.go             # Sync/async producer, batch benchmark
│       ├── consumer.go             # Consumer group reader with DLQ routing
│       ├── admin.go                # Metadata, topic CRUD, topic configs
│       ├── consumers.go            # Consumer group registry, describe, offset reset
│       └── stress.go               # Background stress test producer
├── frontend/
│   └── src/                        # React 19 + Vite 8 + Lucide dashboard
└── docs/
    └── kafka-production-guide.md   # Partitions, acks semantics, graceful shutdown
```

## Key Kafka Concepts at Play

| Concept | How to observe it |
|---------|-------------------|
| **Partitioning** | Produce messages with the same key — all land in the same partition via hash routing |
| **Consumer groups** | Open two browser tabs with same topic + group — messages split. Close one — rebalance |
| **Offset management** | Rewind a group to earliest — consumer reprocesses everything. Kafka never deletes messages |
| **ISR / replication** | Create topic with RF=3, check Kafka UI. `docker stop` a broker, watch ISR shrink |
| **Leader election** | Stop the leader broker — controller promotes a follower. Transparent to producers/consumers |
| **Producer acks** | Benchmark tool: compare `acks=0` (fast, lossy) vs `acks=all` (slow, durable) |
| **Consumer lag** | Start stress test while consumer is running — lag climbs. Stop — lag drains |
| **Retention** | Create topic with `retention.ms=10000`, produce messages, wait 10s — they disappear |
| **Log compaction** | Create compacted topic, produce 3 messages with same key — only latest is retained |
| **Dead-letter queue** | Click **Trigger Failure** — message routes to `{topic}-dlq` with tracing headers |
| **Compression** | Benchmark with gzip vs lz4 vs none — see throughput difference at cost of CPU |

## Dependencies

- **Go backend**: [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go), [`gin-gonic/gin`](https://github.com/gin-gonic/gin), [`gorilla/websocket`](https://github.com/gorilla/websocket)
- **Frontend**: React 19, Vite 8, Lucide React
- **Infra**: Apache Kafka 4.x (KRaft mode), Kafka UI
