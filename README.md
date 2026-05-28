# Kafka Playground

Hands-on Kafka learning environment with a Go backend, React dashboard, and single-broker Kafka cluster — all containerized and ready in one command.

## Architecture

```
┌─────────────────┐     WebSocket       ┌─────────────────┐
│   React SPA     │ ◄──────────────►    │   Go Backend    │
│   (Vite :5173)  │    REST API         │   (Gin :8081)   │
└─────────────────┘                     └────────┬────────┘
                                                 │
                                          segmentio/kafka-go
                                                 │
                                     ┌───────────▼───────────┐
                                     │   Kafka Broker :9092  │
                                     │   (KRaft, no ZK)      │
                                     └───────────┬───────────┘
                                                 │
                                     ┌───────────▼───────────┐
                                     │   Kafka UI :8080      │
                                     │   (cluster inspector) │
                                     └───────────────────────┘
```

## Quick Start

```bash
# Start Kafka + Kafka UI
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

## What You Can Do

### From the Dashboard
- Create and delete topics with custom partition/replication settings
- Publish messages with keys and headers (use the **Trigger Failure** template to test DLQ routing)
- Stream messages in real time via WebSocket (pick a topic, consumer group, and offset policy)
- Inspect broker metadata, partition leaders, and in-sync replicas
- Watch failed messages route to a **dead-letter queue** (`{topic}-dlq`) with tracing headers

### From the CLI (inside the broker container)

```bash
docker exec -it kafka-playground-broker bash

# List topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Create a topic manually
/opt/kafka/bin/kafka-topics.sh --create \
  --topic orders --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Produce messages interactively
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders --bootstrap-server localhost:9092

# Consume from the beginning
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders --from-beginning --bootstrap-server localhost:9092

# Check consumer group lag
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group my-service --describe
```

## API Reference

All endpoints are under `http://localhost:8081/api`.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metadata` | Cluster brokers, topics, partition leaders, ISR |
| `POST` | `/topics` | Create a topic `{name, partitions, replicationFactor}` |
| `DELETE` | `/topics/:name` | Delete a topic |
| `POST` | `/produce` | Publish a message `{topic, key?, value, headers?}` |
| `GET` | `/consume/ws?topic=&groupId=&offset=` | WebSocket stream of consumed messages |

### Example: Produce a message

```bash
curl -X POST http://localhost:8081/api/produce \
  -H "Content-Type: application/json" \
  -d '{"topic":"orders","key":"user-42","value":"order placed"}'
```

Response:
```json
{
  "status": "Message published",
  "partition": 1,
  "offset": 0
}
```

### Example: Stream messages over WebSocket

```js
const ws = new WebSocket("ws://localhost:8081/api/consume/ws?topic=orders&groupId=demo&offset=earliest")
ws.onmessage = (e) => console.log(JSON.parse(e.data))
```

Each event has a `type` field — `"success"` for normal processing or `"dlq"` when a message fails and gets routed to the dead-letter topic:

```json
// Success
{
  "type": "success",
  "message": { "partition": 0, "offset": 4, "key": "user-42", "value": "order placed", ... }
}

// Dead-lettered
{
  "type": "dlq",
  "message": { "partition": 1, "offset": 7, "key": "fail-user-99", "value": "{\"comment\":\"...contains fail...\"}", ... },
  "dlqTopic": "orders-dlq",
  "dlqPartition": 0,
  "dlqOffset": 0,
  "failureReason": "Simulated processing error: Payload validation failed"
}
```

## Project Structure

```
.
├── docker-compose.yml         # Kafka broker + Kafka UI
├── backend/
│   ├── main.go                # Gin HTTP server, API handlers, WebSocket
│   └── kafka/
│       ├── client.go          # Broker address constant
│       ├── producer.go        # Sync producer (Hash balancer, acks=1)
│       ├── consumer.go        # Consumer group reader with DLQ routing on failure
│       └── admin.go           # Metadata, create/delete topics
├── frontend/
│   └── src/                   # React + Vite + Lucide dashboard
└── docs/
    └── kafka-production-guide.md   # Deep dive: partitions, acks, lag, graceful shutdown
```

## Key Kafka Concepts at Play

| Concept | Where to see it |
|---------|----------------|
| **Partitioning** | Create a topic with 3+ partitions, produce messages with the same key — all land in the same partition |
| **Consumer groups** | Open two browser tabs streaming the same topic + group — messages split between them |
| **Offset management** | Reload the page with `offset=latest` vs `offset=earliest` — see the difference |
| **ISR / replication** | Check topic describe in Kafka UI — single broker means ISR list has 1 entry |
| **Producer acks** | Backend uses `RequireOne` — if the leader broker crashes before replicating, that message is lost |
| **Consumer lag** | Watch `kafka-consumer-groups.sh --describe` while producing faster than consuming |
| **Dead-letter queue** | Click **Trigger Failure** in the dashboard — message routes to `{topic}-dlq` with failure metadata in headers |

## Dependencies

- **Go backend**: [`segmentio/kafka-go`](https://github.com/segmentio/kafka-go), [`gin-gonic/gin`](https://github.com/gin-gonic/gin), [`gorilla/websocket`](https://github.com/gorilla/websocket)
- **Frontend**: React 19, Vite 8, Lucide React
- **Infra**: Apache Kafka (KRaft mode), Kafka UI
