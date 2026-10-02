# Apache Kafka & Outbox Publisher Architecture

## 1. Why Kafka Exists in This Architecture

In the **Event-Driven Order Fulfillment Platform**, services are decoupled through asynchronous event-driven choreography.

Rather than having the **Order Service** synchronously call downstream services (such as the upcoming Inventory Service) via HTTP or RPC during order creation:

- Synchronous calls couple availability: if the Inventory Service is slow or temporarily down, the client's order placement fails or times out.
- Synchronous orchestration creates tight temporal coupling and prevents independent service scaling.

**Apache Kafka** serves as the central distributed streaming backbone:

- **Decoupled Asynchrony**: The Order Service publishes domain events when business milestones occur without knowing or caring how many downstream consumers exist or when they process them.
- **Partitioned Total Ordering**: Kafka guarantees strict total ordering of messages within each partition. By partitioning on the aggregate ID (`orderId`), all events related to a specific order are consumed in the exact sequence they occurred.
- **Durable Replayability**: Events are persisted on disk across configurable retention windows, allowing downstream consumers to catch up after outages or replay streams for auditing.

---

## 2. Kafka Topic: `order.events`

In Phase 4, the platform introduces a single domain topic:

- **Topic Name**: `order.events`
- **Purpose**: Carries lifecycle domain events emitted by the Order Service.
- **Partitions**: Configured with 3 partitions in local development (`KAFKA_NUM_PARTITIONS=3`), enabling parallel consumption while maintaining deterministic key-based partition affinity.
- **Replication Factor**: 1 for local development.

---

## 3. Event Type: `OrderCreated`

Phase 4 publishes the initial domain event:

- **`eventType`**: `OrderCreated`
- **Trigger**: An order has been persisted into `orders` and `order_items` within the Order Service's database transaction.
- **Semantics**: Indicates that an order has been created with status `PENDING` and is awaiting downstream inventory processing.

---

## 4. Kafka Message Key: `orderId`

Every `OrderCreated` event published to Kafka has its message key explicitly set to:

```text
key = orderId (UUID)
```

### Why `orderId` is the Partition Key:

1. **Partition Affinity**: Kafka hashes message keys using Murmur2 to map messages to specific topic partitions (`hash(key) % num_partitions`).
2. **Per-Entity In-Order Delivery**: By using `orderId` as the key, all current and future events for that order (`OrderCreated`, `OrderInventoryReserved`, `OrderConfirmed`, `OrderCancelled`) are routed to the **same partition**.
3. **Consumer Concurrency Without Race Conditions**: A consumer group reading `order.events` will process all events for a given order sequentially on a single consumer thread, eliminating out-of-order state transitions.

---

## 5. Event Envelope Specification

The Kafka message value is serialized as a JSON string matching the Phase 3 application event envelope:

```json
{
  "eventId": "a1111111-1111-4111-8111-111111111111",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-10-02T12:00:00.000Z",
  "aggregateType": "Order",
  "aggregateId": "b2222222-2222-4222-8222-222222222222",
  "correlationId": "d4444444-4444-4444-8444-444444444444",
  "payload": {
    "orderId": "b2222222-2222-4222-8222-222222222222",
    "customerId": "c3333333-3333-4333-8333-333333333333",
    "items": [
      {
        "productId": "88888888-aaaa-4bbb-8ccc-000000000002",
        "quantity": 2
      }
    ]
  }
}
```

### Key Envelope Fields:

- `eventId`: Unique UUID for the event (matches `outbox_events.id`).
- `eventType`: String identifier for dispatching and deserialization (`"OrderCreated"`).
- `eventVersion`: Schema version (starts at `1`) for future event schema evolution.
- `occurredAt`: ISO 8601 UTC timestamp of creation.
- `aggregateType`: Root aggregate type (`"Order"`).
- `aggregateId`: Aggregate root identifier (`orderId`).
- `correlationId`: End-to-end tracing correlation identifier.
- `payload`: Minimal data payload containing only what downstream inventory processing needs (`orderId`, `customerId`, `items`). Deliberately excludes monetary fields (`unitPrice`, `totalAmount`) to maintain clean bounded contexts and prevent floating-point precision hazards.

---

## 6. Outbox → Kafka Flow & Architecture

```text
                       ┌───────────────────┐
                       │   API Gateway     │
                       │     GraphQL       │
                       └─────────┬─────────┘
                                 │ HTTP POST
                                 ▼
                       ┌───────────────────┐
                       │   Order Service   │
                       │                   │
                       │ Order API         │
                       │ Order Logic       │
                       │ Outbox Publisher  │
                       │ Kafka Producer    │
                       └─────────┬─────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                    ▼                         ▼
             ┌─────────────┐          ┌──────────────┐
             │ PostgreSQL  │          │    Kafka     │
             │             │          │              │
             │ order_db    │          │ order.events │
             │             │          │              │
             │ orders      │          └───────┬──────┘
             │ order_items │                  │
             │ outbox      │                  │
             └─────────────┘                  │
                                              ▼
                                      Inventory Service
                                        (Phase 5)
```

1. **Transactional Insertion**: The client places an order via GraphQL. Order Service inserts records into `orders`, `order_items`, and `outbox_events` in **ONE PostgreSQL transaction** (`BEGIN` ... `COMMIT`).
2. **Decoupled Boundary**: Kafka does **NOT** participate in the database transaction. If Kafka is down, the order creation still commits reliably.
3. **Asynchronous Polling**: The in-process `OutboxPublisher` polls `outbox_events` for rows `WHERE published_at IS NULL`.
4. **Broker Dispatch**: For each unpublished event, the publisher serializes the envelope and publishes it to `order.events` via `KafkaOrderProducer`.
5. **State Finalization**: Upon receiving Kafka's acknowledgment, the publisher updates `published_at = NOW()`.

---

## 7. Outbox Publisher Responsibilities

The `OutboxPublisher` (`apps/order-service/src/messaging/outbox.publisher.ts`) manages:

1. **Polling**: Executes polling cycles at configurable intervals (`OUTBOX_POLL_INTERVAL_MS`).
2. **Batch Processing**: Retrieves up to `OUTBOX_BATCH_SIZE` unpublished events ordered chronologically (`created_at ASC`) using the partial index `idx_outbox_events_unpublished`.
3. **Topic Mapping**: Maps `eventType` to the corresponding Kafka topic via `EVENT_TOPIC_MAPPING`:
   - `OrderCreated` → `order.events`
4. **Acknowledgment Handling**: Only marks an event as published (`markAsPublished(eventId)`) after the broker returns positive confirmation.
5. **Retry Management**: On any publishing failure, increments `retry_count` and leaves `published_at = NULL` for the next cycle.
6. **Graceful Shutdown**: On process termination (`SIGINT`/`SIGTERM`), ceases scheduling, waits for in-flight polling batches to complete, and cleanly disconnects the Kafka producer.

---

## 8. Retry Behavior & Fault Tolerance

```text
Publisher Polling Cycle
          ↓
Query unpublished events (LIMIT batchSize)
          ↓
Attempt Kafka producer dispatch
          ↓
     Success?
     ├── YES ──► UPDATE outbox_events SET published_at = NOW()
     └── NO  ──► UPDATE outbox_events SET retry_count = retry_count + 1
```

If Kafka experiences downtime:

- Events accumulate safely in `outbox_events`.
- In each cycle, the publisher attempts publication, logs structured diagnostics with `{ eventId, eventType, aggregateId, retryCount, error }`, and increments `retry_count`.
- Zero data is lost.
- As soon as Kafka recovers, the next polling cycle dispatches accumulated events in FIFO order and marks them published.

---

## 9. At-Least-Once Delivery Semantics

The architecture guarantees **at-least-once delivery**:

- Events are never dropped because they remain in PostgreSQL until published.
- In distributed systems, exactly-once delivery across independent network boundaries without 2PC (two-phase commit) is impossible.
- Therefore, the system acknowledges that duplicate delivery can occur under specific crash scenarios.

---

## 10. Duplicate Publication Possibility

Consider this failure scenario:

1. `OutboxPublisher` sends `OrderCreated` message to Kafka.
2. Kafka accepts and commits the message to partition log.
3. Network blip or node crash occurs **before** `UPDATE outbox_events SET published_at = NOW()` completes in PostgreSQL.
4. When the Order Service restarts, the event row still has `published_at = NULL`.
5. The publisher polls the row and publishes it to Kafka a second time.

Result: Two identical `OrderCreated` events exist on `order.events` with the same `eventId` and `correlationId`.

---

## 11. Why Exactly-Once is Not Assumed

Kafka supports transactional producers (`initTransactions`, `sendOffsetsToTransaction`), but transactions spanning **PostgreSQL and Kafka** cannot be coordinated atomically without heavy two-phase commit protocols that drastically reduce availability and throughput.

Assuming "exactly-once" transport leads to fragile systems. Instead, reliable architectures embrace at-least-once publishing combined with **consumer-side idempotency**.

---

## 12. Why Consumer Idempotency Will Be Needed Later

In **Phase 5**, the downstream **Inventory Service** will consume events from `order.events`.

Because duplicate `OrderCreated` events are possible, the Inventory Service must track processed message IDs:

- Maintain an `inbox_events` table or idempotency check on `eventId`.
- Before allocating stock, check if `eventId` was already processed.
- If already processed, acknowledge the Kafka offset without re-allocating inventory.

_(Consumer idempotency belongs to Phase 5 and is not implemented in Phase 4.)_

---

## 13. Local Kafka Setup (KRaft Mode via Docker Compose)

The local Kafka environment is configured using KRaft (Kafka Raft Metadata mode) without ZooKeeper via [docker-compose.yml](file:///c:/Users/varch/OneDrive/Desktop/coding/My%20Project/event-driven-order-platform/docker-compose.yml):

```yaml
services:
  postgres:
    image: postgres:17
    container_name: event-platform-postgres
    ports:
      - '5432:5432'
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: local_password
      POSTGRES_DB: order_db

  kafka:
    image: apache/kafka:3.8.0
    container_name: event-platform-kafka
    ports:
      - '9092:9092'
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
```

### Commands:

```bash
# Start infrastructure
docker compose up -d

# Verify Kafka health
docker exec event-platform-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# List Kafka topics
docker exec event-platform-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume messages from order.events
docker exec event-platform-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order.events --from-beginning --property print.key=true
```

---

## 14. Environment Variables

| Variable                  | Default          | Description                                                      |
| :------------------------ | :--------------- | :--------------------------------------------------------------- |
| `KAFKA_BROKERS`           | `localhost:9092` | Comma-separated list of Kafka broker seed addresses              |
| `KAFKA_CLIENT_ID`         | `order-service`  | Kafka client identifier for logging and monitoring               |
| `OUTBOX_POLL_INTERVAL_MS` | `1000`           | Frequency in milliseconds between outbox polling sweeps          |
| `OUTBOX_BATCH_SIZE`       | `100`            | Maximum number of unpublished events processed per polling sweep |
