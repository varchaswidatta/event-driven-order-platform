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

## 2. Kafka Topics

The platform operates two core domain event topics:

### A. `order.events`

- **Purpose**: Carries lifecycle domain events emitted by the Order Service.
- **Partitions**: Configured with 3 partitions in local development (`KAFKA_NUM_PARTITIONS=3`), enabling parallel consumption while maintaining deterministic key-based partition affinity.
- **Replication Factor**: 1 for local development.
- **Publisher**: Order Service (`OutboxPublisher`).
- **Consumer Group**: `inventory-service` (Inventory Service).

### B. `inventory.events` (Phase 7)

- **Purpose**: Carries stock reservation outcome events emitted by the Inventory Service.
- **Partitions**: Configured with 3 partitions in local development (`KAFKA_NUM_PARTITIONS=3`).
- **Replication Factor**: 1 for local development.
- **Publisher**: Inventory Service (`InventoryOutboxPublisher`).
- **Consumer Group**: `order-service` (Order Service).

---

## 3. Domain Event Specifications

### A. `OrderCreated`

- **Topic**: `order.events`
- **Trigger**: An order has been persisted into `orders` and `order_items` within the Order Service's database transaction.
- **Partition Key**: `orderId` (UUID)
- **Semantics**: Indicates that an order has been created with status `PENDING` and is awaiting downstream inventory processing.

### B. `InventoryReserved` (Phase 7)

- **Topic**: `inventory.events`
- **Trigger**: Stock Service successfully reserved stock via gRPC, and Inventory Service atomically recorded `RESERVED` status and outbox event in `inventory_db`.
- **Partition Key**: `orderId` (UUID)
- **Semantics**: Confirms inventory allocation for the order. Order Service transitions order status to `CONFIRMED`.

### C. `InventoryReservationFailed` (Phase 7)

- **Topic**: `inventory.events`
- **Trigger**: Stock Service rejected reservation due to business logic (e.g., `INSUFFICIENT_STOCK`), and Inventory Service atomically recorded `FAILED` status and outbox event in `inventory_db`.
- **Partition Key**: `orderId` (UUID)
- **Semantics**: Informs that stock could not be reserved. Order Service transitions order status to `INVENTORY_FAILED`.

---

## 4. Kafka Message Key: `orderId`

Every event published to either `order.events` or `inventory.events` has its message key explicitly set to:

```text
key = orderId (UUID)
```

### Why `orderId` is the Partition Key:

1. **Partition Affinity**: Kafka hashes message keys using Murmur2 to map messages to specific topic partitions (`hash(key) % num_partitions`).
2. **Per-Entity In-Order Delivery**: By using `orderId` as the key, all current and future events for that order across both topics are routed to the **same partition**.
3. **Consumer Concurrency Without Race Conditions**: A consumer group reading `order.events` or `inventory.events` will process all events for a given order sequentially on a single consumer thread, eliminating out-of-order state transitions.

---

## 5. Event Envelope Specifications

All messages across topics adhere to the standard `EventEnvelope<T>` schema.

### A. `OrderCreated` Envelope

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

### B. `InventoryReserved` Envelope (Phase 7)

```json
{
  "eventId": "e5555555-5555-4555-8555-555555555555",
  "eventType": "InventoryReserved",
  "eventVersion": 1,
  "occurredAt": "2026-10-04T12:00:01.000Z",
  "aggregateType": "InventoryReservation",
  "aggregateId": "b2222222-2222-4222-8222-222222222222",
  "correlationId": "d4444444-4444-4444-8444-444444444444",
  "payload": {
    "orderId": "b2222222-2222-4222-8222-222222222222",
    "reservationId": "f6666666-6666-4666-8666-666666666666",
    "reservedItems": [
      {
        "productId": "88888888-aaaa-4bbb-8ccc-000000000002",
        "quantity": 2
      }
    ]
  }
}
```

### C. `InventoryReservationFailed` Envelope (Phase 7)

```json
{
  "eventId": "e7777777-7777-4777-8777-777777777777",
  "eventType": "InventoryReservationFailed",
  "eventVersion": 1,
  "occurredAt": "2026-10-04T12:00:01.000Z",
  "aggregateType": "InventoryReservation",
  "aggregateId": "b2222222-2222-4222-8222-222222222222",
  "correlationId": "d4444444-4444-4444-8444-444444444444",
  "payload": {
    "orderId": "b2222222-2222-4222-8222-222222222222",
    "reason": "INSUFFICIENT_STOCK",
    "details": "Insufficient stock for product 88888888-aaaa-4bbb-8ccc-000000000002"
  }
}
```

---

## 6. End-to-End Choreography Flow & Architecture

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
                        │ Order Repository  │
                        │ Outbox Publisher  │
                        │ Inventory Consumer│
                        └─────┬───────▲─────┘
                 Postgres ACID│       │
              (orders+outbox) │       │ Kafka
                              ▼       │ inventory.events
                       ┌──────────────┴───────┐
                       │        Kafka         │
                       │                      │
                       │ 1. order.events      │
                       │ 2. inventory.events  │
                       └──────▲───────┬───────┘
                 Kafka order. │       │
                       events │       │ Postgres ACID
                              ▼       │ (reservations+outbox)
                        ┌─────┴───────┴─────┐
                        │ Inventory Service │
                        │                   │
                        │ Order Consumer    │
                        │ Outbox Publisher  │
                        │ gRPC Client       │
                        └─────────┬─────────┘
                                  │
                                  │ gRPC ReserveStock
                                  ▼
                        ┌───────────────────┐
                        │   Stock Service   │
                        │  (gRPC Server +   │
                        │   stock_db)       │
                        └───────────────────┘
```

1. **Order Placement**: Client sends `createOrder` mutation. Order Service persists order + outbox event `OrderCreated` atomically in `order_db`.
2. **Order Event Publishing**: Order Service `OutboxPublisher` polls `order_db.outbox_events` and dispatches to `order.events` (key: `orderId`).
3. **Inventory Consumption**: Inventory Service (`inventory-service` consumer group) consumes `OrderCreated`.
4. **Synchronous Stock Reservation**: Inventory Service calls Stock Service via gRPC `ReserveStock`.
5. **Inventory Outbox Insertion**: In a single PostgreSQL transaction on `inventory_db`, Inventory Service updates reservation status (`RESERVED` or `FAILED`) and inserts an outbox event (`InventoryReserved` or `InventoryReservationFailed`).
6. **Inventory Event Publishing**: Inventory Service `InventoryOutboxPublisher` polls `inventory_db.outbox_events` and dispatches to `inventory.events` (key: `orderId`).
7. **Order Result Consumption**: Order Service (`order-service` consumer group) consumes `inventory.events` and transitions `orders.status` to `CONFIRMED` or `INVENTORY_FAILED`.

---

## 7. Outbox Publishers

Both services utilize dedicated, in-process outbox publishers:

1. **Order Service Outbox Publisher** (`apps/order-service/src/messaging/outbox.publisher.ts`):
   - Polls `order_db.outbox_events` (`WHERE published_at IS NULL`).
   - Maps `OrderCreated` → `order.events`.
   - Partition key: `orderId`.

2. **Inventory Service Outbox Publisher** (`apps/inventory-service/src/messaging/outbox.publisher.ts`):
   - Polls `inventory_db.outbox_events` (`WHERE published_at IS NULL`).
   - Maps `InventoryReserved` and `InventoryReservationFailed` → `inventory.events`.
   - Partition key: `orderId`.

Both publishers share identical fault tolerance patterns:

- Exponential/interval-based polling cycles (`OUTBOX_POLL_INTERVAL_MS`).
- Batching (`OUTBOX_BATCH_SIZE`) via partial indexes (`WHERE published_at IS NULL`).
- Acknowledgment before marking `published_at = NOW()`.
- Error increments `retry_count` and leaves `published_at = NULL` for re-attempt.
- Clean shutdown on `SIGINT`/`SIGTERM`.

---

## 8. Consumer Groups & Idempotency

### A. Inventory Service Consumer (`inventory-service` group)

- Subscribes to: `order.events`
- Idempotency Strategy: Database uniqueness constraint on `inventory_reservations(order_id UNIQUE)`.
- Re-delivered `OrderCreated` events hit the unique constraint and are safely treated as idempotent duplicates without re-allocating stock.

### B. Order Service Consumer (`order-service` group)

- Subscribes to: `inventory.events`
- Idempotency Strategy: Explicit state machine transition guard in `OrderRepository.updateOrderStatus`:
  - `PENDING` / `INVENTORY_PROCESSING` → `CONFIRMED` or `INVENTORY_FAILED` (valid transition).
  - Already `CONFIRMED` or `INVENTORY_FAILED` → Returns `{ alreadyUpdated: true }` without mutation (safe idempotent replay).
  - Missing order (`OrderNotFoundError`) → Safely acknowledged with a warning (prevents blocking partition on orphaned messages).
  - Database connection errors → Re-thrown to trigger Kafka consumer retry and prevent message loss.

---

## 9. Error Classification: Business Failures vs Infrastructure Retries

| Scenario                | Classification         | Kafka Action                     | System Outcome                                                                                         |
| :---------------------- | :--------------------- | :------------------------------- | :----------------------------------------------------------------------------------------------------- |
| **Insufficient Stock**  | Business Failure       | Acknowledge `order.events`       | Publishes `InventoryReservationFailed` to `inventory.events`. Order status becomes `INVENTORY_FAILED`. |
| **Stock Service Down**  | Infrastructure Failure | Throw error / Do NOT acknowledge | Kafka consumer retries message redelivery. No failure event published prematurely.                     |
| **Database Disconnect** | Infrastructure Failure | Throw error / Do NOT acknowledge | Consumer retries until database connectivity resumes.                                                  |
| **Malformed Envelope**  | Poison Pill            | Acknowledge & Skip               | Logged as error; consumer does not deadlock.                                                           |

---

## 10. Local Kafka Setup (KRaft Mode via Docker Compose)

The local Kafka environment is configured using KRaft (Kafka Raft Metadata mode) without ZooKeeper via `docker-compose.yml`:

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

### CLI Verification Commands:

```bash
# List Kafka topics
docker exec event-platform-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Consume order events
docker exec event-platform-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order.events --from-beginning --property print.key=true

# Consume inventory events
docker exec event-platform-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic inventory.events --from-beginning --property print.key=true
```

---

## 11. Environment Variables

| Variable                  | Service           | Default             | Description                                         |
| :------------------------ | :---------------- | :------------------ | :-------------------------------------------------- |
| `KAFKA_BROKERS`           | All               | `localhost:9092`    | Comma-separated list of Kafka broker seed addresses |
| `KAFKA_CLIENT_ID`         | Order Service     | `order-service`     | Kafka client identifier for logging and monitoring  |
| `KAFKA_CLIENT_ID`         | Inventory Service | `inventory-service` | Kafka client identifier for logging and monitoring  |
| `KAFKA_GROUP_ID`          | Order Service     | `order-service`     | Consumer group for consuming `inventory.events`     |
| `KAFKA_GROUP_ID`          | Inventory Service | `inventory-service` | Consumer group for consuming `order.events`         |
| `OUTBOX_POLL_INTERVAL_MS` | Order / Inventory | `1000`              | Frequency in ms between outbox polling sweeps       |
| `OUTBOX_BATCH_SIZE`       | Order / Inventory | `100`               | Max unpublished events processed per polling sweep  |
