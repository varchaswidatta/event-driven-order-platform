# Transactional Outbox Pattern — Order Service

## 1. Overview & Purpose

In a distributed microservice architecture, a service often needs to update its local database and notify external services via an event broker (such as Apache Kafka).

A naive implementation attempts dual writes:

```text
1. INSERT INTO orders ...;
2. kafkaProducer.send(event); // If this fails or crashes, database state and broker state diverge!
```

Dual writes inevitably lead to inconsistencies due to network partitions, broker downtime, or application crashes between the two operations.

The **Transactional Outbox pattern** solves the dual-write problem by persisting domain state and the corresponding integration event **within the same ACID database transaction**:

```text
GraphQL Client
      ↓
API Gateway
      ↓ HTTP
Order Service
      ↓
┌───────────────────────────────────────────────┐
│ PostgreSQL Transaction (ACID)                 │
│                                               │
│   1. INSERT orders                            │
│   2. INSERT order_items                       │
│   3. INSERT outbox_events (OrderCreated)      │
└───────────────────────────────────────────────┘
```

If the transaction commits, both the order data and the `OrderCreated` outbox event are guaranteed to be durable. If any operation fails, the transaction is rolled back completely via PostgreSQL `ROLLBACK`, leaving zero orphan data in any table.

---

## 2. Why Kafka is NOT Part of Phase 3

Phase 3 is purposefully scoped to establish the **transactional outbox persistence foundation**.

Publishing events directly to Kafka during this phase is intentionally excluded:

- It isolates database transaction orchestration from external messaging infrastructure.
- It proves transactional atomicity in PostgreSQL without coupling to a broker.
- Phase 4 will introduce the asynchronous outbox publisher (poller/CDC) that reads unpublished events from `outbox_events` and streams them reliably to Kafka.

---

## 3. Database Schema: `outbox_events`

The `outbox_events` table is created via SQL migration `002_create_outbox_events.sql` inside the Order Service database (`order_db`):

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type VARCHAR(64) NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type VARCHAR(128) NOT NULL,
  event_version INTEGER NOT NULL DEFAULT 1,
  payload JSONB NOT NULL,
  correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ NULL,
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0)
);

-- Partial index for high-throughput FIFO retrieval of unpublished events
CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished
  ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;
```

### Column Specifications:

| Column           | Type           | Constraints                                     | Description                                                                           |
| :--------------- | :------------- | :---------------------------------------------- | :------------------------------------------------------------------------------------ |
| `id`             | `UUID`         | `PRIMARY KEY, DEFAULT gen_random_uuid()`        | Unique event identifier (`eventId`).                                                  |
| `aggregate_type` | `VARCHAR(64)`  | `NOT NULL`                                      | Root entity type (e.g. `'Order'`).                                                    |
| `aggregate_id`   | `UUID`         | `NOT NULL`                                      | ID of the aggregate instance (the order ID).                                          |
| `event_type`     | `VARCHAR(128)` | `NOT NULL`                                      | Domain event name (e.g. `'OrderCreated'`).                                            |
| `event_version`  | `INTEGER`      | `NOT NULL, DEFAULT 1`                           | Event schema version for evolutionary compatibility.                                  |
| `payload`        | `JSONB`        | `NOT NULL`                                      | Serialized domain event payload for downstream consumers.                             |
| `correlation_id` | `UUID`         | `NOT NULL, DEFAULT gen_random_uuid()`           | Request correlation identifier for end-to-end tracing.                                |
| `created_at`     | `TIMESTAMPTZ`  | `NOT NULL, DEFAULT NOW()`                       | Event generation timestamp.                                                           |
| `published_at`   | `TIMESTAMPTZ`  | `NULL`                                          | Timestamp when publisher successfully dispatches to broker; `NULL` while unpublished. |
| `retry_count`    | `INTEGER`      | `NOT NULL, DEFAULT 0, CHECK (retry_count >= 0)` | Number of failed publish attempts.                                                    |

### Partial Index Performance:

The partial index `idx_outbox_events_unpublished` indexes only records `WHERE published_at IS NULL` sorted by `created_at ASC`. Because published events remain with `published_at != NULL`, this index stays extremely compact, ensuring low-latency retrieval for the future publisher without scanning historical published events.

---

## 4. Application Event Envelope

Every event produced by the Order Service adheres to a standardized, typed event envelope:

```json
{
  "eventId": "a7b3c2d1-0000-4000-8000-000000000001",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-10-02T12:00:00.000Z",
  "aggregateType": "Order",
  "aggregateId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "correlationId": "b1c2d3e4-1111-4111-a111-111111111111",
  "payload": {
    "orderId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "customerId": "00000000-0000-4000-a000-000000000001",
    "items": [
      {
        "productId": "00000000-0000-4000-b000-000000000001",
        "quantity": 2
      },
      {
        "productId": "00000000-0000-4000-b000-000000000002",
        "quantity": 1
      }
    ]
  }
}
```

### Event Payload Design & Safe Money Boundaries:

The payload is strictly tailored for downstream services (specifically the upcoming **Inventory Service**):

- Contains only business data necessary for inventory reservation: `orderId`, `customerId`, and line items (`productId`, `quantity`).
- **NO monetary values** (`unitPrice`, `totalAmount`) are exposed in `OrderCreated` payload. Inventory reservation only requires product counts; omitting pricing prevents leaking Order Service monetary schemas and eliminates floating-point representation hazards.
- Internal database row IDs (`order_items.id`) are excluded; only the domain `productId` is exposed.

---

## 5. Transaction Boundary & Atomicity

The order creation operation is orchestrated in `OrderRepository.createOrder` using a dedicated client checked out from the connection pool:

```text
BEGIN
  1. INSERT INTO orders ...
  2. INSERT INTO order_items ... (for each item)
  3. INSERT INTO outbox_events ... (OrderCreated event)
COMMIT
```

### Failure Handling:

- If inserting the order, any line item, or the outbox event fails, PostgreSQL executes `ROLLBACK`.
- The client connection is guaranteed to be released back to the pool in a `finally` block.
- Neither the order nor the outbox event persists on rollback.

---

## 6. Outbox Repository Abstraction

Persistence operations for the outbox are encapsulated in `IOutboxRepository` / `OutboxRepository` (`apps/order-service/src/repositories/outbox.repository.ts`):

- `insertEvent(data, client?)`: Inserts an outbox record. Accepts an optional `PoolClient` to participate seamlessly in an outer transaction.
- `findUnpublishedEvents(limit)`: Retrieves unpublished events ordered chronologically for FIFO processing.
- `markAsPublished(id, publishedAt?)`: Updates `published_at` upon successful publishing.
- `incrementRetryCount(id)`: Increments `retry_count` after a transient publishing failure.
- `findById(id)`: Fetches an outbox record by primary key.
- `findByAggregateId(aggregateId)`: Fetches all outbox events for a given aggregate.

---

## 7. Next Phase: Phase 4 Kafka Publisher

In **Phase 4**, a background worker or polling publisher will read from `outbox_events`:

1. Query unpublished events: `findUnpublishedEvents(batchSize)`.
2. Produce each event to Kafka topic `orders.events` partitioned by `aggregateId` (`orderId`) for partition-ordered consumption.
3. Upon broker acknowledgment, mark event as published: `markAsPublished(eventId)`.
4. If broker dispatch fails, increment `retry_count`: `incrementRetryCount(eventId)`.
