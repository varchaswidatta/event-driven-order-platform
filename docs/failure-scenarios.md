# Failure Scenarios & Resilience Patterns

This document details the system failure modes, fault-tolerance mechanisms, and disaster-recovery patterns implemented across the Event-Driven Order Fulfillment Platform.

---

## 1. Dual-Write Failure Prevention (Transactional Outbox Pattern)

### The Dual-Write Problem

In distributed systems, updating a database and publishing a message to a broker (e.g. Apache Kafka) without two-phase commit (2PC) inherently risks inconsistency:

- If the database transaction commits first but Kafka publish fails or times out, the message is lost forever (the order exists, but downstream services are never notified).
- If Kafka publish happens first but the database transaction fails or rolls back, a phantom event is broadcast to the platform (downstream services reserve stock for an order that does not exist).

### The Solution: Transactional Outbox

Both **Order Service** and **Inventory Service** eliminate dual-write hazards via the Transactional Outbox Pattern:

1. **Order Service Outbox**:
   - Order creation (`orders`, `order_items`) and domain event insertion (`outbox_events` with `OrderCreated`) occur within **one ACID PostgreSQL transaction** (`order_db`).
   - If anything fails, the entire transaction is rolled back via `ROLLBACK`.
   - An asynchronous in-process poller (`OutboxPublisher`) periodically scans for unpublished events (`WHERE published_at IS NULL`), publishes them to `order.events`, and marks `published_at = NOW()`.

2. **Inventory Service Outbox**:
   - Upon receiving the result of a stock reservation from Stock Service via gRPC, Inventory Service updates the reservation status (`RESERVED` or `FAILED`) and inserts the domain event (`InventoryReserved` or `InventoryReservationFailed` into `inventory_db.outbox_events`) in a **single ACID PostgreSQL transaction**.
   - The `InventoryOutboxPublisher` polls `inventory_db.outbox_events`, dispatches to `inventory.events`, and marks `published_at = NOW()`.

---

## 2. Broker & Infrastructure Outages

### Kafka Downtime

When Kafka brokers are unavailable (network partition, node crash, leader re-election):

- **Order Placement Unaffected**: Clients can continue creating orders via GraphQL. Orders commit to `order_db` and outbox events safely accumulate.
- **Publisher Resilience**:
  - `OutboxPublisher` catches broker connection exceptions.
  - Increments `retry_count` on failed event records.
  - Leaves `published_at = NULL`.
  - Emits structured error logs.
  - Automatically resumes and dispatches backlogged events in FIFO order as soon as Kafka connectivity returns.
- **Zero Data Loss**: Domain events are persisted on durable PostgreSQL disk storage until Kafka explicitly acknowledges receipt.

### Database Downtime

- If `order_db` or `inventory_db` is unreachable:
  - Consumer message processing fails with a database connection error.
  - The consumer catches the database error and **re-throws it** to the KafkaJS runner.
  - The consumer does **NOT** commit the offset.
  - Kafka consumer group pauses/retries redelivery according to consumer backoff until the database is restored.

---

## 3. Business Failures vs Infrastructure Failures

A critical architectural distinction in Phase 7 is the separation between **business failures** (expected domain outcomes) and **infrastructure failures** (transient system faults):

| Scenario                    | Classification         | Mechanism                                                          | Consumer Action                   | Platform Outcome                                                                                                                     |
| :-------------------------- | :--------------------- | :----------------------------------------------------------------- | :-------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------- |
| **Insufficient Stock**      | Business Failure       | Stock Service returns `success: false, reason: INSUFFICIENT_STOCK` | Acknowledge `order.events` offset | Inventory Service records `FAILED`, inserts `InventoryReservationFailed` outbox event. Order Service marks order `INVENTORY_FAILED`. |
| **Stock Service Offline**   | Infrastructure Failure | gRPC returns `UNAVAILABLE` or connection refused                   | Throw error, DO NOT acknowledge   | Consumer retries delivery. No failure event is emitted prematurely. Order remains `PENDING`.                                         |
| **gRPC Call Timeout**       | Infrastructure Failure | gRPC returns `DEADLINE_EXCEEDED`                                   | Throw error, DO NOT acknowledge   | Consumer retries delivery. System recovers automatically when Stock Service recovers.                                                |
| **Database Pool Exhausted** | Infrastructure Failure | PostgreSQL query timeout / pool exhaustion                         | Throw error, DO NOT acknowledge   | Consumer retries delivery without losing messages.                                                                                   |
| **Poison-Pill Message**     | Schema/Payload Failure | Malformed JSON or invalid Zod schema                               | Acknowledge & Skip                | Error logged with details; consumer does not stall or deadlock.                                                                      |

---

## 4. Consumer Idempotency & Duplicate Delivery

In distributed message brokers adhering to at-least-once delivery, duplicate messages are inevitable (e.g. broker failover after publish ack loss, consumer rebalance before offset commit).

### A. Idempotency in Inventory Service

- **Mechanism**: `inventory_reservations` enforces a `UNIQUE (order_id)` database constraint.
- **Behavior**:
  - When an `OrderCreated` event is consumed for the first time, a reservation row with `order_id` is created.
  - If a duplicate `OrderCreated` event arrives, the database insert throws a unique constraint violation (`23505`).
  - The repository catches this violation and returns `{ alreadyProcessed: true }`.
  - The consumer recognizes the duplicate, logs an informational notice, and safely acknowledges the message without calling Stock Service again.

### B. Idempotency in Order Service

- **Mechanism**: State machine validation in `OrderRepository.updateOrderStatus`.
- **Behavior**:
  - Valid transitions: `PENDING` / `INVENTORY_PROCESSING` → `CONFIRMED` or `INVENTORY_FAILED`.
  - If a duplicate `InventoryReserved` or `InventoryReservationFailed` event arrives and the order is already in a terminal state (`CONFIRMED` or `INVENTORY_FAILED`), the update is skipped.
  - The method returns `{ alreadyUpdated: true }`, allowing the consumer to acknowledge the Kafka offset without throwing errors or causing inconsistent mutations.

### C. Orphaned Messages

- If an `inventory.events` message arrives for an order ID that does not exist in `order_db`:
  - `OrderRepository.updateOrderStatus` throws `OrderNotFoundError`.
  - The `InventoryEventsConsumer` catches this error, logs a warning with the missing `orderId`, and acknowledges the message.
  - This prevents an orphaned or corrupt event from perpetually blocking the Kafka partition.

---

## 5. Concurrency & Deadlock Prevention in Stock Service

When multiple orders attempt to reserve stock for the same products concurrently:

1. **Row-Level Locking**: Stock Service executes `SELECT ... FOR UPDATE` on `stock` rows.
2. **Lexicographical Sorting**: Product IDs are sorted lexicographically before acquiring row locks (`items.sort((a, b) => a.productId.localeCompare(b.productId))`).
   - If Order A requests Products [P1, P2] and Order B requests Products [P2, P1], both transactions acquire locks in the identical order [P1, then P2].
   - This mathematically eliminates circular wait conditions, guaranteeing total deadlock prevention.
3. **All-or-Nothing Stock Allocation**:
   - Stock Service verifies that every requested product has `available_quantity >= requested_quantity`.
   - If even one item has insufficient stock, no rows are decremented, and the transaction exits without partial reservations.
4. **Kafka Partition Affinity**:
   - Both `order.events` and `inventory.events` use `orderId` as the Kafka message key.
   - Kafka guarantees that all events for a given order are routed to the same partition and consumed sequentially by a single thread, preventing race conditions within any single order aggregate.
