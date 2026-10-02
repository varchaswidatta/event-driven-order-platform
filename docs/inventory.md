# Inventory Service Architecture & Kafka Consumer

## 1. Inventory Service Responsibility

The **Inventory Service** is an autonomous microservice responsible for orchestrating inventory reservations for orders placed across the platform.

In **Phase 5**, the Inventory Service:

- Acts as a dedicated event consumer on the `order.events` Kafka topic.
- Consumes `OrderCreated` events published by the Order Service's Transactional Outbox Publisher.
- Atomically persists inventory reservations in its private database (`inventory_db`) with status `PENDING`.
- Provides strict idempotency guarantees to tolerate Kafka at-least-once message delivery.

```
+---------------+        Kafka Topic         +-------------------+        Private DB
| Order Service | ------------------------>  | Inventory Service | ------> [inventory_db]
|  (Publisher)  |       order.events         |    (Consumer)     |         - inventory_reservations
+---------------+    (key = orderId UUID)    +-------------------+         - inventory_reservation_items
```

---

## 2. Database Ownership & Microservice Isolation

The platform enforces a strict **Database-per-Service** pattern:

- **Inventory Service owns `inventory_db`**: Exclusive read and write ownership of all reservation data belongs to the Inventory Service.
- **Strict Data Isolation**: The Inventory Service **never** queries or touches `order_db` tables (`orders`, `order_items`, `outbox_events`). All order details are derived solely from the immutable `OrderCreated` domain event payload delivered over Kafka.
- **No Reverse Access**: The Order Service never queries or joins against `inventory_db`.
- **Infrastructure Colocation**: For local development efficiency, `order_db` and `inventory_db` reside as separate logical databases within the same PostgreSQL instance, but maintain absolute logical and operational isolation.

---

## 3. Kafka Consumer Architecture

The Inventory Service consumes events using KafkaJS through a decoupled layered architecture:

```
                      +---------------------------------------+
                      |           Kafka Broker                |
                      |        Topic: order.events            |
                      +---------------------------------------+
                                          |
                                          | EachMessagePayload
                                          v
+---------------------------------------------------------------------------------+
| Inventory Service                                                               |
|                                                                                 |
|  +---------------------------------------------------------------------------+  |
|  | OrderEventsConsumer (Transport Layer)                                     |  |
|  | - Group ID: inventory-service                                             |  |
|  | - Subscribes to order.events                                              |  |
|  | - JSON deserialization                                                    |  |
|  | - Routes errors (skips poison pills; throws on db failures)               |  |
|  +---------------------------------------------------------------------------+  |
|                                          |                                      |
|                                          v                                      |
|  +---------------------------------------------------------------------------+  |
|  | InventoryService (Domain Service Layer)                                   |  |
|  | - Validates EventEnvelope schema via Zod                                  |  |
|  | - Validates OrderCreatedPayload schema via Zod                            |  |
|  | - Enforces eventVersion = 1 & aggregateType = 'Order'                     |  |
|  | - Maps event payload to repository input                                  |  |
|  +---------------------------------------------------------------------------+  |
|                                          |                                      |
|                                          v                                      |
|  +---------------------------------------------------------------------------+  |
|  | InventoryRepository (Persistence Layer)                                   |  |
|  | - BEGIN PostgreSQL Transaction                                           |  |
|  | - Check existing order_id (FOR UPDATE)                                    |  |
|  | - Insert reservation + reservation items atomically                       |  |
|  | - COMMIT / ROLLBACK                                                       |  |
|  +---------------------------------------------------------------------------+  |
|                                          |                                      |
+------------------------------------------|--------------------------------------+
                                           v
                                   [ inventory_db ]
```

---

## 4. `order.events` Consumption

- **Topic Name**: `order.events`
- **Partitioning Key**: `orderId` (UUID). The Order Service producer partitions events by `orderId`, ensuring all events relating to the same order arrive in strict chronological order on the same partition.
- **Payload Format**: Standard `EventEnvelope<OrderCreatedPayload>` serialized as JSON.

---

## 5. Consumer Group

- **Consumer Group ID**: `inventory-service`
- **Purpose**: Decouples the Inventory Service's consumer offsets from any other service or diagnostic tool.
- **Offset Management**: Offsets are committed automatically upon successful completion of `eachMessage`. If an infrastructure failure occurs (such as a database disconnection), an error is thrown, preventing offset commit and prompting KafkaJS to retry message delivery.

---

## 6. `OrderCreated` Processing Flow

When an `OrderCreated` event is received:

1. **JSON Deserialization**: Deserializes the Kafka message value into a JavaScript object.
2. **Envelope & Payload Validation**: Validates the envelope structure, event type, aggregate type, version, and line item quantities using Zod schemas (`eventEnvelopeSchema` and `orderCreatedPayloadSchema`).
3. **Atomic Persistence**: Calls `InventoryRepository.createReservation()` within a single database transaction.
4. **Idempotency Guard**: Checks if a reservation already exists for `order_id`. If found, existing data is returned without writing duplicates.
5. **Item Insertion**: Inserts rows into `inventory_reservation_items` for every product in the order.
6. **Transaction Commit**: Commits the transaction, persisting the reservation in `PENDING` status.

---

## 7. Database Schema (`inventory_db`)

The schema is defined in `apps/inventory-service/src/db/migrations/001_create_inventory_reservations.sql`:

### `inventory_reservations` Table

| Column       | Type        | Constraints                                                    | Description                                                       |
| ------------ | ----------- | -------------------------------------------------------------- | ----------------------------------------------------------------- |
| `id`         | UUID        | PRIMARY KEY, DEFAULT `gen_random_uuid()`                       | Unique reservation identifier                                     |
| `order_id`   | UUID        | NOT NULL, **UNIQUE**                                           | Order ID from OrderCreated event; prevents duplicate reservations |
| `status`     | VARCHAR(50) | NOT NULL, DEFAULT `'PENDING'`, CHECK (`status IN ('PENDING')`) | Lifecycle status of reservation                                   |
| `created_at` | TIMESTAMPTZ | NOT NULL, DEFAULT `NOW()`                                      | Timestamp when reservation was created                            |
| `updated_at` | TIMESTAMPTZ | NOT NULL, DEFAULT `NOW()`                                      | Timestamp when reservation was last modified                      |

### `inventory_reservation_items` Table

| Column           | Type        | Constraints                                                         | Description                          |
| ---------------- | ----------- | ------------------------------------------------------------------- | ------------------------------------ |
| `id`             | UUID        | PRIMARY KEY, DEFAULT `gen_random_uuid()`                            | Unique line item identifier          |
| `reservation_id` | UUID        | NOT NULL, REFERENCES `inventory_reservations(id)` ON DELETE CASCADE | Parent reservation reference         |
| `product_id`     | UUID        | NOT NULL                                                            | Identifier of product to be reserved |
| `quantity`       | INTEGER     | NOT NULL, CHECK (`quantity > 0`)                                    | Number of units requested            |
| `created_at`     | TIMESTAMPTZ | NOT NULL, DEFAULT `NOW()`                                           | Timestamp when item was recorded     |

**Indexes**:

- `idx_reservation_items_reservation_id`: Speeds up retrieval of line items for a reservation.
- `inventory_reservations_order_id_key`: Unique B-tree index enforcing order uniqueness and accelerating idempotency lookups.

---

## 8. Idempotency Strategy

Kafka guarantees **at-least-once** delivery. Under normal distributed conditions (network blips, consumer rebalances, producer retries), the same `OrderCreated` event may be delivered more than once.

The Inventory Service guarantees idempotency through a two-layered defense:

1. **Database Constraint (Ground Truth)**:
   - `order_id` has a `UNIQUE` constraint in `inventory_reservations`.
   - The database will physically reject duplicate reservations for the same order.

2. **Application Transaction Logic**:
   ```sql
   BEGIN;
   SELECT id, order_id, status FROM inventory_reservations WHERE order_id = $1 FOR UPDATE;
   ```
   - If an existing reservation row is returned, the repository skips inserting reservation and item rows, commits the read transaction, and returns the existing reservation with `alreadyExisted = true`.
   - If no reservation exists, the transaction proceeds with the insertion of the reservation and item rows.
   - This ensures safe concurrent handling without throwing unhandled database violation errors.

---

## 9. Transaction Behavior

Creating a reservation and its items is strictly atomic:

- **Single PostgreSQL Transaction**: Enclosed within `BEGIN` and `COMMIT`.
- **Atomicity**: If any item insert fails (e.g. invalid quantity constraint violation), the entire transaction rolls back via `ROLLBACK`.
- **Consistency Guarantee**: There are never orphaned reservations without items, nor partially inserted items.

---

## 10. Failure & Error Handling

| Scenario                                  | Service Behavior                                         | Kafka Offset Impact                                        |
| ----------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------------- |
| **Malformed JSON**                        | Logged as error; message skipped.                        | Offset committed (prevents poison pill blocking).          |
| **Invalid Envelope / Schema**             | `InvalidEventError` logged with issues; message skipped. | Offset committed (poison pill prevention).                 |
| **Unsupported Event Type**                | `UnsupportedEventTypeError` logged; message skipped.     | Offset committed (avoids stalling queue on future events). |
| **Unsupported Event Version**             | `UnsupportedEventVersionError` logged; message skipped.  | Offset committed.                                          |
| **PostgreSQL Connection / Query Failure** | Error thrown out of `eachMessage`.                       | **Offset NOT committed**. Kafka redelivers message.        |
| **Duplicate `OrderCreated` Delivery**     | Handled idempotently via `order_id` check.               | Offset committed successfully.                             |

---

## 11. Current Phase 5 Boundaries & Limitations

In Phase 5:

- Reservations are created with `status = PENDING`.
- The Inventory Service does **not** communicate with physical stock or warehouses yet.
- The Inventory Service does **not** publish confirmation or failure events back to Kafka yet.
- The GraphQL API still returns `PENDING` immediately.

---

## 12. What Belongs to Phase 6

Phase 6 will introduce:

1. **gRPC Client in Inventory Service**: Synchronously communicating with the upcoming Stock Service.
2. **Stock Service (`stock_db`)**: Managing actual physical product stock balances and reservations.
3. **Reservation Status Progression**: Transitioning reservations from `PENDING` to `CONFIRMED` or `FAILED`.
4. **Outbox Events in Inventory Service**: Emitting `InventoryReserved` or `InventoryFailed` events back to Kafka for the Order Service to consume.
