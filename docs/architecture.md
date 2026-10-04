# System Architecture

## 1. Overview

The **Event-Driven Order Fulfillment Platform** is a distributed, event-driven microservices system built with **TypeScript**, **Apache Kafka**, **PostgreSQL**, **gRPC**, **GraphQL**, and **Docker**.

The platform processes e-commerce orders through an asynchronous, event-driven fulfillment workflow while ensuring data consistency across isolated domain databases through the **Transactional Outbox Pattern**, **at-least-once event delivery**, **idempotent message consumption**, and **synchronous gRPC stock reservations**.

---

## 2. System Topology & Architecture Diagram

```text
                               +-----------------------------+
                               |     Client Application      |
                               +-----------------------------+
                                              |
                                              | GraphQL POST
                                              v
                              +-------------------------------+
                              |      GraphQL API Gateway      |
                              |         (:4000/graphql)       |
                              +-------------------------------+
                                              |
                                              | HTTP POST /orders
                                              v
+------------------+          +-------------------------------+          +------------------+
|                  |  ACID Tx |         Order Service         | Consume  |                  |
|     order_db     |<-------->|            (:4001)            |<---------|  Kafka Broker    |
|                  |          +-------------------------------+          |   (PLAINTEXT)    |
+------------------+                          |                          |                  |
                                              | Poll Outbox & Produce    |  Topics:         |
                                              +------------------------->|  - order.events  |
                                                                         |  - inventory.    |
                                                                         |    events        |
                                                                         |                  |
+------------------+          +-------------------------------+          |                  |
|                  |  ACID Tx |       Inventory Service       | Consume  |                  |
|   inventory_db   |<-------->|       (Kafka & Outbox)        |<---------|                  |
|                  |          +-------------------------------+          +------------------+
+------------------+                          |                                    ^
                                              | Synchronous gRPC                   |
                                              | ReserveStock()                     | Poll Outbox
                                              v                                    | & Produce
+------------------+          +-------------------------------+                    |
|                  |  ACID Tx |         Stock Service         |                    |
|     stock_db     |<-------->|           (:50051)            |--------------------+
|                  |          +-------------------------------+
+------------------+
```

---

## 3. Microservice Roles & Responsibilities

### 3.1 GraphQL API Gateway (`apps/api-gateway`)

- **Role**: Client ingress and API facade.
- **Protocol**: Exposes GraphQL queries (`order`, `orders`) and mutations (`createOrder`) on port 4000.
- **Data Boundary**: Zero direct database access. Translates GraphQL operations into internal HTTP requests to the Order Service (`http://order-service:4001`).
- **Resilience**: Enforces request timeouts (`AbortSignal.timeout(5000)`) and formats user-friendly GraphQL error responses.

### 3.2 Order Service (`apps/order-service`)

- **Role**: Order aggregate management and fulfillment state machine.
- **Protocol**: Internal Fastify HTTP server on port 4001; Kafka producer and consumer.
- **Database Ownership**: Exclusive ownership of `order_db` (`orders`, `order_items`, `outbox_events`).
- **Patterns**:
  - Atomically writes order data and `OrderCreated` outbox event in a single PostgreSQL ACID transaction.
  - Background `OutboxPublisher` polls unpublished outbox events and publishes to `order.events` partitioned by `orderId`.
  - Background `InventoryEventsConsumer` subscribes to `inventory.events` and transitions order status to `CONFIRMED` or `INVENTORY_FAILED`.

### 3.3 Inventory Service (`apps/inventory-service`)

- **Role**: Event-driven orchestration of inventory reservations.
- **Protocol**: Kafka consumer (`order.events`), gRPC client to Stock Service, and Kafka outbox publisher.
- **Database Ownership**: Exclusive ownership of `inventory_db` (`inventory_reservations`, `inventory_reservation_items`, `outbox_events`).
- **Patterns**:
  - Consumes `OrderCreated` from `order.events` idempotently (enforced via `UNIQUE (order_id)` constraint).
  - Invokes `StockService.ReserveStock` synchronously over gRPC with strict timeouts and error mapping.
  - Atomically records reservation outcome and corresponding outbox event (`InventoryReserved` or `InventoryReservationFailed`) in `inventory_db`.
  - Background `InventoryOutboxPublisher` dispatches events to `inventory.events`.

### 3.4 Stock Service (`apps/stock-service`)

- **Role**: Atomic stock level tracking and reservation management.
- **Protocol**: gRPC server on port 50051 implementing `stock.proto`.
- **Database Ownership**: Exclusive ownership of `stock_db` (`products`, `stock`, `stock_reservations`, `stock_reservation_items`).
- **Patterns**:
  - Implements `ReserveStock` RPC with row-level locks (`SELECT ... FOR UPDATE`) ordered lexicographically by product ID to prevent deadlocks.
  - Validates stock availability across all requested items and applies all-or-nothing reservations.
  - Guarantees idempotency via `UNIQUE (order_id)` constraint on `stock_reservations`.
  - `ReleaseStock` RPC is intentionally declared in the protobuf contract but remains explicitly `UNIMPLEMENTED`.

---

## 4. Inter-Service Communication Patterns

| Boundary                           | Pattern                   | Protocol                   | Key Benefit                                                              |
| ---------------------------------- | ------------------------- | -------------------------- | ------------------------------------------------------------------------ |
| Client -> API Gateway              | Request/Response          | GraphQL over HTTP          | Strongly-typed client contract; field selection; scalar validations.     |
| Gateway -> Order Service           | Request/Response          | REST/JSON over HTTP        | Microservice encapsulation; gateway isolated from storage layer.         |
| Order Service -> Inventory Service | Event-Driven Asynchronous | Kafka (`order.events`)     | Temporal decoupling; publisher unaffected by downstream latency/outages. |
| Inventory Service -> Stock Service | Synchronous RPC           | gRPC (HTTP/2 + Protobuf)   | Low latency, compact binary payload, strong schema contract.             |
| Inventory Service -> Order Service | Event-Driven Asynchronous | Kafka (`inventory.events`) | Asynchronous completion of distributed fulfillment workflow.             |

---

## 5. Storage Architecture & Database-per-Service Isolation

Each service connects to its own dedicated PostgreSQL logical database on the central database server:

- `order_db`: Managed by Order Service migrations. Contains `orders`, `order_items`, `outbox_events`.
- `inventory_db`: Managed by Inventory Service migrations. Contains `inventory_reservations`, `inventory_reservation_items`, `outbox_events`.
- `stock_db`: Managed by Stock Service migrations. Contains `products`, `stock`, `stock_reservations`, `stock_reservation_items`.

**Zero Cross-Service Access**: Services never share database credentials or issue cross-database queries. All coordination occurs via Kafka events or gRPC calls.

---

## 6. End-to-End Fulfillment Lifecycle

### 6.1 Success Path

1. **Client** issues `createOrder` mutation to GraphQL Gateway.
2. **Gateway** forwards JSON payload to Order Service `POST /orders`.
3. **Order Service** opens ACID transaction in `order_db`:
   - Inserts order row with status `PENDING`.
   - Inserts line items into `order_items`.
   - Inserts `OrderCreated` event into `outbox_events`.
   - Commits transaction and returns HTTP 201 with `PENDING` order.
4. **Gateway** returns initial `PENDING` order state immediately to client.
5. **OutboxPublisher** polls `outbox_events`, publishes to `order.events` (keyed by `orderId`), and marks `published_at = NOW()`.
6. **Inventory Service** consumes `OrderCreated` from `order.events`:
   - Creates `PENDING` reservation in `inventory_db`.
   - Calls `StockService.ReserveStock()` via gRPC.
7. **Stock Service** in `stock_db`:
   - Locks stock rows via `FOR UPDATE` sorted by `product_id`.
   - Verifies sufficient quantity for all items.
   - Deducts stock quantity and inserts `stock_reservations` + items.
   - Returns gRPC `ReserveStockResponse { success: true }`.
8. **Inventory Service** in `inventory_db`:
   - Updates reservation status to `RESERVED`.
   - Inserts `InventoryReserved` event into `outbox_events`.
   - Commits transaction.
9. **InventoryOutboxPublisher** polls outbox, publishes `InventoryReserved` to `inventory.events` (keyed by `orderId`), and marks event published.
10. **Order Service** `InventoryEventsConsumer` consumes `InventoryReserved`:
    - Locks order row in `order_db`.
    - Updates order status from `PENDING` to `CONFIRMED`.
    - Commits transaction.
11. Subsequent client query `order(id)` returns `CONFIRMED` status (eventual consistency achieved).

### 6.2 Insufficient Stock Path

1. Steps 1-6 execute as above.
2. In Step 7, **Stock Service** detects insufficient available stock for requested items:
   - Does not mutate stock levels.
   - Returns gRPC `ReserveStockResponse { success: false, failure_reason: "INSUFFICIENT_STOCK" }`.
3. In Step 8, **Inventory Service**:
   - Updates reservation status to `FAILED`.
   - Inserts `InventoryReservationFailed` into `inventory_db.outbox_events`.
4. In Step 9, `InventoryOutboxPublisher` dispatches `InventoryReservationFailed` to `inventory.events`.
5. In Step 10, **Order Service** `InventoryEventsConsumer`:
   - Updates order status from `PENDING` to `INVENTORY_FAILED`.
6. Subsequent client query `order(id)` returns `INVENTORY_FAILED`.
