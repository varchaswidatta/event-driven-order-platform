# Event-Driven Order Fulfillment Platform

A production-grade, event-driven distributed microservices platform built with **TypeScript**, demonstrating core distributed systems patterns including **Apache Kafka**, **PostgreSQL**, **gRPC**, **GraphQL**, and container orchestration with **Docker Compose**.

This repository serves as a reference implementation for asynchronous order processing, resilient transactional outbox messaging, deadlock-free inventory reservation, and guaranteed eventual consistency.

---

## 1. Project Purpose

The platform implements an asynchronous, decoupled e-commerce fulfillment workflow. When a client places an order, the system accepts it immediately in a `PENDING` state and confirms or fails the order asynchronously as stock reservations are coordinated across microservice boundaries. The platform solves classic distributed systems challenges including dual-write consistency, network partition recovery, at-least-once message delivery, and concurrency deadlocks.

---

## 2. System Architecture

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

## 3. Services

The platform consists of four autonomous services:

1. **API Gateway (`apps/api-gateway`)**
   - Public client entry point hosting Apollo Server GraphQL on port `4000`.
   - Exposes strongly-typed GraphQL mutations (`createOrder`) and queries (`order`, `orders`).
   - Delegates commands to the Order Service via internal HTTP requests (`http://order-service:4001`).
   - Maintains zero direct database coupling.

2. **Order Service (`apps/order-service`)**
   - Manages order lifecycles and status transitions.
   - Internal Fastify HTTP server on port `4001`.
   - Atomically persists orders and outbox events in `order_db`.
   - Operates a background `OutboxPublisher` to produce `OrderCreated` events to Kafka.
   - Operates a background `InventoryEventsConsumer` to consume `inventory.events` and finalize order states.

3. **Inventory Service (`apps/inventory-service`)**
   - Event-driven orchestrator reacting to `order.events`.
   - Manages reservation records in `inventory_db`.
   - Calls the Stock Service over synchronous gRPC to verify and reserve stock.
   - Atomically records reservation state and outbox events (`InventoryReserved` or `InventoryReservationFailed`).
   - Operates a background `InventoryOutboxPublisher` to produce results to `inventory.events`.

4. **Stock Service (`apps/stock-service`)**
   - High-performance inventory tracking service hosting a gRPC server on port `50051`.
   - Implements `ReserveStock` RPC defined in `proto/stock.proto`.
   - Executes atomic, deadlock-free row-level locking (`SELECT ... FOR UPDATE`) in `stock_db`.
   - Enforces all-or-nothing stock reservation semantics.
   - Declares `ReleaseStock` in its protobuf definition (intentionally `UNIMPLEMENTED` by specification).

---

## 4. Technology Stack

- **Runtime & Language**: Node.js (>= 20.0.0 LTS), TypeScript 5.9 (Strict mode)
- **Monorepo & Package Management**: pnpm workspaces
- **Messaging & Event Streaming**: Apache Kafka 3.8 (KRaft mode, KafkaJS client)
- **Database**: PostgreSQL 17 (raw SQL with `pg` client driver, zero ORM abstraction)
- **Edge Layer**: Apollo Server 4 / GraphQL 16
- **Internal HTTP**: Fastify 5
- **RPC Framework**: gRPC (`@grpc/grpc-js`, `@grpc/proto-loader`, Protocol Buffers v3)
- **Validation**: Zod
- **Testing**: Vitest
- **Containerization**: Docker, Docker Compose (BuildKit multi-stage builds)
- **CI/CD**: GitLab CI (`.gitlab-ci.yml`)

---

## 5. Event Flow & Lifecycle

The distributed fulfillment lifecycle follows an asynchronous event-driven workflow:

```text
Customer
  ↓
GraphQL API Gateway
  ↓ HTTP
Order Service
  ↓ PostgreSQL transaction (orders + order_items + transactional outbox)
Outbox Publisher
  ↓
Kafka: order.events
  ↓
Inventory Service
  ↓ gRPC (synchronous ReserveStock)
Stock Service (stock_db row locks)
  ↓
Inventory result + transactional outbox
  ↓
Kafka: inventory.events
  ↓
Order Service
  ↓ PostgreSQL update
Final Order Status (CONFIRMED / INVENTORY_FAILED)
```

Core order lifecycle states:

- `PENDING`: Order recorded, awaiting inventory verification.
- `INVENTORY_PROCESSING`: Downstream inventory allocation in progress.
- `CONFIRMED`: Stock successfully reserved; order confirmed.
- `INVENTORY_FAILED`: Insufficient stock or invalid product; order failed.

---

## 6. Kafka Topics & Partitioning

The platform operates two primary Kafka topics:

1. **`order.events`**
   - **Events**: `OrderCreated`
   - **Partitions**: 3 partitions (enables parallel processing across consumer instances)
   - **Message Key**: `orderId` (UUID)
   - **Guarantee**: Total message ordering per order via hash-based partition routing.

2. **`inventory.events`**
   - **Events**: `InventoryReserved`, `InventoryReservationFailed`
   - **Partitions**: 3 partitions
   - **Message Key**: `orderId` (UUID)
   - **Guarantee**: Preserves correlation with original order stream.

---

## 7. gRPC Communication

Internal communication between **Inventory Service** and **Stock Service** uses synchronous gRPC:

- **Contract Definition**: `proto/stock.proto`
- **RPCs**:
  - `rpc ReserveStock(ReserveStockRequest) returns (ReserveStockResponse);`
  - `rpc ReleaseStock(ReleaseStockRequest) returns (ReleaseStockResponse);` _(explicitly UNIMPLEMENTED)_
- **Resilience**: Configurable deadline timeouts (default 5000ms), fail-fast error sanitization, and structured failure reasons (`INSUFFICIENT_STOCK`, `PRODUCT_NOT_FOUND`).

---

## 8. Transactional Outbox Pattern

To eliminate dual-write inconsistency between PostgreSQL and Kafka:

1. Business data (`orders` or `inventory_reservations`) and domain events (`outbox_events`) are inserted within the **same local ACID database transaction**.
2. If the transaction rolls back, no message is ever published.
3. If the transaction commits, the event is guaranteed to exist on durable disk.
4. An asynchronous polling worker (`OutboxPublisher`) periodically scans for unpublished events (`WHERE published_at IS NULL`), publishes them to Kafka with retry tracking, and marks `published_at = NOW()`.
5. This guarantees **at-least-once event delivery** to the Kafka cluster without distributed two-phase commit transactions.

---

## 9. Idempotency & Concurrency

Distributed networks inevitably deliver duplicate messages. The system implements end-to-end idempotency:

- **Inventory Service**: `inventory_reservations` enforces `UNIQUE (order_id)`. Re-delivered `OrderCreated` events are detected, skipped, and acknowledged without duplicate reservations.
- **Stock Service**: `stock_reservations` enforces `UNIQUE (order_id)`. Re-delivered reservation requests return the existing reservation ID without deducting stock again.
- **Order Service**: `OrderRepository.updateOrderStatus` checks current order status and ignores duplicate `InventoryReserved` or `InventoryReservationFailed` events.
- **Deadlock Avoidance**: Stock Service sorts product IDs lexicographically before issuing `SELECT ... FOR UPDATE` row locks, preventing circular wait conditions during concurrent multi-item reservations.

---

## 10. Database Ownership

The architecture enforces strict **Database-per-Service** isolation:

- `order_db`: Owned exclusively by Order Service.
- `inventory_db`: Owned exclusively by Inventory Service.
- `stock_db`: Owned exclusively by Stock Service.

No service connects to or queries another service's database. All cross-boundary communication occurs exclusively via Kafka events or gRPC.

---

## 11. Repository Structure

```text
event-driven-order-platform/
├── apps/
│   ├── api-gateway/            # GraphQL Apollo Server Gateway
│   ├── order-service/          # Order service, HTTP server & outbox publisher
│   ├── inventory-service/      # Inventory consumer, gRPC client & outbox publisher
│   └── stock-service/          # gRPC stock reservation service
├── proto/
│   └── stock.proto             # Protocol Buffers service definition
├── infrastructure/
│   ├── postgres/               # PostgreSQL multi-database init scripts
│   └── kafka/                  # Kafka cluster & topic initialization
├── docs/                       # Comprehensive architecture & design docs
│   ├── architecture.md         # System topology and domain boundaries
│   ├── database.md             # Database schemas, migrations & outbox
│   ├── kafka.md                # Topic topologies, event contracts & outbox
│   ├── grpc.md                 # gRPC service definition & resilience
│   ├── graphql.md              # GraphQL schema and operation contracts
│   └── failure-scenarios.md    # Fault tolerance and error classifications
├── docker-compose.yml          # Full platform Docker Compose orchestration
├── .gitlab-ci.yml              # CI/CD validation pipeline
├── .env.example                # Safe environment variable template
├── package.json                # Root monorepo configuration & package scripts
├── pnpm-workspace.yaml         # pnpm workspace configuration
└── README.md                   # Project documentation
```

---

## 12. Local Prerequisites

- **Docker Desktop** (or Docker Engine 24+ and Docker Compose v2)
- **Node.js** (>= 20.0.0 LTS) _(for running local tests or scripts outside Docker)_
- **pnpm** (>= 12.0.0) _(recommended for local development)_

---

## 13. Environment Configuration

Copy the example environment file:

```bash
cp .env.example .env
```

The system is pre-configured with sensible defaults:

- **Host execution**: connects to `localhost:5432`, `localhost:9092`.
- **Docker Compose execution**: services automatically use Compose network hostnames (`postgres`, `kafka`, `order-service`, `stock-service`).

---

## 14. How to Start the System (Docker Compose)

Launch the complete containerized stack with a single command:

```bash
docker compose up --build -d
```

Or using the pnpm convenience script:

```bash
pnpm run docker:up
```

### Startup Verification

Inspect running containers:

```bash
docker compose ps
```

All 6 services will be running and healthy:

- `event-platform-postgres` (PostgreSQL on `:5432` with `order_db`, `inventory_db`, `stock_db`)
- `event-platform-kafka` (Kafka broker on `:9092` internal, `:29092` external)
- `event-platform-kafka-init` (Auto-creates `order.events` and `inventory.events` topics)
- `event-platform-stock-service` (gRPC on `:50051`)
- `event-platform-inventory-service` (Kafka consumer & outbox worker)
- `event-platform-order-service` (HTTP on `:4001`)
- `event-platform-api-gateway` (GraphQL on `:4000`)

To view real-time logs across all services:

```bash
docker compose logs -f
```

To stop the system:

```bash
docker compose down
```

---

## 15. How to Run Tests

### Unit & Integration Tests (Local)

Ensure PostgreSQL and Kafka are running via Compose:

```bash
docker compose up -d postgres kafka
```

Execute the full automated test suite (134 tests):

```bash
pnpm test
```

Execute static quality checks:

```bash
pnpm format:check   # Prettier verification
pnpm lint           # ESLint verification
pnpm typecheck      # TypeScript compiler validation
pnpm build          # Workspace build verification
```

---

## 16. Accessing the GraphQL API

The GraphQL API Gateway is accessible at:

- **URL**: `http://localhost:4000/graphql`
- **Method**: `POST`
- **Content-Type**: `application/json`

---

## 17. Example: Creating an Order

Send a `POST` request to `http://localhost:4000/graphql`:

```graphql
mutation CreateOrder {
  createOrder(
    input: {
      customerId: "a1111111-1111-4111-8111-111111111111"
      items: [{ productId: "88888888-aaaa-4bbb-8ccc-000000000001", quantity: 2, unitPrice: 1500 }]
    }
  ) {
    id
    customerId
    status
    totalAmount
    currency
    items {
      productId
      quantity
      unitPrice
    }
  }
}
```

### Initial Response (Immediate)

The mutation returns immediately with status `PENDING`:

```json
{
  "data": {
    "createOrder": {
      "id": "e7a8a1cd-5ea5-4d0d-bb42-b319b9305e57",
      "customerId": "a1111111-1111-4111-8111-111111111111",
      "status": "PENDING",
      "totalAmount": "3000.00",
      "currency": "USD",
      "items": [
        {
          "productId": "88888888-aaaa-4bbb-8ccc-000000000001",
          "quantity": 2,
          "unitPrice": "1500.00"
        }
      ]
    }
  }
}
```

---

## 18. Observing Eventual Consistency

Within 1-2 seconds, the background outbox publishers and Kafka consumers coordinate stock reservation and update the order.

Query the order by ID:

```graphql
query GetOrder {
  order(id: "e7a8a1cd-5ea5-4d0d-bb42-b319b9305e57") {
    id
    customerId
    status
    totalAmount
    items {
      productId
      quantity
    }
  }
}
```

### Final Response (Eventual Consistency Achieved)

```json
{
  "data": {
    "order": {
      "id": "e7a8a1cd-5ea5-4d0d-bb42-b319b9305e57",
      "customerId": "a1111111-1111-4111-8111-111111111111",
      "status": "CONFIRMED",
      "totalAmount": "3000.00",
      "items": [
        {
          "productId": "88888888-aaaa-4bbb-8ccc-000000000001",
          "quantity": 2
        }
      ]
    }
  }
}
```

---

## 19. Business Failure Scenario (Insufficient Stock)

Request an order with a quantity exceeding available stock (e.g. quantity `999999`):

```graphql
mutation CreateOrderExcessStock {
  createOrder(
    input: {
      customerId: "b2222222-2222-4222-8222-222222222222"
      items: [
        { productId: "88888888-aaaa-4bbb-8ccc-000000000001", quantity: 999999, unitPrice: 1500 }
      ]
    }
  ) {
    id
    status
  }
}
```

### Resulting Workflow

1. Initial response returns `PENDING`.
2. Inventory Service attempts gRPC reservation with Stock Service.
3. Stock Service detects insufficient stock, leaves inventory unchanged, and returns `{ success: false, failure_reason: "INSUFFICIENT_STOCK" }`.
4. Inventory Service marks reservation `FAILED` and publishes `InventoryReservationFailed` to `inventory.events`.
5. Order Service consumes event and transitions order status to `INVENTORY_FAILED`.
6. Subsequent query `order(id)` returns:
   ```json
   {
     "data": {
       "order": {
         "id": "0a0e5f19-19c5-431f-abe5-71ebc35b7230",
         "status": "INVENTORY_FAILED"
       }
     }
   }
   ```

---

## 20. Seed Data for Testing

On startup, `stock_db` is automatically seeded with default products:

| Product ID                             | Product Name        | Initial Stock |
| -------------------------------------- | ------------------- | ------------- |
| `88888888-aaaa-4bbb-8ccc-000000000001` | Wireless Headphones | 50 units      |
| `88888888-aaaa-4bbb-8ccc-000000000002` | Mechanical Keyboard | 30 units      |
| `88888888-aaaa-4bbb-8ccc-000000000003` | USB-C Hub           | 100 units     |

---

## 21. CI/CD Pipeline

The project includes a GitLab CI pipeline defined in `.gitlab-ci.yml` structured across four validation stages:

1. **`quality`**: Enforces Prettier code formatting (`pnpm format:check`) and ESLint rules (`pnpm lint`).
2. **`test`**: Enforces TypeScript compilation (`pnpm typecheck`) and runs unit tests (`pnpm run test -- tests/unit`).
3. **`build`**: Compiles all TypeScript workspace packages (`pnpm build`).
4. **`docker`**: Validates Compose configuration (`docker compose config`) and verifies Docker image builds (`docker compose build`).

---

## 22. Architectural Invariants & Non-Goals

To maintain high architectural integrity and clear domain boundaries, the system adheres to strict design boundaries:

- **At-least-once delivery**: The platform embraces at-least-once messaging; all consumers are strictly idempotent.
- **Synchronous gRPC communication**: Used exclusively for internal, tight-coupling domain dependencies (Inventory ➔ Stock).
- **Transactional outbox**: Eliminates distributed transactions without requiring two-phase commit.
- **Eventual consistency**: External clients observe state progression through polling or subsequent queries.
- **Non-Goals**: Does not implement distributed Saga compensation, distributed caching, Elasticsearch, frontend applications, or Kubernetes.
