# Event-Driven Order Fulfillment Platform

A robust, enterprise-grade event-driven microservice platform built with **TypeScript**, demonstrating production patterns with **Apache Kafka**, **PostgreSQL**, **gRPC**, **GraphQL**, **Docker**, and **CI/CD**.

This project serves as an end-to-end reference implementation for resilient, distributed order processing and inventory management under high concurrency and failure-prone distributed environments.

---

## Architecture Flow

The complete end-to-end asynchronous order fulfillment workflow:

```text
Client
  ↓
GraphQL API Gateway
  ↓ HTTP
Order Service
  ↓ ACID
PostgreSQL (order_db) + Transactional Outbox
  ↓
Kafka (order.events)
  ↓
Inventory Service
  ↓ gRPC
Stock Service
  ↓ ACID
PostgreSQL (stock_db)
  ↓
Inventory Service (Outbox)
  ↓
Kafka (inventory.events)
  ↓
Order Service (Consumer)
  ↓
PostgreSQL (order_db: CONFIRMED / INVENTORY_FAILED)
```

### Architectural Highlights

- **GraphQL API Gateway**: Single point of ingress for clients, providing typed queries and mutations without coupling to backend message brokers or storage.
- **Order Service**: Manages order creation, lifecycle state transitions, and guarantees atomic event publishing via the **Transactional Outbox Pattern**.
- **Apache Kafka**: High-throughput distributed streaming backbone operating `order.events` and `inventory.events` topics with deterministic partition affinity on `orderId`.
- **Inventory Service**: Event-driven consumer processing `order.events`, orchestrating stock reservations via gRPC, and publishing results to `inventory.events` via its own transactional outbox.
- **Stock Service**: High-performance internal service handling atomic stock reservations with deadlock-free row-level locking via **gRPC**.
- **PostgreSQL**: Dedicated database-per-service isolation (`order_db`, `inventory_db`, `stock_db`).

---

## Monorepo Structure

```text
event-driven-order-platform/
├── apps/
│   ├── api-gateway/            # GraphQL API gateway (client ingress)
│   ├── order-service/          # Order management & outbox publisher
│   ├── inventory-service/      # Kafka consumer & inventory orchestration
│   └── stock-service/          # gRPC stock reservation service
├── proto/                      # Protocol Buffer definitions for gRPC contracts
├── infrastructure/             # Infrastructure definitions & configs
│   ├── postgres/               # PostgreSQL initialization and schema configs
│   └── kafka/                  # Kafka broker configs, topics, and scripts
├── docs/                       # Architectural & design documentation
│   ├── architecture.md         # System topology and domain boundaries
│   ├── database.md             # Database-per-service schemas & outbox
│   ├── kafka.md                # Topic topologies and event contracts
│   ├── grpc.md                 # gRPC service definitions & resilience
│   ├── graphql.md              # GraphQL schema and query/mutation contracts
│   └── failure-scenarios.md    # Resilience, DLQ, and fault-tolerance patterns
├── scripts/                    # Automation and utility scripts
├── .env.example                # Safe template for environment variables
├── .gitignore                  # Git ignore rules for node, dist, and secrets
├── .prettierrc                 # Code formatting configuration
├── .prettierignore             # Prettier ignore patterns
├── package.json                # Root monorepo configuration
├── pnpm-workspace.yaml         # pnpm workspace definition
├── tsconfig.json               # Shared strict TypeScript base configuration
└── README.md                   # Project overview and documentation
```

---

## Getting Started

### Prerequisites

- **Node.js**: `>= 20.0.0`
- **pnpm**: `>= 9.0.0`
- **Git**

### Installation

Clone the repository and install workspace dependencies:

```bash
# Clone the repository
git clone <repository-url>
cd event-driven-order-platform

# Install dependencies across all workspace packages
pnpm install
```

### Environment Configuration

Copy the example environment configuration:

```bash
cp .env.example .env
```

> [!WARNING]
> `.env` contains local development values and must never be committed to source control.

---

## Available Scripts

From the repository root, you can execute commands across all packages using pnpm workspaces:

| Command                      | Description                                          |
| :--------------------------- | :--------------------------------------------------- |
| `pnpm run build`             | Builds TypeScript across all workspace packages      |
| `pnpm run typecheck`         | Runs TypeScript type checking without emitting files |
| `pnpm run test`              | Runs tests across workspace packages                 |
| `pnpm run lint`              | Runs ESLint across the codebase                      |
| `pnpm run lint:fix`          | Runs ESLint and automatically applies fixes          |
| `pnpm run format`            | Formats all files using Prettier                     |
| `pnpm run format:check`      | Verifies code formatting compliance                  |
| `pnpm run migrate:order`     | Runs Order Service database migrations               |
| `pnpm run migrate:inventory` | Runs Inventory Service database migrations           |
| `pnpm run migrate:stock`     | Runs Stock Service database migrations               |
| `pnpm run seed:stock`        | Seeds deterministic sample products & stock          |

---

## Implementation Roadmap

- [x] **Phase 0: Foundation & Monorepo Setup** (Complete)
  - pnpm workspace initialization
  - Base TypeScript, ESLint, and Prettier configurations
  - Directory skeleton and placeholder services
  - Architecture documentation outlines
- [x] **Phase 1: Order Service & PostgreSQL Persistence** (Complete)
  - Raw PostgreSQL persistence via `pg` (no ORM)
  - SQL migration runner and `001_create_orders.sql`
  - Atomic transaction handling (`orders` + `order_items`)
  - Domain models and clean repository/service layering
  - Input validation via Zod
  - Exact decimal-safe monetary arithmetic via `BigInt`
  - Unit tests and real PostgreSQL integration tests
- [x] **Phase 2: GraphQL API Gateway & Order Service Integration** (Complete)
  - Apollo Server GraphQL API Gateway on `http://localhost:4000/graphql`
  - Internal Fastify HTTP server for Order Service on `http://localhost:4001`
  - Decoupled `OrderServiceClient` communicating via HTTP/JSON (zero direct database coupling)
  - Custom `DateTime` and `Decimal` GraphQL scalars
  - Typed `createOrder` mutation, `order(id)` and `orders(customerId)` queries
  - End-to-end integration tests (Client → GraphQL Gateway → Order Service HTTP → PostgreSQL)
- [x] **Phase 3: Transactional Outbox Pattern** (Complete)
  - `outbox_events` table and partial index `idx_outbox_events_unpublished` (`002_create_outbox_events.sql`)
  - Single ACID PostgreSQL transaction: `orders` + `order_items` + `outbox_events` (`OrderCreated`)
  - Atomic rollback guarantee (verified with real PostgreSQL integration tests)
  - Reusable application `EventEnvelope` and `OrderCreated` event payload
  - Safe monetary boundary (inventory event payload strictly omits price/amount)
  - UUID correlation ID generation at Order Service boundary
  - Dedicated `OutboxRepository` for atomic insert, FIFO retrieval, mark published, and retry count
  - Full documentation in [docs/outbox.md](docs/outbox.md)
- [x] **Phase 4: Kafka Integration & Outbox Publisher** (Complete)
  - Apache Kafka in KRaft mode via Docker Compose (`docker-compose.yml`)
  - Topic `order.events` with key-based partitioning on `orderId`
  - In-process `OutboxPublisher` in Order Service with at-least-once publishing semantics
  - Dedicated `KafkaOrderProducer` abstraction using `kafkajs`
  - Automated retry tracking (`retry_count` increment on broker failure)
  - Unit tests covering all publisher states and edge cases
  - Live PostgreSQL + Kafka integration tests and fault tolerance recovery tests
  - Complete End-to-End test (GraphQL API Gateway → Order Service HTTP → PostgreSQL ACID Transaction → Outbox Publisher → Kafka `order.events`)
  - Full architectural documentation in [docs/kafka.md](docs/kafka.md)
- [x] **Phase 5: Inventory Service Event Consumption & Reservation** (Complete)
  - Dedicated PostgreSQL database `inventory_db` with `inventory_reservations` and `inventory_reservation_items` tables (`001_create_inventory_reservations.sql`)
  - Kafka consumer group `inventory-service` consuming `order.events` with `orderId` key affinity
  - Strict idempotency via `order_id UNIQUE` constraint and atomic transaction handling
  - Full domain event validation via Zod (`EventEnvelope` & `OrderCreatedPayload`)
  - Poison-pill resilience: invalid envelopes and unsupported event types/versions safely logged and skipped; db errors trigger retry
  - Real PostgreSQL and Kafka consumer integration tests
  - End-to-end flow test (GraphQL Gateway → Order Service → PostgreSQL → Outbox Publisher → Kafka → Inventory Service → `inventory_db`)
  - Full architectural documentation in [docs/inventory.md](docs/inventory.md)
- [x] **Phase 6: Stock Service & gRPC Reservation** (Complete)
  - Dedicated PostgreSQL database `stock_db` with `products`, `stock`, `stock_reservations`, and `stock_reservation_items` tables
  - gRPC server implementing `StockService.ReserveStock` and contract placeholder `ReleaseStock` using Protocol Buffers (`proto/stock.proto`)
  - Concurrency-safe atomic reservation transactions via PostgreSQL row-level locks (`SELECT ... FOR UPDATE`) with lexicographical product ID sorting to prevent deadlocks
  - Strict all-or-nothing stock reservation semantics (no partial allocations)
  - Idempotent gRPC reservation handling via `order_id UNIQUE` constraint on `stock_reservations`
  - Decoupled `StockServiceClient` in Inventory Service with configurable request deadlines, timeouts, and error sanitization
  - Clean separation of business failures (`INSUFFICIENT_STOCK`) from infrastructure failures (`UNAVAILABLE`, `DEADLINE_EXCEEDED`) ensuring reliable Kafka consumer retry semantics
  - Unit tests, concurrency tests, gRPC integration tests, and full Phase 6 End-to-End flow tests (GraphQL → Order Service → Outbox → Kafka → Inventory Service → gRPC → Stock Service → `stock_db`)
  - Full architectural documentation in [docs/stock.md](docs/stock.md) and [docs/grpc.md](docs/grpc.md)
- [x] **Phase 7: Complete Asynchronous Order Fulfillment Workflow** (Complete)
  - Dedicated outbox table `outbox_events` and schema migration in `inventory_db` (`002_create_outbox_events.sql`)
  - Extended reservation status constraint (`PENDING`, `RESERVED`, `FAILED`)
  - Inventory Service Transactional Outbox: atomic reservation status update and outbox event creation (`InventoryReserved` or `InventoryReservationFailed`)
  - Kafka topic `inventory.events` with 3 partitions and message key `orderId`
  - In-process `InventoryOutboxPublisher` dispatching reservation events with retry tracking and at-least-once delivery
  - Order Service `InventoryEventsConsumer` (consumer group `order-service`) listening to `inventory.events`
  - Deterministic state machine transitions in `OrderRepository.updateOrderStatus`:
    - `InventoryReserved` → `CONFIRMED`
    - `InventoryReservationFailed` → `INVENTORY_FAILED`
  - Consumer idempotency across both services: duplicate message replays safely acknowledged without side effects
  - Non-retryable orphaned event handling (`OrderNotFoundError`) preventing partition stalls
  - Comprehensive unit tests, integration tests, and full End-to-End flow tests covering all scenarios (success, insufficient stock, transient infrastructure failure, duplicate delivery)
  - Full architectural documentation in [docs/kafka.md](docs/kafka.md), [docs/database.md](docs/database.md), and [docs/failure-scenarios.md](docs/failure-scenarios.md)
- [ ] **Phase 8: Dockerization, Production Deployment & CI/CD**
  - Full multi-service docker-compose production environment
  - GitHub Actions CI/CD pipeline
  - Health checks, monitoring, and telemetry
