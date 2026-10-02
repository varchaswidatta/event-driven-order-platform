# Event-Driven Order Fulfillment Platform

A robust, enterprise-grade event-driven microservice platform built with **TypeScript**, demonstrating production patterns with **Apache Kafka**, **PostgreSQL**, **gRPC**, **GraphQL**, **Docker**, and **CI/CD**.

This project serves as an end-to-end reference implementation for resilient, distributed order processing and inventory management under high concurrency and failure-prone distributed environments.

---

## Planned Architecture

> [!NOTE]
> The architecture diagram and flow below represent the **planned target architecture** for the platform. In this initial setup (Phase 0), the monorepo foundation, workspace configurations, and structural boundaries are established. Implementation will be added incrementally in subsequent phases.

```text
Client
  ↓
GraphQL API Gateway
  ↓
Order Service
  ↓
PostgreSQL + Transactional Outbox
  ↓
Kafka
  ↓
Inventory Service
  ↓
gRPC
  ↓
Stock Service
  ↓
PostgreSQL
```

### Architectural Highlights

- **GraphQL API Gateway**: Single point of ingress for clients, providing typed queries and mutations.
- **Order Service**: Manages order creation, lifecycle state transitions, and guarantees atomic event publishing via the **Transactional Outbox Pattern**.
- **Apache Kafka**: High-throughput distributed event log serving as the asynchronous event backbone.
- **Inventory Service**: Event-driven consumer processing order events and orchestrating stock availability checks.
- **Stock Service**: High-performance internal service handling stock reservations and allocations via **gRPC**.
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

| Command                  | Description                                          |
| :----------------------- | :--------------------------------------------------- |
| `pnpm run build`         | Builds TypeScript across all workspace packages      |
| `pnpm run typecheck`     | Runs TypeScript type checking without emitting files |
| `pnpm run test`          | Runs tests across workspace packages                 |
| `pnpm run lint`          | Runs ESLint across the codebase                      |
| `pnpm run lint:fix`      | Runs ESLint and automatically applies fixes          |
| `pnpm run format`        | Formats all files using Prettier                     |
| `pnpm run format:check`  | Verifies code formatting compliance                  |
| `pnpm run migrate:order` | Runs Order Service database migrations               |

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
- [ ] **Phase 4: Kafka Event Publishing & Inventory Consumer**
  - Background outbox publisher streaming unpublished events to Apache Kafka
  - Kafka topics and partition-key ordering
  - Downstream Inventory Service event consumption and inventory workflow
- [ ] **Phase 5: Stock Service & gRPC Integration**
  - Protocol Buffers definition in `proto/`
  - Stock Service gRPC implementation and stock reservation
- [ ] **Phase 6: Resilience, Testing, Docker Compose & CI/CD**
  - Full docker-compose environment
  - GitHub Actions CI pipeline
