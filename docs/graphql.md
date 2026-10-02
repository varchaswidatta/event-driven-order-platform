# GraphQL API Gateway Architecture & Specification

## 1. Overview & Architecture

The **GraphQL API Gateway** serves as the public, client-facing entry point for the Event-Driven Order Fulfillment Platform. It exposes typed GraphQL queries and mutations to external clients (web, mobile, third-party integrations) while abstracting internal microservice boundaries.

```text
  Client (Web / Mobile / CLI)
               │
               ▼ GraphQL (HTTP POST /graphql)
┌──────────────────────────────────────────────┐
│           GraphQL API Gateway                │
│             (Apollo Server)                  │
└──────────────────────┬───────────────────────┘
                       │
                       │ Internal HTTP/JSON
                       ▼
┌──────────────────────────────────────────────┐
│                Order Service                 │
│              (Fastify Server)                │
└──────────────────────┬───────────────────────┘
                       │
                       │ node-postgres (pg)
                       ▼
┌──────────────────────────────────────────────┐
│             PostgreSQL Database              │
│                 (order_db)                   │
└──────────────────────────────────────────────┘
```

---

## 2. Why GraphQL Does Not Access PostgreSQL Directly

In accordance with strict microservice design principles and domain boundaries:

- **Database-per-Service Isolation:** Each microservice strictly encapsulates and owns its data store. `order_db` belongs exclusively to the Order Service.
- **Encapsulation of Business Invariants:** The Order Service is responsible for business logic, status transitions, schema validation, and transactional integrity (such as atomic insertion of orders and order items in PostgreSQL transactions).
- **Decoupled Evolution:** Direct database access from the API Gateway would couple the edge layer to internal relational schemas, leading to tight coupling, leaky abstractions, and brittle deployment lifecycles.
- **Security & Blast Radius:** The API Gateway has zero database credentials and no direct network dependency on the database, eliminating the risk of accidental direct query vulnerabilities at the edge.

---

## 3. Gateway to Order Service Communication

- **Protocol:** Synchronous JSON over HTTP (`fetch`).
- **Transport Security & Reliability:**
  - Content negotiation using standard `application/json` headers.
  - Fail-fast request timeouts via `AbortSignal.timeout(5000)` to prevent gateway thread exhaustion.
  - Dedicated `OrderServiceClient` abstraction isolating HTTP calls and error translation from GraphQL resolvers.
- **Service Ports:**
  - **API Gateway:** `http://localhost:4000/graphql` (`API_GATEWAY_PORT`)
  - **Order Service (Internal):** `http://localhost:4001` (`ORDER_SERVICE_PORT`)

---

## 4. GraphQL Endpoint

- **URL:** `http://localhost:4000/graphql`
- **Method:** `POST`
- **Headers:** `Content-Type: application/json`

---

## 5. GraphQL Schema

```graphql
scalar DateTime
scalar Decimal

enum OrderStatus {
  PENDING
  INVENTORY_PROCESSING
  CONFIRMED
  INVENTORY_FAILED
}

type OrderItem {
  productId: ID!
  quantity: Int!
  unitPrice: Decimal!
}

type Order {
  id: ID!
  customerId: ID!
  status: OrderStatus!
  totalAmount: Decimal!
  currency: String!
  items: [OrderItem!]!
  createdAt: DateTime!
  updatedAt: DateTime!
}

input OrderItemInput {
  productId: ID!
  quantity: Int!
  unitPrice: Decimal!
}

input CreateOrderInput {
  customerId: ID!
  items: [OrderItemInput!]!
}

type Query {
  order(id: ID!): Order
  orders(customerId: ID): [Order!]!
}

type Mutation {
  createOrder(input: CreateOrderInput!): Order!
}
```

---

## 6. Temporary Pricing Decision for Phase 2

In Phase 1, the Order Service was built with strict validation for `unit_price`, storing it as a fixed-point `NUMERIC(12,2)` and computing exact totals with integer arithmetic.

To keep pricing deterministic **without inventing a fake product catalog or prematurely creating an ad-hoc stock database**, the GraphQL `OrderItemInput` explicitly accepts `unitPrice: Decimal!`:

```graphql
input OrderItemInput {
  productId: ID!
  quantity: Int!
  unitPrice: Decimal!
}
```

> [!NOTE]
> In later phases (Phase 3 and Phase 4), when the **Stock Service** and inventory catalog are introduced, pricing validation and stock reservation will occur dynamically. For Phase 2, this explicit contract keeps the order creation flow 100% deterministic and preserves Phase 1's decimal safety.

---

## 7. Custom Scalars

### Decimal Scalar

- **Purpose:** Prevents binary floating-point rounding errors (e.g. `0.1 + 0.2 !== 0.3`).
- **Transport Representation:** Serialized as exact string representations (e.g. `"100.00"`).
- **Validation:** Matches non-negative decimals with up to 2 decimal places (`/^\d+(\.\d{1,2})?$/`). Unformatted integers or single-decimal numbers are canonically normalized (e.g. `"100"` → `"100.00"`).
- **Forbidden:** Raw JavaScript `Number` types are never used for monetary math.

### DateTime Scalar

- **Purpose:** Strict ISO-8601 UTC timestamp serialization.
- **Format:** `YYYY-MM-DDTHH:mm:ss.sssZ` (e.g. `2026-10-02T10:00:00.000Z`).
- **Validation:** Parses and validates valid date literals and variables, rejecting invalid calendar dates.

---

## 8. Query and Mutation Examples

### 8.1 Mutation: `createOrder`

**Request:**

```graphql
mutation CreateOrder($input: CreateOrderInput!) {
  createOrder(input: $input) {
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
    createdAt
    updatedAt
  }
}
```

**Variables:**

```json
{
  "input": {
    "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "items": [
      {
        "productId": "c9a646d3-9c61-4cd7-bf17-0f829f04653a",
        "quantity": 2,
        "unitPrice": "100.00"
      },
      {
        "productId": "d9a646d3-9c61-4cd7-bf17-0f829f04653b",
        "quantity": 1,
        "unitPrice": "49.99"
      }
    ]
  }
}
```

**Response:**

```json
{
  "data": {
    "createOrder": {
      "id": "e82a32c2-849a-4c28-97fb-c5bb20d436a5",
      "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "status": "PENDING",
      "totalAmount": "249.99",
      "currency": "USD",
      "items": [
        {
          "productId": "c9a646d3-9c61-4cd7-bf17-0f829f04653a",
          "quantity": 2,
          "unitPrice": "100.00"
        },
        {
          "productId": "d9a646d3-9c61-4cd7-bf17-0f829f04653b",
          "quantity": 1,
          "unitPrice": "49.99"
        }
      ],
      "createdAt": "2026-10-02T00:57:46.000Z",
      "updatedAt": "2026-10-02T00:57:46.000Z"
    }
  }
}
```

---

### 8.2 Query: `order(id: ID!)`

**Request:**

```graphql
query GetOrder($id: ID!) {
  order(id: $id) {
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
    createdAt
  }
}
```

**Variables:**

```json
{
  "id": "e82a32c2-849a-4c28-97fb-c5bb20d436a5"
}
```

**Response (Found):**

```json
{
  "data": {
    "order": {
      "id": "e82a32c2-849a-4c28-97fb-c5bb20d436a5",
      "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "status": "PENDING",
      "totalAmount": "249.99",
      "currency": "USD",
      "items": [
        {
          "productId": "c9a646d3-9c61-4cd7-bf17-0f829f04653a",
          "quantity": 2,
          "unitPrice": "100.00"
        }
      ],
      "createdAt": "2026-10-02T00:57:46.000Z"
    }
  }
}
```

**Response (Not Found):**

```json
{
  "data": {
    "order": null
  }
}
```

---

### 8.3 Query: `orders(customerId: ID)`

**Request:**

```graphql
query GetCustomerOrders($customerId: ID) {
  orders(customerId: $customerId) {
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

**Variables:**

```json
{
  "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
}
```

**Response:**

```json
{
  "data": {
    "orders": [
      {
        "id": "e82a32c2-849a-4c28-97fb-c5bb20d436a5",
        "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "status": "PENDING",
        "totalAmount": "249.99",
        "items": [{ "productId": "c9a646d3-9c61-4cd7-bf17-0f829f04653a", "quantity": 2 }]
      }
    ]
  }
}
```

---

## 9. Error Handling & Translation

The GraphQL API Gateway standardizes error translation from downstream services:

| Downstream Event              | HTTP Status       | GraphQL Error Code          | GraphQL Behavior                                |
| :---------------------------- | :---------------- | :-------------------------- | :---------------------------------------------- |
| **Order not found**           | `404 Not Found`   | N/A                         | Query returns `null`                            |
| **Validation failure**        | `400 Bad Request` | `BAD_USER_INPUT`            | Returns descriptive issue array                 |
| **Invalid Decimal / Date**    | N/A               | `GRAPHQL_VALIDATION_FAILED` | Scalar parse rejection                          |
| **Service Down / Timeout**    | `503 / 504`       | `INTERNAL_SERVER_ERROR`     | Sanitized message without leaking traces        |
| **Unexpected database error** | `500`             | `INTERNAL_SERVER_ERROR`     | Sanitized message, zero SQL/credentials exposed |

---

## 10. Asynchronous Inventory Note

> [!IMPORTANT]
> In Phase 2, order status is initialized to `PENDING`. Asynchronous inventory validation, reservation, and Kafka outbox event streaming will be implemented in subsequent phases. No Kafka broker or event consumer is active during Phase 2.

---

## 11. Local Startup Instructions

### 1. Ensure PostgreSQL is Running

```bash
docker ps
# Ensure event-platform-postgres is listening on localhost:5432
```

### 2. Run Database Migrations

```bash
pnpm run migrate:order
```

### 3. Start Order Service

```bash
pnpm --filter order-service run start
# Listens on http://localhost:4001
```

### 4. Start GraphQL API Gateway

```bash
pnpm --filter api-gateway run start
# Listens on http://localhost:4000/graphql
```
