# Stock Service Architecture & Specification

## 1. Overview & Responsibility

The **Stock Service** is an autonomous microservice responsible for catalog product metadata, real-time inventory levels, and atomic stock reservation.

### Architecture Boundary

```
     Order Service
           │
           │ (asynchronous Kafka: order.events)
           ▼
    Inventory Service
           │
           │ (synchronous gRPC: ReserveStock)
           ▼
      Stock Service
           │
           ▼
       stock_db
```

### Strict Architectural Boundaries

- **Database Ownership**: Stock Service has exclusive read/write ownership of `stock_db`.
- **Isolation**: Inventory Service (or any other service) must **NEVER** connect directly to `stock_db`.
- **No Reverse Queries**: Stock Service does not query `order_db` or `inventory_db`.
- **gRPC Boundary**: All communication with Stock Service is strictly via gRPC Protocol Buffers over HTTP/2.

---

## 2. Stock Database (`stock_db`)

The database runs in PostgreSQL and is managed via idempotent SQL migrations in `apps/stock-service/src/db/migrations/`.

### Schema Details

#### `products` Table

- `id UUID PRIMARY KEY`: Unique product identifier.
- `sku VARCHAR(64) UNIQUE NOT NULL`: Stock keeping unit.
- `name VARCHAR(255) NOT NULL`: Human-readable product name.
- `price NUMERIC(12, 2) NOT NULL`: Decimal monetary price (`CHECK (price >= 0)`).
- `created_at` / `updated_at`: `TIMESTAMPTZ` audit timestamps.

#### `stock` Table

- `id UUID PRIMARY KEY`: Stock entry identifier.
- `product_id UUID UNIQUE NOT NULL REFERENCES products(id)`: Exactly one stock record per product.
- `available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0)`: Quantity available for reservation.
- `reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0)`: Quantity currently locked for pending orders.
- `updated_at`: `TIMESTAMPTZ`.

#### `stock_reservations` Table

- `id UUID PRIMARY KEY`: Reservation identifier.
- `order_id UUID UNIQUE NOT NULL`: Unique order reference ensuring idempotency.
- `status VARCHAR(50) NOT NULL`: Status (`RESERVED`, `RELEASED`).
- `created_at` / `updated_at`: `TIMESTAMPTZ`.

#### `stock_reservation_items` Table

- `id UUID PRIMARY KEY`: Reservation item identifier.
- `reservation_id UUID NOT NULL REFERENCES stock_reservations(id) ON DELETE CASCADE`.
- `product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT`.
- `quantity INTEGER NOT NULL CHECK (quantity > 0)`.
- `created_at`: `TIMESTAMPTZ`.

---

## 3. Stock Reservation Semantics

### All-or-Nothing Atomicity

When `ReserveStock` is called for an order requesting multiple items (e.g., Product A x 2, Product B x 3):

1. **Transaction Start**: Begin an ACID transaction (`BEGIN`).
2. **Idempotency Check**: Check if `stock_reservations` already contains a reservation for `order_id`. If so, commit and return existing `reservationId` without re-deducting.
3. **Pessimistic Row-Level Locking**: Sort product IDs lexicographically and execute `SELECT ... FROM stock WHERE product_id = ANY($1) FOR UPDATE`.
4. **Availability Verification**: Verify that every requested product exists and has `available_quantity >= requested_quantity`.
5. **Rollback on Any Insufficiency**: If _any_ product has insufficient stock or does not exist, rollback immediately (`ROLLBACK`) and return business failure reason `INSUFFICIENT_STOCK` or `PRODUCT_NOT_FOUND`. **No partial reservations are permitted.**
6. **Stock Mutation**:
   - `available_quantity = available_quantity - requested_quantity`
   - `reserved_quantity = reserved_quantity + requested_quantity`
7. **Record Reservation**: Insert row into `stock_reservations` and `stock_reservation_items`.
8. **Commit**: `COMMIT`.

---

## 4. Concurrency Protection & Deadlock Prevention

### Row-Level Locking

By utilizing PostgreSQL `SELECT ... FOR UPDATE`, any concurrent transaction attempting to reserve the same product is blocked until the active transaction commits or rolls back. The database engine guarantees serializability without relying on in-memory application locks.

### Deadlock Elimination

If Transaction 1 reserves `[Product A, Product B]` while Transaction 2 reserves `[Product B, Product A]`, circular waiting could cause a PostgreSQL deadlock error (`40P01`). To eliminate this risk, `StockRepository` sorts all product IDs lexicographically before acquiring locks:

```typescript
const sortedProductIds = [...productMap.keys()].sort();
```

Both transactions lock product rows in identical order, completely preventing deadlocks.

---

## 5. Seeding & Deterministic Test Data

To facilitate local development and reproducible integration testing, a deterministic seeder is provided:

```bash
# Run migration on stock_db
pnpm run migrate:stock

# Seed default catalog products
pnpm run seed:stock
```

### Seeded Catalog Items:

- **`PROD-A`** (`88888888-aaaa-4bbb-8ccc-000000000001`): Wireless Ergonomic Keyboard, $99.99, Available: 100, Reserved: 0.
- **`PROD-B`** (`88888888-aaaa-4bbb-8ccc-000000000002`): Precision Optical Mouse, $49.50, Available: 50, Reserved: 0.
- **`PROD-C`** (`88888888-aaaa-4bbb-8ccc-000000000003`): Noise-Cancelling Headphones, $199.00, Available: 20, Reserved: 0.

---

## 6. Scope & Phase 7 Deferred Functionality

### Phase 6 Included:

- Stock Service implementation and `stock_db` lifecycle.
- Idempotent migration runner and seeder.
- Concurrency-safe, atomic reservation transaction with row-level locks.
- gRPC server implementing `ReserveStock` and contract placeholder `ReleaseStock`.
- Inventory Service gRPC client with timeouts and error mapping.
- End-to-end integration and concurrency tests.

### Deferred to Phase 7:

- Publishing `InventoryReserved` or `InventoryReservationFailed` events to Kafka.
- `inventory.events` Kafka topic.
- Updating Order Service status (`CONFIRMED` / `INVENTORY_FAILED`).
- Saga compensations and automated release workflows via `ReleaseStock`.
