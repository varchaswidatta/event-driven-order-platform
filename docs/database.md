# Database Architecture & Schemas

## Database Ownership & Isolation

The platform enforces a strict **Database-per-Service** pattern:

- **Order Service owns `order_db`**: The Order Service has exclusive read and write ownership of `order_db`.
- **No Cross-Service Database Access**: The Order Service never directly accesses or queries databases owned by other services (`inventory_db`, `stock_db`). All cross-service coordination will occur through asynchronous events and synchronous APIs in later phases.

---

## Order Database Schema (`order_db`)

The schema for `order_db` is managed via raw SQL migrations located in `apps/order-service/src/db/migrations/`.

### 1. `orders` Table

Stores top-level order records, customer identifiers, lifecycle statuses, and monetary totals.

```sql
CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL,
  status VARCHAR(50) NOT NULL CHECK (
    status IN ('PENDING', 'INVENTORY_PROCESSING', 'CONFIRMED', 'INVENTORY_FAILED')
  ),
  total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
  currency VARCHAR(3) NOT NULL CHECK (length(currency) = 3),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
```

#### Key Constraints:

- `status`: Enforces valid order states (`PENDING`, `INVENTORY_PROCESSING`, `CONFIRMED`, `INVENTORY_FAILED`).
- `total_amount`: Constrained to non-negative values (`CHECK (total_amount >= 0)`).
- `currency`: Enforces standard 3-character ISO currency codes.
- `customer_id` index: Accelerates querying customer order history.

### 2. `order_items` Table

Stores line items associated with each order, tracking product IDs, quantities, and unit prices.

```sql
CREATE TABLE IF NOT EXISTS order_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id UUID NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
```

#### Key Constraints:

- `order_id`: Foreign key reference to `orders(id)` with `ON DELETE CASCADE`.
- `quantity`: Must be a strictly positive integer (`CHECK (quantity > 0)`).
- `unit_price`: Constrained to non-negative values (`CHECK (unit_price >= 0)`).
- `order_id` index: Prevents full table scans when fetching order items for an order.

### 3. `outbox_events` Table

Stores domain events within the same database transaction as business entities (Transactional Outbox Pattern).

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

CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished
  ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;
```

#### Key Constraints & Indexing:

- `id`: Primary key matching domain `eventId`.
- `aggregate_id`: Foreign aggregate reference (points to `orders.id`).
- `published_at`: Stays `NULL` until published to message broker (Kafka in Phase 4).
- `idx_outbox_events_unpublished`: Partial index over unpublished events (`WHERE published_at IS NULL`) enabling efficient polling.
- For complete details, see [docs/outbox.md](outbox.md).

---

## Monetary Representation & Precision

Monetary values in `order_db` use **`NUMERIC(12, 2)`**:

- **Why not `FLOAT` / `DOUBLE PRECISION`**: IEEE-754 binary floating-point numbers cannot accurately represent base-10 fractions (e.g. `0.1 + 0.2 = 0.30000000000000004`), leading to silent rounding errors and financial reconciliation bugs.
- **Node-Postgres Mapping**: The `pg` driver intentionally parses PostgreSQL `NUMERIC` values as strings to avoid loss of precision in JavaScript numbers.
- **Application Boundary Handling**: The Order Service consumes and returns money as fixed-point decimal strings (e.g., `"19.99"`).
- **Exact Calculation**: Monetary math (such as `sum(quantity * unit_price)`) is computed via integer minor units (cents) using `BigInt`, eliminating any floating-point drift.

---

## Transactional Integrity During Order Creation

Order creation spans multiple database operations and is executed within a **single ACID transaction**:

```text
BEGIN;
  INSERT INTO orders (...) VALUES (...) RETURNING id, ...;
  INSERT INTO order_items (...) VALUES (...);
  INSERT INTO order_items (...) VALUES (...);
  INSERT INTO outbox_events (...) VALUES (...); -- OrderCreated event
COMMIT;
```

### Guarantees:

- **Atomicity**: An order is never created without its corresponding order items and outbox event. If inserting any item or the outbox event fails, the entire transaction is rolled back via `ROLLBACK`.
- **Client Management**: A dedicated PostgreSQL connection client is checked out from `pg.Pool`, used for all queries within the transaction, and safely released back to the pool in a `finally` block.

---

## Migration Strategy

Database changes are managed via a lightweight, zero-dependency SQL migration runner (`apps/order-service/src/db/migrate.ts` and `apps/inventory-service/src/db/migrate.ts`):

1. **Tracking Table**: Maintains a `schema_migrations` table recording each migration name and timestamp.
2. **Deterministic Order**: Migration files in `src/db/migrations/` are sorted alphabetically/numerically (e.g., `001_create_orders.sql`).
3. **Idempotent Execution**: Before applying each file, the runner checks `schema_migrations` to ensure no migration is applied twice.
4. **Transactional Migrations**: Each migration runs within its own transaction (`BEGIN` ... `COMMIT`). If an error occurs, the transaction is rolled back immediately, leaving the database in a clean state.

---

## Inventory Database Schema (`inventory_db`)

The schema for `inventory_db` is owned by the Inventory Service and managed via raw SQL migrations located in `apps/inventory-service/src/db/migrations/`.

### 1. `inventory_reservations` Table

Tracks reservations initiated in response to `OrderCreated` events and their subsequent stock allocation state.

```sql
CREATE TABLE IF NOT EXISTS inventory_reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL UNIQUE,
  status VARCHAR(50) NOT NULL DEFAULT 'PENDING' CHECK (
    status IN ('PENDING', 'RESERVED', 'FAILED')
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

#### Key Constraints:

- `order_id UNIQUE`: Enforces consumer idempotency. Prevents duplicate reservation rows when duplicate `OrderCreated` events are delivered by Kafka.
- `status`: Enforces valid states (`PENDING`, `RESERVED`, `FAILED`). Transitioned via `002_create_outbox_events.sql` in Phase 7.
- `created_at` / `updated_at`: `TIMESTAMPTZ` for timezone-safe auditability.

### 2. `inventory_reservation_items` Table

Stores individual product quantities requested for each reservation.

```sql
CREATE TABLE IF NOT EXISTS inventory_reservation_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reservation_id UUID NOT NULL REFERENCES inventory_reservations(id) ON DELETE CASCADE,
  product_id UUID NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reservation_items_reservation_id ON inventory_reservation_items(reservation_id);
```

#### Key Constraints:

- `quantity`: Must be positive (`CHECK (quantity > 0)`).
- `FOREIGN KEY ... ON DELETE CASCADE`: Deleting a reservation cleanly cascades to its line items.
- `idx_reservation_items_reservation_id`: Speeds up joins and queries by parent reservation.

### 3. `outbox_events` Table (Phase 7)

Stores inventory domain events (`InventoryReserved`, `InventoryReservationFailed`) within the same database transaction as the reservation status update (Transactional Outbox Pattern).

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

CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished
  ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;
```

#### Key Constraints & Indexing:

- `id`: Primary key matching domain `eventId`.
- `aggregate_id`: Foreign aggregate reference (points to `inventory_reservations.order_id`).
- `published_at`: Stays `NULL` until acknowledged by Kafka topic `inventory.events`.
- `idx_outbox_events_unpublished`: Partial index over unpublished events (`WHERE published_at IS NULL`) enabling efficient polling.

#### Transactional Atomicity:

When Stock Service returns the reservation outcome, Inventory Service performs:

```text
BEGIN;
  UPDATE inventory_reservations SET status = $1, updated_at = NOW() WHERE order_id = $2;
  INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload, correlation_id) VALUES (...);
COMMIT;
```

If anything fails, neither the status nor the outbox event commits, preventing dual-write inconsistencies.

## Stock Database Schema (`stock_db`)

The schema for `stock_db` is owned strictly and exclusively by the Stock Service. Managed via raw SQL migrations located in `apps/stock-service/src/db/migrations/`.

### 1. `products` Table

Stores catalog product identifiers, SKUs, names, and retail prices.

```sql
CREATE TABLE IF NOT EXISTS products (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sku VARCHAR(64) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL,
  price NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
```

#### Key Constraints:

- `sku UNIQUE`: SKU must be unique across the catalog.
- `price NUMERIC(12, 2)`: Exact fixed-point numeric money representation (`CHECK (price >= 0)`).
- `created_at` / `updated_at`: `TIMESTAMPTZ` audit timestamps.

### 2. `stock` Table

Maintains available and reserved quantities for each product.

```sql
CREATE TABLE IF NOT EXISTS stock (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id UUID NOT NULL UNIQUE REFERENCES products(id) ON DELETE CASCADE,
  available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
  reserved_quantity INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_product_id ON stock(product_id);
```

#### Key Constraints:

- `product_id UNIQUE`: Exactly one stock row per product.
- `available_quantity >= 0`: Available quantity can never drop below zero.
- `reserved_quantity >= 0`: Reserved quantity can never drop below zero.
- `FOREIGN KEY ... REFERENCES products(id)`: Cascades on product deletion.

### 3. `stock_reservations` Table

Tracks reservation audit logs on the stock service side for traceability and idempotency.

```sql
CREATE TABLE IF NOT EXISTS stock_reservations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL UNIQUE,
  status VARCHAR(50) NOT NULL DEFAULT 'RESERVED' CHECK (
    status IN ('RESERVED', 'RELEASED')
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_reservations_order_id ON stock_reservations(order_id);
```

#### Key Constraints:

- `order_id UNIQUE`: Guarantees idempotency. Even if a duplicate gRPC `ReserveStock` call arrives with the same `order_id`, stock is not deducted a second time.

### 4. `stock_reservation_items` Table

Maintains line items allocated for each reservation.

```sql
CREATE TABLE IF NOT EXISTS stock_reservation_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  reservation_id UUID NOT NULL REFERENCES stock_reservations(id) ON DELETE CASCADE,
  product_id UUID NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stock_res_items_reservation_id ON stock_reservation_items(reservation_id);
```

#### Key Constraints:

- `quantity > 0`: Allocated quantity must be strictly positive.
- `product_id REFERENCES products(id) ON DELETE RESTRICT`: Protects reserved products from accidental deletion.
