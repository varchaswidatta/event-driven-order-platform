# gRPC Communication Architecture & Specification

## 1. Overview & Architectural Role

In the Event-Driven Order Fulfillment Platform:

- **Kafka** serves as the **asynchronous inter-service event boundary** across autonomous business domains:
  ```
  Order Service ───(OrderCreated event via Kafka)───► Inventory Service
  ```
- **gRPC** serves as the **synchronous internal service-to-service communication boundary** between tightly-coupled domain peers:
  ```
  Inventory Service ───(synchronous gRPC ReserveStock)───► Stock Service
  ```

### Why gRPC for Inventory ➔ Stock?

1. **Immediate Feedback**: Inventory reservation processing requires immediate certainty of stock allocation before subsequent workflow steps can proceed.
2. **High Throughput & Efficiency**: HTTP/2 multiplexing, compact binary protobuf serialization, and persistent TCP connections yield substantially lower latency and resource overhead compared to REST/JSON.
3. **Strong Contract & Schema Governance**: Protocol Buffers provide strict compile-time types, backward compatibility rules, and language-agnostic interface definitions.

---

## 2. Protobuf Contract (`proto/stock.proto`)

```protobuf
syntax = "proto3";

package stock;

service StockService {
  rpc ReserveStock(ReserveStockRequest) returns (ReserveStockResponse);
  rpc ReleaseStock(ReleaseStockRequest) returns (ReleaseStockResponse);
}

message StockItem {
  string product_id = 1;
  int32 quantity = 2;
}

message ReserveStockRequest {
  string order_id = 1;
  repeated StockItem items = 2;
}

message ReserveStockResponse {
  bool success = 1;
  string reservation_id = 2;
  string failure_reason = 3;
}

message ReleaseStockRequest {
  string reservation_id = 1;
}

message ReleaseStockResponse {
  bool success = 1;
}
```

---

## 3. RPC Operations & Phase Scope

### `ReserveStock` (Implemented in Phase 6)

- **Input**: `order_id` (UUID), `items` (`repeated StockItem` containing `product_id` and positive `quantity`).
- **Atomicity**: Entire order reservation is executed within a single ACID transaction using `SELECT ... FOR UPDATE`.
- **Response**:
  - `success`: Boolean indicating allocation outcome.
  - `reservation_id`: Generated reservation UUID on success.
  - `failure_reason`: Canonical string indicating business refusal (`INSUFFICIENT_STOCK`, `PRODUCT_NOT_FOUND`, `INVALID_REQUEST`).

### `ReleaseStock` (Contract Placeholder)

- Defined in the proto contract to satisfy forward-compatible interface requirements.
- Responds explicitly with canonical status `grpc.status.UNIMPLEMENTED` to prevent callers from mistakenly assuming stock compensation occurred when no compensation logic is yet wired. Full saga rollback orchestration and compensation execution are deferred to Phase 7.

---

## 4. Business Failure vs. Infrastructure Failure

A critical architectural principle is distinguishing **business outcomes** from **infrastructure failures**:

| Category                   | Conditions                                                                                                         | Client Behavior                                                                                                                                                                     |
| :------------------------- | :----------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Business Failure**       | `INSUFFICIENT_STOCK`, `PRODUCT_NOT_FOUND`, `INVALID_REQUEST`                                                       | gRPC call completes normally with `success: false`. Inventory Service processes the rejection cleanly.                                                                              |
| **Infrastructure Failure** | Stock Service down (`UNAVAILABLE`), connection timeout (`DEADLINE_EXCEEDED`), internal database error (`INTERNAL`) | gRPC throws `StockServiceUnavailableError`, `StockServiceTimeoutError`, or `StockServiceError`. Inventory consumer re-throws to prevent Kafka offset commit, triggering redelivery. |

### Error Sanitization

Stock Service strictly sanitizes all internal errors. Database connection strings, SQL statements, raw error codes, and filesystem traces are logged securely server-side and mapped to generic `grpc.status.INTERNAL` responses, preventing information leakage.

---

## 5. Client Resilience & Configuration

### Deadlines & Timeouts

Every outgoing `ReserveStock` request is executed with a client deadline:

```typescript
const deadline = new Date(Date.now() + timeoutMs);
```

Default timeout is `5000ms`, configurable via `STOCK_SERVICE_TIMEOUT_MS`.

### Configuration Variables

| Variable                   | Default (Local) | Docker Compose  | Description                             |
| :------------------------- | :-------------- | :-------------- | :-------------------------------------- |
| `STOCK_SERVICE_GRPC_HOST`  | `localhost`     | `stock-service` | Target hostname for Stock gRPC server   |
| `STOCK_SERVICE_GRPC_PORT`  | `50051`         | `50051`         | Port bound by Stock gRPC server         |
| `STOCK_SERVICE_TIMEOUT_MS` | `5000`          | `5000`          | Client deadline timeout in milliseconds |

---

## 6. Graceful Shutdown

On process termination (`SIGINT` / `SIGTERM`):

1. **gRPC Server**: Calls `server.tryShutdown()` to reject new requests while permitting in-flight RPCs to complete within a drain period.
2. **Database Pool**: Closes the PostgreSQL connection pool gracefully.
3. **gRPC Client**: Closes active HTTP/2 channels via `client.close()`.
