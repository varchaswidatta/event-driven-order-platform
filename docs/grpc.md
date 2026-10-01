# gRPC Communication Architecture

## Overview

This document will describe synchronous inter-service communication between the Inventory Service and Stock Service using gRPC and Protocol Buffers.

## Planned Contents

- **Service Definitions**: `StockService` proto definition covering stock queries and reservations.
- **RPC Methods**:
  - `CheckStock`: Verify product availability and current quantities.
  - `ReserveStock`: Atomically allocate stock for an active order.
  - `ReleaseStock`: Rollback or release reserved stock on order failure/cancellation.
- **Resilience & Fault Tolerance**: Deadlines, timeouts, exponential backoff retries, and circuit breaker patterns.
- **Error Handling**: Canonical gRPC status codes and error payload conventions.
