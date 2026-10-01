# Kafka Event Streaming Architecture

## Overview

This document will document topic topologies, event schema contracts, partitioning keys, and consumer group design in Apache Kafka.

## Planned Contents

- **Topic Topologies**:
  - `orders.v1`: Events emitted upon order placement and cancellation.
  - `inventory.v1`: Events emitted upon inventory allocation and status updates.
  - `orders.dlq`: Dead-letter queue for unprocessable messages.
- **Partitioning & Ordering Guarantees**: Partition key selection (e.g., `order_id`) to ensure strict in-order processing per entity.
- **Event Schemas**: JSON/Avro event contract specifications with versioning and evolution policies.
- **Consumer Group Design**: Offset commit strategies, consumer scaling, and backpressure management.
