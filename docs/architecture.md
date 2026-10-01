# System Architecture

## Overview

This document will detail the architectural design of the Event-Driven Order Fulfillment Platform, covering system topology, communication paradigms, and architectural trade-offs.

## Planned Contents

- **System Topology**: High-level overview of services and infrastructure components.
- **Domain Boundaries**: Responsibilities and data ownership for each microservice (API Gateway, Order Service, Inventory Service, Stock Service).
- **Communication Patterns**:
  - Asynchronous event-driven messaging via Apache Kafka.
  - Synchronous inter-service RPC via gRPC.
  - External client communication via GraphQL API Gateway.
- **Transactional Outbox Pattern**: Reliable dual-write mitigation between PostgreSQL and Kafka.
- **End-to-End Order Lifecycle**: Step-by-step trace of order placement, inventory verification, stock reservation, and state transition workflows.
