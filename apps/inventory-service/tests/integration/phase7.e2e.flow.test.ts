import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';
import { Pool } from 'pg';
import { FastifyInstance } from 'fastify';
import { Kafka } from 'kafkajs';
import crypto from 'node:crypto';
import { createDatabasePool as createOrderDbPool } from '../../../order-service/src/db/client.js';
import { runMigrations as runOrderMigrations } from '../../../order-service/src/db/migrate.js';
import { OrderRepository } from '../../../order-service/src/repositories/order.repository.js';
import { OutboxRepository as OrderOutboxRepository } from '../../../order-service/src/repositories/outbox.repository.js';
import { OrderService } from '../../../order-service/src/services/order.service.js';
import { buildHttpApp } from '../../../order-service/src/http/app.js';
import { KafkaOrderProducer } from '../../../order-service/src/messaging/kafka/kafka.producer.js';
import { OutboxPublisher as OrderOutboxPublisher } from '../../../order-service/src/messaging/outbox.publisher.js';
import { InventoryEventsConsumer } from '../../../order-service/src/messaging/kafka/inventory-events.consumer.js';
import { OrderServiceClient } from '../../../api-gateway/src/clients/order-service.client.js';
import { startGatewayServer, RunningGatewayServer } from '../../../api-gateway/src/server.js';
import { createDatabasePool as createInventoryDbPool } from '../../src/db/client.js';
import { runMigrations as runInventoryMigrations } from '../../src/db/migrate.js';
import { InventoryRepository } from '../../src/repositories/inventory.repository.js';
import { InventoryOutboxRepository } from '../../src/repositories/outbox.repository.js';
import { InventoryService } from '../../src/services/inventory.service.js';
import { KafkaInventoryProducer } from '../../src/messaging/kafka/kafka.producer.js';
import { InventoryOutboxPublisher } from '../../src/messaging/outbox.publisher.js';
import { StockServiceClient } from '../../src/clients/stock-service.client.js';
import {
  OrderEventsConsumer,
  ORDER_EVENTS_TOPIC,
} from '../../src/messaging/kafka/order-events.consumer.js';
import { createDatabasePool as createStockDbPool } from '../../../stock-service/src/db/client.js';
import { runMigrations as runStockMigrations } from '../../../stock-service/src/db/migrate.js';
import { StockRepository } from '../../../stock-service/src/repositories/stock.repository.js';
import { startGrpcServer, RunningGrpcServer } from '../../../stock-service/src/grpc/server.js';
import { env } from '../../src/config/env.js';
import { RESERVATION_STATUS } from '../../src/domain/reservation-status.js';
import { ORDER_STATUS } from '../../../order-service/src/domain/order-status.js';

describe('Phase 7 Complete End-to-End Asynchronous Flow (GraphQL -> Order -> Outbox -> Kafka -> Inventory -> gRPC -> Stock -> Outbox -> Kafka -> Order -> CONFIRMED/INVENTORY_FAILED)', () => {
  let orderPool: Pool;
  let inventoryPool: Pool;
  let stockPool: Pool;

  let orderApp: FastifyInstance;
  let gateway: RunningGatewayServer;
  let gatewayUrl: string;

  let orderOutboxRepo: OrderOutboxRepository;
  let orderRepo: OrderRepository;
  let orderService: OrderService;
  let orderProducer: KafkaOrderProducer;
  let orderOutboxPublisher: OrderOutboxPublisher;
  let orderInventoryConsumer: InventoryEventsConsumer;

  let stockRepo: StockRepository;
  let grpcServer: RunningGrpcServer;
  let stockClient: StockServiceClient;

  let inventoryOutboxRepo: InventoryOutboxRepository;
  let inventoryRepo: InventoryRepository;
  let inventoryService: InventoryService;
  let inventoryProducer: KafkaInventoryProducer;
  let inventoryOutboxPublisher: InventoryOutboxPublisher;
  let inventoryConsumer: OrderEventsConsumer;

  let kafka: Kafka;

  const customerId = '33333333-cccc-4ddd-8eee-000000000001';

  beforeAll(async () => {
    // 1. Stock Service DB & gRPC server
    stockPool = createStockDbPool({ max: 10 });
    await runStockMigrations(stockPool);
    stockRepo = new StockRepository(stockPool);

    grpcServer = await startGrpcServer({
      port: 0,
      host: '127.0.0.1',
      pool: stockPool,
    });

    stockClient = new StockServiceClient({
      host: '127.0.0.1',
      port: grpcServer.port,
      timeoutMs: 5000,
    });

    // 2. Order Service DB setup
    orderPool = createOrderDbPool({ max: 5 });
    await runOrderMigrations(orderPool);
    orderOutboxRepo = new OrderOutboxRepository(orderPool);
    orderRepo = new OrderRepository(orderPool, orderOutboxRepo);
    orderService = new OrderService(orderRepo);

    // 3. Order Service HTTP App (port 4241)
    orderApp = buildHttpApp({ orderService });
    await orderApp.listen({ port: 4241, host: '127.0.0.1' });

    // 4. API Gateway (port 4240)
    const orderServiceClient = new OrderServiceClient({
      baseUrl: 'http://127.0.0.1:4241',
    });
    gateway = await startGatewayServer(4240, { orderServiceClient });
    gatewayUrl = 'http://127.0.0.1:4240/graphql';

    // 5. Kafka Client
    kafka = new Kafka({
      clientId: 'phase7-e2e-runner',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    });

    // 6. Order Service Outbox Publisher & Inventory Consumer
    orderProducer = new KafkaOrderProducer(kafka);
    await orderProducer.connect();

    orderOutboxPublisher = new OrderOutboxPublisher(orderOutboxRepo, orderProducer, {
      pollIntervalMs: 200,
      batchSize: 50,
    });
    await orderOutboxPublisher.start();

    orderInventoryConsumer = new InventoryEventsConsumer(kafka, orderRepo, {
      groupId: `phase7-order-group-${crypto.randomUUID()}`,
    });
    await orderInventoryConsumer.start();

    // 7. Inventory Service DB setup & Outbox Publisher
    inventoryPool = createInventoryDbPool({ max: 5 });
    await runInventoryMigrations(inventoryPool);
    inventoryOutboxRepo = new InventoryOutboxRepository(inventoryPool);
    inventoryRepo = new InventoryRepository(inventoryPool, inventoryOutboxRepo);
    inventoryService = new InventoryService(inventoryRepo, stockClient);

    inventoryProducer = new KafkaInventoryProducer(kafka);
    await inventoryProducer.connect();

    inventoryOutboxPublisher = new InventoryOutboxPublisher(
      inventoryOutboxRepo,
      inventoryProducer,
      {
        pollIntervalMs: 200,
        batchSize: 50,
      },
    );
    await inventoryOutboxPublisher.start();

    // 8. Inventory Service Kafka Consumer for order.events
    inventoryConsumer = new OrderEventsConsumer(kafka, inventoryService, {
      groupId: `phase7-inventory-group-${crypto.randomUUID()}`,
      topic: ORDER_EVENTS_TOPIC,
    });
    await inventoryConsumer.start();

    // Wait for Kafka consumer groups to rebalance
    await new Promise((resolve) => setTimeout(resolve, 3000));
  }, 45000);

  afterAll(async () => {
    await inventoryOutboxPublisher?.stop();
    await orderOutboxPublisher?.stop();
    await inventoryConsumer?.stop();
    await orderInventoryConsumer?.stop();
    await inventoryProducer?.disconnect();
    await orderProducer?.disconnect();
    stockClient?.close();
    grpcServer?.close();
    await gateway?.stop();
    await orderApp?.close();
    await orderPool?.end();
    await inventoryPool?.end();
    await stockPool?.end();
  });

  // D. Full E2E SUCCESS scenario
  it('executes full Phase 7 success flow: GraphQL createOrder -> order_db -> outbox -> Kafka -> inventory_db -> gRPC ReserveStock -> stock_db -> inventory outbox -> Kafka -> order_db CONFIRMED', async () => {
    // 1. Seed product with 100 available stock in stock_db
    const { product: prodA } = await stockRepo.createProductWithStock({
      sku: `SKU-P7-SUCC-1-${crypto.randomUUID().slice(0, 8)}`,
      name: 'Phase 7 Success Product A',
      price: '50.00',
      availableQuantity: 100,
    });
    const { product: prodB } = await stockRepo.createProductWithStock({
      sku: `SKU-P7-SUCC-2-${crypto.randomUUID().slice(0, 8)}`,
      name: 'Phase 7 Success Product B',
      price: '25.00',
      availableQuantity: 50,
    });

    // 2. Call GraphQL createOrder mutation
    const createOrderMutation = `
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
        }
      }
    `;

    const response = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: createOrderMutation,
        variables: {
          input: {
            customerId,
            items: [
              { productId: prodA.id, quantity: 4, unitPrice: '50.00' },
              { productId: prodB.id, quantity: 2, unitPrice: '25.00' },
            ],
          },
        },
      }),
    });

    const body = (await response.json()) as {
      data?: { createOrder: { id: string; status: string } };
      errors?: Array<{ message: string }>;
    };
    expect(body.errors).toBeUndefined();
    expect(body.data?.createOrder).toBeDefined();

    const createdOrder = body.data!.createOrder;
    expect(createdOrder.status).toBe('PENDING');

    // 3. Poll order_db until the status transitions to CONFIRMED
    let orderRow = null;
    let attempts = 0;
    while (attempts < 40) {
      await new Promise((resolve) => setTimeout(resolve, 300));
      const res = await orderPool.query('SELECT id, status FROM orders WHERE id = $1;', [
        createdOrder.id,
      ]);
      if (res.rows.length > 0 && res.rows[0].status === ORDER_STATUS.CONFIRMED) {
        orderRow = res.rows[0];
        break;
      }
      attempts++;
    }

    expect(orderRow).not.toBeNull();
    expect(orderRow!.status).toBe(ORDER_STATUS.CONFIRMED);

    // 4. Verify inventory reservation in inventory_db is RESERVED
    const invRes = await inventoryRepo.findReservationByOrderId(createdOrder.id);
    expect(invRes).not.toBeNull();
    expect(invRes!.status).toBe(RESERVATION_STATUS.RESERVED);
    expect(invRes!.items).toHaveLength(2);

    // 5. Verify stock reservation in stock_db is RESERVED and stock deducted
    const stockRes = await stockRepo.getReservationByOrderId(createdOrder.id);
    expect(stockRes).not.toBeNull();
    expect(stockRes!.status).toBe('RESERVED');
    expect(stockRes!.items).toHaveLength(2);

    const stockA = await stockRepo.getStock(prodA.id);
    expect(stockA!.availableQuantity).toBe(96);
    expect(stockA!.reservedQuantity).toBe(4);

    const stockB = await stockRepo.getStock(prodB.id);
    expect(stockB!.availableQuantity).toBe(48);
    expect(stockB!.reservedQuantity).toBe(2);

    // 6. Query GraphQL gateway to observe final CONFIRMED status
    const queryOrder = `
      query GetOrder($id: ID!) {
        order(id: $id) {
          id
          status
        }
      }
    `;

    const getRes = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: queryOrder,
        variables: { id: createdOrder.id },
      }),
    });

    const getBody = (await getRes.json()) as {
      data?: { order: { id: string; status: string } };
    };
    expect(getBody.data?.order.status).toBe('CONFIRMED');
  }, 35000);

  // E. Full E2E INSUFFICIENT-STOCK scenario
  it('executes full Phase 7 business failure flow: insufficient stock -> InventoryReservationFailed -> INVENTORY_FAILED', async () => {
    // 1. Seed product with available = 2
    const { product: prodLow } = await stockRepo.createProductWithStock({
      sku: `SKU-P7-FAIL-1-${crypto.randomUUID().slice(0, 8)}`,
      name: 'Phase 7 Low Stock Product',
      price: '10.00',
      availableQuantity: 2,
    });

    // 2. Call GraphQL createOrder with requested quantity = 10 (exceeds 2)
    const createOrderMutation = `
      mutation CreateOrder($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id
          status
        }
      }
    `;

    const response = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: createOrderMutation,
        variables: {
          input: {
            customerId,
            items: [{ productId: prodLow.id, quantity: 10, unitPrice: '10.00' }],
          },
        },
      }),
    });

    const body = (await response.json()) as {
      data?: { createOrder: { id: string; status: string } };
    };
    const orderId = body.data!.createOrder.id;

    // 3. Poll order_db until the status transitions to INVENTORY_FAILED
    let orderRow = null;
    let attempts = 0;
    while (attempts < 40) {
      await new Promise((resolve) => setTimeout(resolve, 300));
      const res = await orderPool.query('SELECT id, status FROM orders WHERE id = $1;', [orderId]);
      if (res.rows.length > 0 && res.rows[0].status === ORDER_STATUS.INVENTORY_FAILED) {
        orderRow = res.rows[0];
        break;
      }
      attempts++;
    }

    expect(orderRow).not.toBeNull();
    expect(orderRow!.status).toBe(ORDER_STATUS.INVENTORY_FAILED);

    // 4. Invariants check: no negative stock, no partial mutation in stock_db
    const stock = await stockRepo.getStock(prodLow.id);
    expect(stock!.availableQuantity).toBe(2);
    expect(stock!.reservedQuantity).toBe(0);

    const stockRes = await stockRepo.getReservationByOrderId(orderId);
    expect(stockRes).toBeNull();

    // 5. Inventory reservation in inventory_db is FAILED
    const invRes = await inventoryRepo.findReservationByOrderId(orderId);
    expect(invRes).not.toBeNull();
    expect(invRes!.status).toBe(RESERVATION_STATUS.FAILED);

    // 6. Query GraphQL gateway to observe final INVENTORY_FAILED status
    const queryOrder = `
      query GetOrder($id: ID!) {
        order(id: $id) {
          id
          status
        }
      }
    `;

    const getRes = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: queryOrder,
        variables: { id: orderId },
      }),
    });

    const getBody = (await getRes.json()) as {
      data?: { order: { id: string; status: string } };
    };
    expect(getBody.data?.order.status).toBe('INVENTORY_FAILED');
  }, 35000);

  // F. Failure/retry scenario
  it('handles transient stock service infrastructure failure without marking business failure', async () => {
    // When stock client encounters an unavailable service, it throws an infrastructure error.
    // Inventory Service must NOT mark the reservation FAILED; it must throw so Kafka redelivers.
    const mockFailingStockClient = {
      reserveStock: vi
        .fn()
        .mockRejectedValue(new Error('Stock Service is unreachable: UNAVAILABLE')),
      close: vi.fn(),
    };

    const tempInventoryService = new InventoryService(inventoryRepo, mockFailingStockClient);

    const testOrderId = crypto.randomUUID();
    const event = {
      eventId: crypto.randomUUID(),
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'Order',
      aggregateId: testOrderId,
      correlationId: crypto.randomUUID(),
      payload: {
        orderId: testOrderId,
        customerId,
        items: [{ productId: crypto.randomUUID(), quantity: 1 }],
      },
    };

    // Processing must reject with infrastructure error
    await expect(tempInventoryService.processOrderCreatedEvent(event)).rejects.toThrow(
      'Stock Service is unreachable: UNAVAILABLE',
    );

    // Reservation in inventory_db must NOT be marked FAILED
    const invRes = await inventoryRepo.findReservationByOrderId(testOrderId);
    expect(invRes).not.toBeNull();
    expect(invRes!.status).toBe(RESERVATION_STATUS.PENDING); // Still PENDING, not FAILED
  });

  // G. Duplicate-delivery scenario
  it('handles duplicate OrderCreated and InventoryReserved deliveries idempotently', async () => {
    const { product: prodIdem } = await stockRepo.createProductWithStock({
      sku: `SKU-P7-IDEM-1-${crypto.randomUUID().slice(0, 8)}`,
      name: 'Phase 7 Idempotency Product',
      price: '15.00',
      availableQuantity: 20,
    });

    // Create order via GraphQL
    const createOrderMutation = `
      mutation CreateOrder($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id
          status
        }
      }
    `;

    const response = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: createOrderMutation,
        variables: {
          input: {
            customerId,
            items: [{ productId: prodIdem.id, quantity: 5, unitPrice: '15.00' }],
          },
        },
      }),
    });

    const body = (await response.json()) as {
      data?: { createOrder: { id: string } };
    };
    const orderId = body.data!.createOrder.id;

    // Wait until order becomes CONFIRMED
    let confirmed = false;
    let attempts = 0;
    while (!confirmed && attempts < 40) {
      await new Promise((resolve) => setTimeout(resolve, 300));
      const res = await orderPool.query('SELECT status FROM orders WHERE id = $1;', [orderId]);
      if (res.rows[0]?.status === ORDER_STATUS.CONFIRMED) {
        confirmed = true;
      }
      attempts++;
    }
    expect(confirmed).toBe(true);

    // Verify stock deduction: available = 15, reserved = 5
    const stockBeforeDup = await stockRepo.getStock(prodIdem.id);
    expect(stockBeforeDup!.availableQuantity).toBe(15);
    expect(stockBeforeDup!.reservedQuantity).toBe(5);

    // Replay duplicate OrderCreated event directly to inventory service
    const replayEvent = {
      eventId: crypto.randomUUID(),
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'Order',
      aggregateId: orderId,
      correlationId: crypto.randomUUID(),
      payload: {
        orderId,
        customerId,
        items: [{ productId: prodIdem.id, quantity: 5 }],
      },
    };

    const idemResult = await inventoryService.processOrderCreatedEvent(replayEvent);
    expect(idemResult.alreadyExisted).toBe(true);

    // Stock must NOT be deducted again!
    const stockAfterDup = await stockRepo.getStock(prodIdem.id);
    expect(stockAfterDup!.availableQuantity).toBe(15);
    expect(stockAfterDup!.reservedQuantity).toBe(5);

    // Now replay duplicate InventoryReserved event to OrderRepository
    const updateResult = await orderRepo.updateOrderStatus(orderId, ORDER_STATUS.CONFIRMED);
    expect(updateResult.alreadyUpdated).toBe(true);
    expect(updateResult.order.status).toBe(ORDER_STATUS.CONFIRMED);
  }, 35000);
});
