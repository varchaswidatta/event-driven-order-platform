import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { FastifyInstance } from 'fastify';
import { Kafka } from 'kafkajs';
import crypto from 'node:crypto';
import { createDatabasePool as createOrderDbPool } from '../../../order-service/src/db/client.js';
import { runMigrations as runOrderMigrations } from '../../../order-service/src/db/migrate.js';
import { OrderRepository } from '../../../order-service/src/repositories/order.repository.js';
import { OutboxRepository } from '../../../order-service/src/repositories/outbox.repository.js';
import { OrderService } from '../../../order-service/src/services/order.service.js';
import { buildHttpApp } from '../../../order-service/src/http/app.js';
import { KafkaOrderProducer } from '../../../order-service/src/messaging/kafka/kafka.producer.js';
import { OutboxPublisher } from '../../../order-service/src/messaging/outbox.publisher.js';
import { OrderServiceClient } from '../../../api-gateway/src/clients/order-service.client.js';
import { startGatewayServer, RunningGatewayServer } from '../../../api-gateway/src/server.js';
import { createDatabasePool as createInventoryDbPool } from '../../src/db/client.js';
import { runMigrations as runInventoryMigrations } from '../../src/db/migrate.js';
import { InventoryRepository } from '../../src/repositories/inventory.repository.js';
import { InventoryService } from '../../src/services/inventory.service.js';
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

describe('Phase 6 Complete End-to-End Flow (GraphQL -> Order -> Outbox -> Kafka -> Inventory -> gRPC -> Stock -> stock_db)', () => {
  let orderPool: Pool;
  let inventoryPool: Pool;
  let stockPool: Pool;

  let orderApp: FastifyInstance;
  let gateway: RunningGatewayServer;
  let gatewayUrl: string;

  let outboxRepo: OutboxRepository;
  let orderRepo: OrderRepository;
  let orderService: OrderService;
  let producer: KafkaOrderProducer;
  let publisher: OutboxPublisher;

  let stockRepo: StockRepository;
  let grpcServer: RunningGrpcServer;
  let stockClient: StockServiceClient;

  let inventoryRepo: InventoryRepository;
  let inventoryService: InventoryService;
  let consumer: OrderEventsConsumer;
  let kafka: Kafka;

  const customerId = '11111111-cccc-4ddd-8eee-000000000001';

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
    outboxRepo = new OutboxRepository(orderPool);
    orderRepo = new OrderRepository(orderPool, outboxRepo);
    orderService = new OrderService(orderRepo);

    // 3. Order Service HTTP App (port 4231)
    orderApp = buildHttpApp({ orderService });
    await orderApp.listen({ port: 4231, host: '127.0.0.1' });

    // 4. API Gateway (port 4230)
    const orderServiceClient = new OrderServiceClient({
      baseUrl: 'http://127.0.0.1:4231',
    });
    gateway = await startGatewayServer(4230, { orderServiceClient });
    gatewayUrl = 'http://127.0.0.1:4230/graphql';

    // 5. Kafka & Outbox Publisher
    kafka = new Kafka({
      clientId: 'phase6-e2e-runner',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    });

    producer = new KafkaOrderProducer(kafka);
    await producer.connect();

    publisher = new OutboxPublisher(outboxRepo, producer, {
      pollIntervalMs: 500,
      batchSize: 50,
    });

    // 6. Inventory Service DB setup
    inventoryPool = createInventoryDbPool({ max: 5 });
    await runInventoryMigrations(inventoryPool);
    inventoryRepo = new InventoryRepository(inventoryPool);
    inventoryService = new InventoryService(inventoryRepo, stockClient);

    // 7. Inventory Service Kafka Consumer
    consumer = new OrderEventsConsumer(kafka, inventoryService, {
      groupId: `phase6-e2e-group-${crypto.randomUUID()}`,
      topic: ORDER_EVENTS_TOPIC,
    });
    await consumer.start();

    // Stabilization delay
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }, 45000);

  beforeEach(async () => {
    await inventoryPool.query('DELETE FROM inventory_reservation_items;');
    await inventoryPool.query('DELETE FROM inventory_reservations;');
    await orderPool.query('DELETE FROM outbox_events;');
    await orderPool.query('DELETE FROM order_items;');
    await orderPool.query('DELETE FROM orders;');
    await stockPool.query('DELETE FROM stock_reservation_items;');
    await stockPool.query('DELETE FROM stock_reservations;');
    await stockPool.query('DELETE FROM stock;');
    await stockPool.query('DELETE FROM products;');
  });

  afterAll(async () => {
    if (publisher) {
      await publisher.stop().catch(() => {});
    }
    if (consumer) {
      await consumer.stop().catch(() => {});
    }
    if (producer) {
      await producer.disconnect().catch(() => {});
    }
    if (gateway) {
      await gateway.stop().catch(() => {});
    }
    if (orderApp) {
      await orderApp.close().catch(() => {});
    }
    if (stockClient) {
      stockClient.close();
    }
    if (grpcServer) {
      await grpcServer.close().catch(() => {});
    }
    if (orderPool) {
      await orderPool.query('DELETE FROM outbox_events;').catch(() => {});
      await orderPool.query('DELETE FROM order_items;').catch(() => {});
      await orderPool.query('DELETE FROM orders;').catch(() => {});
      await orderPool.end().catch(() => {});
    }
    if (inventoryPool) {
      await inventoryPool.query('DELETE FROM inventory_reservation_items;').catch(() => {});
      await inventoryPool.query('DELETE FROM inventory_reservations;').catch(() => {});
      await inventoryPool.end().catch(() => {});
    }
    if (stockPool) {
      await stockPool.query('DELETE FROM stock_reservation_items;').catch(() => {});
      await stockPool.query('DELETE FROM stock_reservations;').catch(() => {});
      await stockPool.query('DELETE FROM stock;').catch(() => {});
      await stockPool.query('DELETE FROM products;').catch(() => {});
      await stockPool.end().catch(() => {});
    }
  });

  it('executes full Phase 6 flow: GraphQL createOrder -> order_db -> outbox -> Kafka -> inventory_db -> gRPC ReserveStock -> stock_db', async () => {
    // 1. Seed two products in stock_db
    const { product: prod1 } = await stockRepo.createProductWithStock({
      sku: 'SKU-E2E-1',
      name: 'E2E Product 1',
      price: '30.00',
      availableQuantity: 15,
    });

    const { product: prod2 } = await stockRepo.createProductWithStock({
      sku: 'SKU-E2E-2',
      name: 'E2E Product 2',
      price: '20.00',
      availableQuantity: 10,
    });

    const CREATE_ORDER_MUTATION = `#graphql
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

    // 2. Call GraphQL createOrder mutation on API Gateway
    const response = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: CREATE_ORDER_MUTATION,
        variables: {
          input: {
            customerId,
            items: [
              { productId: prod1.id, quantity: 3, unitPrice: '30.00' },
              { productId: prod2.id, quantity: 2, unitPrice: '20.00' },
            ],
          },
        },
      }),
    });

    const body = (await response.json()) as {
      data?: {
        createOrder: {
          id: string;
          customerId: string;
          status: string;
          totalAmount: string;
          currency: string;
          items: Array<{ productId: string; quantity: number; unitPrice: string }>;
        };
      };
      errors?: Array<{ message: string }>;
    };

    expect(body.errors).toBeUndefined();
    const createdOrder = body.data!.createOrder;

    // Invariant: GraphQL mutation returns PENDING status in Phase 6
    expect(createdOrder.status).toBe('PENDING');
    expect(createdOrder.totalAmount).toBe('130.00'); // (3 * 30) + (2 * 20) = 90 + 40 = 130.00

    // 3. Verify order & outbox in order_db
    const orderRows = await orderPool.query('SELECT * FROM orders WHERE id = $1;', [
      createdOrder.id,
    ]);
    expect(orderRows.rows[0].status).toBe('PENDING');

    const outboxRows = await orderPool.query(
      'SELECT * FROM outbox_events WHERE aggregate_id = $1;',
      [createdOrder.id],
    );
    expect(outboxRows.rows.length).toBe(1);

    // 4. Trigger Outbox Publisher cycle
    const publishedCount = await publisher.publishPendingEvents();
    expect(publishedCount).toBe(1);

    // 5. Poll inventory_db until reservation appears
    let invRes = null;
    let attempts = 0;
    while (!invRes && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      invRes = await inventoryRepo.findReservationByOrderId(createdOrder.id);
      attempts++;
    }

    expect(invRes).not.toBeNull();
    expect(invRes!.orderId).toBe(createdOrder.id);
    expect([RESERVATION_STATUS.PENDING, RESERVATION_STATUS.RESERVED]).toContain(invRes!.status);
    expect(invRes!.items).toHaveLength(2);

    // 6. Poll stock_db until Stock Service records the gRPC reservation
    let stockRes = null;
    let stockAttempts = 0;
    while (!stockRes && stockAttempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      stockRes = await stockRepo.getReservationByOrderId(createdOrder.id);
      stockAttempts++;
    }

    expect(stockRes).not.toBeNull();
    expect(stockRes!.orderId).toBe(createdOrder.id);
    expect(stockRes!.status).toBe('RESERVED');
    expect(stockRes!.items).toHaveLength(2);

    // 7. Verify stock deductions in stock_db
    const stock1 = await stockRepo.getStock(prod1.id);
    expect(stock1!.availableQuantity).toBe(12); // 15 - 3
    expect(stock1!.reservedQuantity).toBe(3);

    const stock2 = await stockRepo.getStock(prod2.id);
    expect(stock2!.availableQuantity).toBe(8); // 10 - 2
    expect(stock2!.reservedQuantity).toBe(2);
  }, 35000);
});
