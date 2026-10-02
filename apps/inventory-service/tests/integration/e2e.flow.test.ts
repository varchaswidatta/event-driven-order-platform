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
import {
  OrderEventsConsumer,
  ORDER_EVENTS_TOPIC,
} from '../../src/messaging/kafka/order-events.consumer.js';
import { env } from '../../src/config/env.js';
import { RESERVATION_STATUS } from '../../src/domain/reservation-status.js';

describe('Phase 5 Complete End-to-End Flow (GraphQL Gateway -> Order Service -> Outbox -> Kafka -> Inventory Service -> inventory_db)', () => {
  let orderPool: Pool;
  let inventoryPool: Pool;
  let orderApp: FastifyInstance;
  let gateway: RunningGatewayServer;
  let gatewayUrl: string;
  let outboxRepo: OutboxRepository;
  let orderRepo: OrderRepository;
  let orderService: OrderService;
  let producer: KafkaOrderProducer;
  let publisher: OutboxPublisher;
  let inventoryRepo: InventoryRepository;
  let inventoryService: InventoryService;
  let consumer: OrderEventsConsumer;
  let kafka: Kafka;

  const testCustomerId = '11111111-aaaa-4bbb-8ccc-000000000001';
  const testProductId1 = '22222222-aaaa-4bbb-8ccc-000000000002';
  const testProductId2 = '33333333-aaaa-4bbb-8ccc-000000000003';

  beforeAll(async () => {
    // 1. Order Service database setup (order_db)
    orderPool = createOrderDbPool({ max: 5 });
    await runOrderMigrations(orderPool);
    outboxRepo = new OutboxRepository(orderPool);
    orderRepo = new OrderRepository(orderPool, outboxRepo);
    orderService = new OrderService(orderRepo);

    // 2. Order Service HTTP App (listening on port 4221)
    orderApp = buildHttpApp({ orderService });
    await orderApp.listen({ port: 4221, host: '127.0.0.1' });

    // 3. API Gateway (listening on port 4220) pointing to Order Service
    const orderServiceClient = new OrderServiceClient({
      baseUrl: 'http://127.0.0.1:4221',
    });
    gateway = await startGatewayServer(4220, { orderServiceClient });
    gatewayUrl = 'http://127.0.0.1:4220/graphql';

    // 4. Kafka & Order Outbox Publisher
    kafka = new Kafka({
      clientId: 'e2e-phase5-runner',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    });

    producer = new KafkaOrderProducer(kafka);
    await producer.connect();

    publisher = new OutboxPublisher(outboxRepo, producer, {
      pollIntervalMs: 500,
      batchSize: 50,
    });

    // 5. Inventory Service database setup (inventory_db)
    inventoryPool = createInventoryDbPool({ max: 5 });
    await runInventoryMigrations(inventoryPool);
    inventoryRepo = new InventoryRepository(inventoryPool);
    inventoryService = new InventoryService(inventoryRepo);

    // 6. Inventory Service Kafka Consumer
    consumer = new OrderEventsConsumer(kafka, inventoryService, {
      groupId: `e2e-inventory-group-${crypto.randomUUID()}`,
      topic: ORDER_EVENTS_TOPIC,
    });
    await consumer.start();

    // Stabilization delay for consumer partition assignment
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }, 40000);

  beforeEach(async () => {
    // Clean up both databases between tests
    await inventoryPool.query('DELETE FROM inventory_reservation_items;');
    await inventoryPool.query('DELETE FROM inventory_reservations;');
    await orderPool.query('DELETE FROM outbox_events;');
    await orderPool.query('DELETE FROM order_items;');
    await orderPool.query('DELETE FROM orders;');
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
  });

  it('executes full asynchronous flow: GraphQL createOrder -> order_db -> outbox -> Kafka -> inventory_db PENDING reservation', async () => {
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

    // Step 1: Send GraphQL createOrder mutation to API Gateway
    const response = await fetch(gatewayUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        query: CREATE_ORDER_MUTATION,
        variables: {
          input: {
            customerId: testCustomerId,
            items: [
              { productId: testProductId1, quantity: 2, unitPrice: '25.00' },
              { productId: testProductId2, quantity: 3, unitPrice: '10.00' },
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

    // Step 2: Confirm GraphQL immediately returns status = PENDING
    expect(createdOrder.status).toBe('PENDING');
    expect(createdOrder.totalAmount).toBe('80.00'); // (2 * 25.00) + (3 * 10.00) = 50 + 30 = 80.00

    // Step 3: Verify order exists in order_db
    const orderRows = await orderPool.query('SELECT * FROM orders WHERE id = $1;', [
      createdOrder.id,
    ]);
    expect(orderRows.rows.length).toBe(1);
    expect(orderRows.rows[0].status).toBe('PENDING');

    // Step 4: Verify outbox_events row exists in order_db
    const outboxRows = await orderPool.query(
      'SELECT * FROM outbox_events WHERE aggregate_id = $1;',
      [createdOrder.id],
    );
    expect(outboxRows.rows.length).toBe(1);
    expect(outboxRows.rows[0].event_type).toBe('OrderCreated');

    // Step 5: Trigger Outbox Publisher cycle to publish OrderCreated to Kafka
    const publishedCount = await publisher.publishPendingEvents();
    expect(publishedCount).toBe(1);

    // Step 6: Wait for Inventory Service Kafka consumer to receive and process event
    let reservation = null;
    let attempts = 0;
    while (!reservation && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      reservation = await inventoryRepo.findReservationByOrderId(createdOrder.id);
      attempts++;
    }

    // Step 7: Verify inventory_reservations row exists in inventory_db with status = PENDING
    expect(reservation).not.toBeNull();
    expect(reservation!.orderId).toBe(createdOrder.id);
    expect(reservation!.status).toBe(RESERVATION_STATUS.PENDING);

    // Step 8: Verify inventory_reservation_items match OrderCreated payload
    expect(reservation!.items).toHaveLength(2);
    const item1 = reservation!.items.find((i) => i.productId === testProductId1);
    const item2 = reservation!.items.find((i) => i.productId === testProductId2);
    expect(item1).toBeDefined();
    expect(item1!.quantity).toBe(2);
    expect(item2).toBeDefined();
    expect(item2!.quantity).toBe(3);
  }, 30000);
});
