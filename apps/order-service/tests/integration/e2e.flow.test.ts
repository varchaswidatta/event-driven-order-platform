import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { FastifyInstance } from 'fastify';
import { Kafka, Consumer } from 'kafkajs';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { OrderRepository } from '../../src/repositories/order.repository.js';
import { OutboxRepository } from '../../src/repositories/outbox.repository.js';
import { OrderService } from '../../src/services/order.service.js';
import { buildHttpApp } from '../../src/http/app.js';
import { KafkaOrderProducer } from '../../src/messaging/kafka/kafka.producer.js';
import { OutboxPublisher } from '../../src/messaging/outbox.publisher.js';
import { OrderServiceClient } from '../../../api-gateway/src/clients/order-service.client.js';
import { startGatewayServer, RunningGatewayServer } from '../../../api-gateway/src/server.js';
import { EventEnvelope, OrderCreatedPayload } from '../../src/domain/outbox-event.js';
import { env } from '../../src/config/env.js';

describe('Phase 4 Complete E2E Flow (GraphQL Gateway -> Order Service -> PostgreSQL -> Outbox Publisher -> Kafka)', () => {
  let pool: Pool;
  let orderApp: FastifyInstance;
  let gateway: RunningGatewayServer;
  let gatewayUrl: string;
  let outboxRepo: OutboxRepository;
  let orderRepo: OrderRepository;
  let orderService: OrderService;
  let producer: KafkaOrderProducer;
  let publisher: OutboxPublisher;
  let kafka: Kafka;
  let consumer: Consumer;

  const testCustomerId = '99999999-aaaa-4bbb-8ccc-000000000001';
  const testProductId1 = '88888888-aaaa-4bbb-8ccc-000000000002';
  const testProductId2 = '77777777-aaaa-4bbb-8ccc-000000000003';

  const capturedMessages: Array<{ key: string; value: string }> = [];

  beforeAll(async () => {
    // 1. PostgreSQL setup
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);
    outboxRepo = new OutboxRepository(pool);
    orderRepo = new OrderRepository(pool, outboxRepo);
    orderService = new OrderService(orderRepo);

    // 2. Order Service HTTP App (listening on 4121)
    orderApp = buildHttpApp({ orderService });
    await orderApp.listen({ port: 4121, host: '127.0.0.1' });

    // 3. API Gateway (listening on 4120) pointing to Order Service HTTP
    const orderServiceClient = new OrderServiceClient({
      baseUrl: 'http://127.0.0.1:4121',
    });
    gateway = await startGatewayServer(4120, { orderServiceClient });
    gatewayUrl = 'http://127.0.0.1:4120/graphql';

    // 4. Kafka & Outbox Publisher setup
    kafka = new Kafka({
      clientId: 'e2e-order-flow-verifier',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    });

    producer = new KafkaOrderProducer(kafka);
    await producer.connect();

    publisher = new OutboxPublisher(outboxRepo, producer, {
      pollIntervalMs: 500,
      batchSize: 50,
    });

    // 5. Kafka consumer intercepting order.events
    consumer = kafka.consumer({
      groupId: `e2e-verifier-group-${crypto.randomUUID()}`,
      maxWaitTimeInMs: 100,
    });
    await consumer.connect();
    await consumer.subscribe({ topic: 'order.events', fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        if (message.key && message.value) {
          capturedMessages.push({
            key: message.key.toString(),
            value: message.value.toString(),
          });
        }
      },
    });

    // Wait for consumer partition assignment
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }, 35000);

  beforeEach(async () => {
    capturedMessages.length = 0;
    await pool.query('DELETE FROM outbox_events;');
    await pool.query('DELETE FROM order_items;');
    await pool.query('DELETE FROM orders;');
  });

  afterAll(async () => {
    if (publisher) {
      await publisher.stop();
    }
    if (consumer) {
      await consumer.disconnect().catch(() => {});
    }
    if (producer) {
      await producer.disconnect().catch(() => {});
    }
    if (gateway) {
      await gateway.stop();
    }
    if (orderApp) {
      await orderApp.close();
    }
    if (pool) {
      await pool.query('DELETE FROM outbox_events;').catch(() => {});
      await pool.query('DELETE FROM order_items;').catch(() => {});
      await pool.query('DELETE FROM orders;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  it('validates complete End-to-End flow: GraphQL mutation -> PostgreSQL transaction -> Outbox Publisher -> Kafka order.events', async () => {
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
              { productId: testProductId1, quantity: 3, unitPrice: '20.00' },
              { productId: testProductId2, quantity: 2, unitPrice: '15.50' },
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

    // Step 2: Confirm response status is PENDING
    expect(createdOrder.status).toBe('PENDING');
    expect(createdOrder.totalAmount).toBe('91.00'); // (3 * 20.00) + (2 * 15.50) = 60 + 31 = 91.00

    // Step 3: Confirm order exists in PostgreSQL
    const orderRows = await pool.query('SELECT * FROM orders WHERE id = $1;', [createdOrder.id]);
    expect(orderRows.rows.length).toBe(1);
    expect(orderRows.rows[0].status).toBe('PENDING');

    // Step 4: Confirm OrderCreated row exists in outbox_events table
    const outboxRows = await pool.query('SELECT * FROM outbox_events WHERE aggregate_id = $1;', [
      createdOrder.id,
    ]);
    expect(outboxRows.rows.length).toBe(1);
    expect(outboxRows.rows[0].event_type).toBe('OrderCreated');
    expect(outboxRows.rows[0].published_at).toBeNull();

    // Step 5 & 6: Run Outbox Publisher cycle and confirm event is dispatched to Kafka
    const publishedCount = await publisher.publishPendingEvents();
    expect(publishedCount).toBe(1);

    // Wait for message receipt on Kafka consumer
    let attempts = 0;
    while (capturedMessages.length === 0 && attempts < 25) {
      await new Promise((resolve) => setTimeout(resolve, 150));
      attempts++;
    }

    expect(capturedMessages.length).toBeGreaterThanOrEqual(1);
    const kafkaMessage = capturedMessages.find((m) => m.key === createdOrder.id);
    expect(kafkaMessage).toBeDefined();

    // Step 7: Confirm Kafka key is the orderId
    expect(kafkaMessage!.key).toBe(createdOrder.id);

    // Step 8: Confirm event envelope & payload matches the order
    const envelope = JSON.parse(kafkaMessage!.value) as EventEnvelope<OrderCreatedPayload>;
    expect(envelope.eventType).toBe('OrderCreated');
    expect(envelope.eventVersion).toBe(1);
    expect(envelope.aggregateType).toBe('Order');
    expect(envelope.aggregateId).toBe(createdOrder.id);
    expect(envelope.correlationId).toBeDefined();
    expect(envelope.payload).toEqual({
      orderId: createdOrder.id,
      customerId: testCustomerId,
      items: [
        { productId: testProductId1, quantity: 3 },
        { productId: testProductId2, quantity: 2 },
      ],
    });

    // Confirm PostgreSQL outbox_events row now has published_at populated
    const updatedOutboxRows = await pool.query(
      'SELECT published_at, retry_count FROM outbox_events WHERE aggregate_id = $1;',
      [createdOrder.id],
    );
    expect(updatedOutboxRows.rows[0].published_at).toBeInstanceOf(Date);
    expect(updatedOutboxRows.rows[0].retry_count).toBe(0);
  }, 25000);
});
