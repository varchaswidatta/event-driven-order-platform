import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import { Pool } from 'pg';
import { Kafka, Consumer } from 'kafkajs';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { OrderRepository } from '../../src/repositories/order.repository.js';
import { OutboxRepository } from '../../src/repositories/outbox.repository.js';
import { OrderService } from '../../src/services/order.service.js';
import { KafkaOrderProducer, IKafkaProducer } from '../../src/messaging/kafka/kafka.producer.js';
import { OutboxPublisher } from '../../src/messaging/outbox.publisher.js';
import { EventEnvelope, OrderCreatedPayload } from '../../src/domain/outbox-event.js';
import { env } from '../../src/config/env.js';

describe('Kafka Outbox Publisher Integration Tests (Live PostgreSQL + Kafka)', () => {
  let pool: Pool;
  let outboxRepo: OutboxRepository;
  let orderRepo: OrderRepository;
  let orderService: OrderService;
  let kafka: Kafka;
  let producer: KafkaOrderProducer;
  let publisher: OutboxPublisher;
  let consumer: Consumer;

  const testCustomerId = '11111111-2222-4333-8444-555555555555';
  const testProductId1 = '22222222-3333-4444-8555-666666666666';
  const testProductId2 = '33333333-4444-4555-8666-777777777777';

  beforeAll(async () => {
    // 1. Initialize PostgreSQL
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);

    outboxRepo = new OutboxRepository(pool);
    orderRepo = new OrderRepository(pool, outboxRepo);
    orderService = new OrderService(orderRepo);

    // 2. Initialize Kafka
    kafka = new Kafka({
      clientId: 'order-publisher-integration-test',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
      retry: {
        retries: 5,
        initialRetryTime: 100,
      },
    });

    producer = new KafkaOrderProducer(kafka);
    await producer.connect();

    publisher = new OutboxPublisher(outboxRepo, producer, {
      pollIntervalMs: 500,
      batchSize: 50,
    });

    // 3. Setup a Kafka test consumer to intercept published events
    consumer = kafka.consumer({
      groupId: `test-group-${crypto.randomUUID()}`,
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

    // Allow consumer to complete group assignment before tests begin
    await new Promise((resolve) => setTimeout(resolve, 1500));
  }, 30000);

  const capturedMessages: Array<{ key: string; value: string }> = [];

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
    if (pool) {
      await pool.query('DELETE FROM outbox_events;').catch(() => {});
      await pool.query('DELETE FROM order_items;').catch(() => {});
      await pool.query('DELETE FROM orders;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  it('publishes OrderCreated event from outbox to Kafka and marks event published', async () => {
    // 1. Create an order in PostgreSQL (persisting order + items + outbox event in ONE transaction)
    const createdOrder = await orderService.createOrder({
      customerId: testCustomerId,
      currency: 'USD',
      items: [
        { productId: testProductId1, quantity: 2, unitPrice: '29.99' },
        { productId: testProductId2, quantity: 1, unitPrice: '14.99' },
      ],
    });

    // Verify initial outbox state: published_at is NULL
    const initialOutboxEvents = await outboxRepo.findByAggregateId(createdOrder.id);
    expect(initialOutboxEvents.length).toBe(1);
    expect(initialOutboxEvents[0].publishedAt).toBeNull();
    expect(initialOutboxEvents[0].retryCount).toBe(0);

    // 2. Trigger Outbox Publisher cycle
    const publishedCount = await publisher.publishPendingEvents();
    expect(publishedCount).toBe(1);

    // Wait briefly for consumer to receive message from broker
    let retries = 0;
    while (capturedMessages.length === 0 && retries < 20) {
      await new Promise((resolve) => setTimeout(resolve, 150));
      retries++;
    }

    expect(capturedMessages.length).toBeGreaterThanOrEqual(1);

    const received = capturedMessages.find((m) => m.key === createdOrder.id);
    expect(received).toBeDefined();

    // 4. Verify Kafka Key is orderId
    expect(received!.key).toBe(createdOrder.id);

    // 5. Verify Event Envelope & Payload
    const envelope = JSON.parse(received!.value) as EventEnvelope<OrderCreatedPayload>;
    expect(envelope.eventType).toBe('OrderCreated');
    expect(envelope.eventVersion).toBe(1);
    expect(envelope.aggregateType).toBe('Order');
    expect(envelope.aggregateId).toBe(createdOrder.id);
    expect(envelope.payload.orderId).toBe(createdOrder.id);
    expect(envelope.payload.customerId).toBe(testCustomerId);
    expect(envelope.payload.items).toEqual([
      { productId: testProductId1, quantity: 2 },
      { productId: testProductId2, quantity: 1 },
    ]);

    // Safe monetary verification: strictly NO pricing fields in Kafka message payload
    expect(envelope.payload).not.toHaveProperty('totalAmount');
    expect(envelope.payload.items[0]).not.toHaveProperty('unitPrice');

    // 6. Verify outbox_events in PostgreSQL: published_at is populated!
    const updatedOutboxEvents = await outboxRepo.findByAggregateId(createdOrder.id);
    expect(updatedOutboxEvents.length).toBe(1);
    expect(updatedOutboxEvents[0].publishedAt).toBeInstanceOf(Date);
    expect(updatedOutboxEvents[0].retryCount).toBe(0);

    // 7. Verify no unpublished events remain
    const remainingUnpublished = await outboxRepo.findUnpublishedEvents();
    expect(remainingUnpublished.length).toBe(0);
  }, 20000);

  it('demonstrates resilience: increments retry_count on Kafka failure, then publishes upon recovery', async () => {
    // 1. Create order and outbox record
    const createdOrder = await orderService.createOrder({
      customerId: testCustomerId,
      currency: 'USD',
      items: [{ productId: testProductId1, quantity: 1, unitPrice: '10.00' }],
    });

    const targetEventId = (await outboxRepo.findByAggregateId(createdOrder.id))[0].id;

    // 2. Create a temporarily failing Kafka producer simulating Kafka broker outage
    let shouldFail = true;
    const resilientMockProducer: IKafkaProducer = {
      connect: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
      publish: vi.fn().mockImplementation(async (options) => {
        if (shouldFail) {
          throw new Error('Simulated Kafka cluster connection refused');
        }
        await producer.publish(options);
      }),
      isConnected: vi.fn().mockReturnValue(true),
    };

    const failingPublisher = new OutboxPublisher(outboxRepo, resilientMockProducer, {
      batchSize: 10,
    });

    // 3. Attempt publish during Kafka outage — cycle 1
    const count1 = await failingPublisher.publishPendingEvents();
    expect(count1).toBe(0);

    let eventInDb = await outboxRepo.findById(targetEventId);
    expect(eventInDb?.publishedAt).toBeNull();
    expect(eventInDb?.retryCount).toBe(1);

    // 4. Attempt publish during continuing Kafka outage — cycle 2
    const count2 = await failingPublisher.publishPendingEvents();
    expect(count2).toBe(0);

    eventInDb = await outboxRepo.findById(targetEventId);
    expect(eventInDb?.publishedAt).toBeNull();
    expect(eventInDb?.retryCount).toBe(2);

    // 5. Kafka cluster recovers!
    shouldFail = false;

    // 6. Run publisher cycle 3 — event is successfully dispatched
    const count3 = await failingPublisher.publishPendingEvents();
    expect(count3).toBe(1);

    eventInDb = await outboxRepo.findById(targetEventId);
    expect(eventInDb?.publishedAt).toBeInstanceOf(Date);
    // retryCount remains recorded as 2 from the previous attempts
    expect(eventInDb?.retryCount).toBe(2);
  }, 20000);
});
