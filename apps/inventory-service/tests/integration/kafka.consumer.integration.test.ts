import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { Kafka, Producer } from 'kafkajs';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { InventoryRepository } from '../../src/repositories/inventory.repository.js';
import { InventoryService } from '../../src/services/inventory.service.js';
import {
  OrderEventsConsumer,
  ORDER_EVENTS_TOPIC,
} from '../../src/messaging/kafka/order-events.consumer.js';
import { env } from '../../src/config/env.js';
import { RESERVATION_STATUS } from '../../src/domain/reservation-status.js';

describe('Inventory Service Kafka Consumer Integration Tests', () => {
  let pool: Pool;
  let repository: InventoryRepository;
  let inventoryService: InventoryService;
  let consumer: OrderEventsConsumer;
  let kafka: Kafka;
  let testProducer: Producer;
  let testGroupId: string;

  beforeAll(async () => {
    // 1. PostgreSQL setup
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);
    repository = new InventoryRepository(pool);
    inventoryService = new InventoryService(repository);

    // 2. Kafka setup
    kafka = new Kafka({
      clientId: 'inventory-consumer-test-client',
      brokers: env.KAFKA_BROKERS.split(',').map((b) => b.trim()),
    });

    testProducer = kafka.producer();
    await testProducer.connect();

    // Dedicated consumer group per test run to avoid rebalance delays
    testGroupId = `inventory-test-group-${crypto.randomUUID()}`;
    consumer = new OrderEventsConsumer(kafka, inventoryService, {
      groupId: testGroupId,
      topic: ORDER_EVENTS_TOPIC,
    });

    await consumer.start();

    // Brief stabilization pause for consumer partition assignment
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }, 35000);

  beforeEach(async () => {
    await pool.query('DELETE FROM inventory_reservation_items;');
    await pool.query('DELETE FROM inventory_reservations;');
  });

  afterAll(async () => {
    if (consumer) {
      await consumer.stop().catch(() => {});
    }
    if (testProducer) {
      await testProducer.disconnect().catch(() => {});
    }
    if (pool) {
      await pool.query('DELETE FROM inventory_reservation_items;').catch(() => {});
      await pool.query('DELETE FROM inventory_reservations;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  it('consumes a valid OrderCreated event and creates PENDING reservation and items', async () => {
    const orderId = crypto.randomUUID();
    const customerId = crypto.randomUUID();
    const productId1 = crypto.randomUUID();
    const productId2 = crypto.randomUUID();

    const orderCreatedMessage = {
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
        items: [
          { productId: productId1, quantity: 2 },
          { productId: productId2, quantity: 5 },
        ],
      },
    };

    // Publish to order.events with orderId as key
    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Poll database until consumer persists the reservation
    let reservation = null;
    let attempts = 0;
    while (!reservation && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      reservation = await repository.findReservationByOrderId(orderId);
      attempts++;
    }

    expect(reservation).not.toBeNull();
    expect(reservation!.orderId).toBe(orderId);
    expect(reservation!.status).toBe(RESERVATION_STATUS.PENDING);
    expect(reservation!.items).toHaveLength(2);

    const item1 = reservation!.items.find((i) => i.productId === productId1);
    const item2 = reservation!.items.find((i) => i.productId === productId2);
    expect(item1).toBeDefined();
    expect(item1!.quantity).toBe(2);
    expect(item2).toBeDefined();
    expect(item2!.quantity).toBe(5);
  }, 25000);

  it('handles duplicate OrderCreated delivery idempotently without creating duplicate rows', async () => {
    const orderId = crypto.randomUUID();
    const customerId = crypto.randomUUID();
    const productId = crypto.randomUUID();

    const orderCreatedMessage = {
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
        items: [{ productId, quantity: 3 }],
      },
    };

    // Send the first message
    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Wait for first processing
    let reservation = null;
    let attempts = 0;
    while (!reservation && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 200));
      reservation = await repository.findReservationByOrderId(orderId);
      attempts++;
    }
    expect(reservation).not.toBeNull();
    const originalReservationId = reservation!.id;

    // Send duplicate event (same orderId, same payload)
    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify(orderCreatedMessage),
        },
      ],
    });

    // Wait a short time to allow duplicate message to be consumed
    await new Promise((resolve) => setTimeout(resolve, 1500));

    // Verify exactly one reservation row exists for orderId
    const resCount = await pool.query(
      'SELECT COUNT(*)::int as count FROM inventory_reservations WHERE order_id = $1;',
      [orderId],
    );
    expect(resCount.rows[0].count).toBe(1);

    // Verify reservation ID is unchanged
    const currentRes = await repository.findReservationByOrderId(orderId);
    expect(currentRes!.id).toBe(originalReservationId);

    // Verify exactly one item row exists
    const itemCount = await pool.query(
      'SELECT COUNT(*)::int as count FROM inventory_reservation_items WHERE reservation_id = $1;',
      [originalReservationId],
    );
    expect(itemCount.rows[0].count).toBe(1);
  }, 25000);

  it('safely skips unsupported event types without crashing the consumer', async () => {
    const unknownMessage = {
      eventId: crypto.randomUUID(),
      eventType: 'OrderShipped', // Unsupported in Phase 5
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'Order',
      aggregateId: crypto.randomUUID(),
      correlationId: crypto.randomUUID(),
      payload: { foo: 'bar' },
    };

    await testProducer.send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: 'some-key',
          value: JSON.stringify(unknownMessage),
        },
      ],
    });

    // Consumer should continue running smoothly
    await new Promise((resolve) => setTimeout(resolve, 1000));
    expect(consumer.isActive()).toBe(true);
  }, 15000);
});
