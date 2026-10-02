import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { OrderRepository } from '../../src/repositories/order.repository.js';
import {
  OutboxRepository,
  IOutboxRepository,
} from '../../src/repositories/outbox.repository.js';
import { OrderService } from '../../src/services/order.service.js';
import { ORDER_STATUS } from '../../src/domain/order-status.js';
import { DatabaseOperationError } from '../../src/errors/order.errors.js';

describe('Transactional Outbox Integration Tests (PostgreSQL)', () => {
  let pool: Pool;
  let outboxRepository: OutboxRepository;
  let orderRepository: OrderRepository;
  let orderService: OrderService;

  const testCustomerId = '11111111-1111-4111-a111-111111111111';
  const testProductId1 = '22222222-2222-4222-a222-222222222222';
  const testProductId2 = '33333333-3333-4333-a333-333333333333';
  const testProductId3 = '44444444-4444-4444-a444-444444444444';

  const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

  beforeAll(async () => {
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);
    outboxRepository = new OutboxRepository(pool);
    orderRepository = new OrderRepository(pool, outboxRepository);
    orderService = new OrderService(orderRepository);
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM outbox_events;');
    await pool.query('DELETE FROM order_items;');
    await pool.query('DELETE FROM orders;');
  });

  afterAll(async () => {
    if (pool) {
      await pool.query('DELETE FROM outbox_events;').catch(() => {});
      await pool.query('DELETE FROM order_items;').catch(() => {});
      await pool.query('DELETE FROM orders;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  describe('OutboxRepository Direct Operations', () => {
    it('inserts an outbox event with defaults and retrieves it by ID', async () => {
      const aggregateId = crypto.randomUUID();
      const correlationId = crypto.randomUUID();

      const inserted = await outboxRepository.insertEvent({
        aggregateType: 'Order',
        aggregateId,
        eventType: 'OrderCreated',
        eventVersion: 1,
        correlationId,
        payload: {
          orderId: aggregateId,
          customerId: testCustomerId,
          items: [{ productId: testProductId1, quantity: 2 }],
        },
      });

      expect(inserted.id).toMatch(UUID_REGEX);
      expect(inserted.aggregateType).toBe('Order');
      expect(inserted.aggregateId).toBe(aggregateId);
      expect(inserted.eventType).toBe('OrderCreated');
      expect(inserted.eventVersion).toBe(1);
      expect(inserted.correlationId).toBe(correlationId);
      expect(inserted.publishedAt).toBeNull();
      expect(inserted.retryCount).toBe(0);
      expect(inserted.createdAt).toBeInstanceOf(Date);
      expect(inserted.payload).toEqual({
        orderId: aggregateId,
        customerId: testCustomerId,
        items: [{ productId: testProductId1, quantity: 2 }],
      });

      // Verify retrieval by ID
      const fetched = await outboxRepository.findById(inserted.id);
      expect(fetched).not.toBeNull();
      expect(fetched?.id).toBe(inserted.id);
      expect(fetched?.aggregateId).toBe(aggregateId);
      expect(fetched?.publishedAt).toBeNull();
    });

    it('retrieves unpublished events ordered by created_at (FIFO)', async () => {
      const id1 = crypto.randomUUID();
      const id2 = crypto.randomUUID();

      await outboxRepository.insertEvent({
        aggregateType: 'Order',
        aggregateId: id1,
        eventType: 'OrderCreated',
        payload: { orderId: id1, customerId: testCustomerId, items: [] },
      });

      // Small delay to ensure sequential timestamps
      await new Promise((resolve) => setTimeout(resolve, 20));

      await outboxRepository.insertEvent({
        aggregateType: 'Order',
        aggregateId: id2,
        eventType: 'OrderCreated',
        payload: { orderId: id2, customerId: testCustomerId, items: [] },
      });

      const unpublished = await outboxRepository.findUnpublishedEvents(10);
      expect(unpublished.length).toBe(2);
      expect(unpublished[0].aggregateId).toBe(id1);
      expect(unpublished[1].aggregateId).toBe(id2);
      expect(unpublished[0].createdAt.getTime()).toBeLessThanOrEqual(
        unpublished[1].createdAt.getTime(),
      );
    });

    it('marks an event as published and excludes it from unpublished queries', async () => {
      const aggregateId = crypto.randomUUID();
      const inserted = await outboxRepository.insertEvent({
        aggregateType: 'Order',
        aggregateId,
        eventType: 'OrderCreated',
        payload: { orderId: aggregateId, customerId: testCustomerId, items: [] },
      });

      let unpublished = await outboxRepository.findUnpublishedEvents();
      expect(unpublished.some((e) => e.id === inserted.id)).toBe(true);

      const publishTime = new Date();
      await outboxRepository.markAsPublished(inserted.id, publishTime);

      // Verify event is no longer in unpublished set
      unpublished = await outboxRepository.findUnpublishedEvents();
      expect(unpublished.some((e) => e.id === inserted.id)).toBe(false);

      // Verify published_at is set in database
      const fetched = await outboxRepository.findById(inserted.id);
      expect(fetched?.publishedAt).not.toBeNull();
    });

    it('increments retry_count monotonically', async () => {
      const aggregateId = crypto.randomUUID();
      const inserted = await outboxRepository.insertEvent({
        aggregateType: 'Order',
        aggregateId,
        eventType: 'OrderCreated',
        payload: { orderId: aggregateId, customerId: testCustomerId, items: [] },
      });

      expect(inserted.retryCount).toBe(0);

      await outboxRepository.incrementRetryCount(inserted.id);
      let updated = await outboxRepository.findById(inserted.id);
      expect(updated?.retryCount).toBe(1);

      await outboxRepository.incrementRetryCount(inserted.id);
      updated = await outboxRepository.findById(inserted.id);
      expect(updated?.retryCount).toBe(2);
    });

    it('finds events by aggregate ID', async () => {
      const aggregateId = crypto.randomUUID();
      await outboxRepository.insertEvent({
        aggregateType: 'Order',
        aggregateId,
        eventType: 'OrderCreated',
        payload: { orderId: aggregateId, customerId: testCustomerId, items: [] },
      });

      const events = await outboxRepository.findByAggregateId(aggregateId);
      expect(events.length).toBe(1);
      expect(events[0].aggregateId).toBe(aggregateId);
    });
  });

  describe('Transactional Order Creation with Outbox Event', () => {
    it('creates an order, items, and outbox event in ONE database transaction', async () => {
      const createdOrder = await orderService.createOrder({
        customerId: testCustomerId,
        currency: 'USD',
        items: [
          { productId: testProductId1, quantity: 2, unitPrice: '25.00' },
          { productId: testProductId2, quantity: 1, unitPrice: '15.50' },
        ],
      });

      expect(createdOrder.id).toMatch(UUID_REGEX);
      expect(createdOrder.status).toBe(ORDER_STATUS.PENDING);
      expect(createdOrder.totalAmount).toBe('65.50');
      expect(createdOrder.items.length).toBe(2);

      // 1. Direct PostgreSQL verification: orders table
      const orderDbResult = await pool.query('SELECT * FROM orders WHERE id = $1;', [
        createdOrder.id,
      ]);
      expect(orderDbResult.rows.length).toBe(1);
      const orderRow = orderDbResult.rows[0];
      expect(orderRow.customer_id).toBe(testCustomerId);
      expect(orderRow.status).toBe(ORDER_STATUS.PENDING);
      expect(String(orderRow.total_amount)).toBe('65.50');
      expect(orderRow.currency).toBe('USD');

      // 2. Direct PostgreSQL verification: order_items table
      const itemsDbResult = await pool.query(
        'SELECT * FROM order_items WHERE order_id = $1 ORDER BY created_at ASC;',
        [createdOrder.id],
      );
      expect(itemsDbResult.rows.length).toBe(2);
      expect(itemsDbResult.rows[0].product_id).toBe(testProductId1);
      expect(itemsDbResult.rows[0].quantity).toBe(2);
      expect(String(itemsDbResult.rows[0].unit_price)).toBe('25.00');
      expect(itemsDbResult.rows[1].product_id).toBe(testProductId2);
      expect(itemsDbResult.rows[1].quantity).toBe(1);
      expect(String(itemsDbResult.rows[1].unit_price)).toBe('15.50');

      // 3. Direct PostgreSQL verification: outbox_events table
      const outboxDbResult = await pool.query(
        'SELECT * FROM outbox_events WHERE aggregate_id = $1;',
        [createdOrder.id],
      );
      expect(outboxDbResult.rows.length).toBe(1);
      const outboxRow = outboxDbResult.rows[0];

      expect(outboxRow.id).toMatch(UUID_REGEX);
      expect(outboxRow.aggregate_type).toBe('Order');
      expect(outboxRow.aggregate_id).toBe(createdOrder.id);
      expect(outboxRow.event_type).toBe('OrderCreated');
      expect(outboxRow.event_version).toBe(1);
      expect(outboxRow.correlation_id).toMatch(UUID_REGEX);
      expect(outboxRow.published_at).toBeNull();
      expect(outboxRow.retry_count).toBe(0);
      expect(outboxRow.created_at).toBeInstanceOf(Date);

      // 4. Verify Payload structure conforms to Inventory Service specifications
      const payload =
        typeof outboxRow.payload === 'string' ? JSON.parse(outboxRow.payload) : outboxRow.payload;

      expect(payload).toEqual({
        orderId: createdOrder.id,
        customerId: testCustomerId,
        items: [
          { productId: testProductId1, quantity: 2 },
          { productId: testProductId2, quantity: 1 },
        ],
      });

      // Crucial Check: payload MUST NOT contain unitPrice or totalAmount (safe money boundary)
      expect(payload).not.toHaveProperty('totalAmount');
      expect(payload).not.toHaveProperty('currency');
      expect(payload.items[0]).not.toHaveProperty('unitPrice');
      expect(payload.items[1]).not.toHaveProperty('unitPrice');
    });

    it('correctly maps multiple items in outbox payload', async () => {
      const createdOrder = await orderService.createOrder({
        customerId: testCustomerId,
        currency: 'USD',
        items: [
          { productId: testProductId1, quantity: 5, unitPrice: '10.00' },
          { productId: testProductId2, quantity: 3, unitPrice: '20.00' },
          { productId: testProductId3, quantity: 1, unitPrice: '30.00' },
        ],
      });

      expect(createdOrder.items.length).toBe(3);

      const outboxResult = await pool.query(
        'SELECT payload FROM outbox_events WHERE aggregate_id = $1;',
        [createdOrder.id],
      );
      expect(outboxResult.rows.length).toBe(1);

      const payload =
        typeof outboxResult.rows[0].payload === 'string'
          ? JSON.parse(outboxResult.rows[0].payload)
          : outboxResult.rows[0].payload;

      expect(payload.items).toEqual([
        { productId: testProductId1, quantity: 5 },
        { productId: testProductId2, quantity: 3 },
        { productId: testProductId3, quantity: 1 },
      ]);
    });
  });

  describe('Atomic Rollback Guarantee (PostgreSQL ACID)', () => {
    it('rolls back orders, order_items, AND outbox_events when outbox insertion fails', async () => {
      // Create a failing outbox repository that throws during insertEvent
      const failingOutboxRepo: IOutboxRepository = {
        insertEvent: async () => {
          throw new Error('Simulated outbox database failure before transaction commit');
        },
        findUnpublishedEvents: async () => [],
        markAsPublished: async () => {},
        incrementRetryCount: async () => {},
        findById: async () => null,
        findByAggregateId: async () => [],
      };

      const failingOrderRepo = new OrderRepository(pool, failingOutboxRepo);
      const targetOrderId = crypto.randomUUID();

      // Attempt to create order — should throw DatabaseOperationError
      await expect(
        failingOrderRepo.createOrder({
          id: targetOrderId,
          customerId: testCustomerId,
          status: ORDER_STATUS.PENDING,
          totalAmount: '50.00',
          currency: 'USD',
          items: [{ productId: testProductId1, quantity: 2, unitPrice: '25.00' }],
        }),
      ).rejects.toThrow(DatabaseOperationError);

      // Verify actual transactional atomicity in PostgreSQL:
      // Neither order, order_items, nor outbox_events must exist!
      const ordersInDb = await pool.query('SELECT * FROM orders WHERE id = $1;', [targetOrderId]);
      expect(ordersInDb.rows.length).toBe(0);

      const itemsInDb = await pool.query('SELECT * FROM order_items WHERE order_id = $1;', [
        targetOrderId,
      ]);
      expect(itemsInDb.rows.length).toBe(0);

      const outboxInDb = await pool.query('SELECT * FROM outbox_events WHERE aggregate_id = $1;', [
        targetOrderId,
      ]);
      expect(outboxInDb.rows.length).toBe(0);
    });

    it('rolls back orders and outbox_events when order item insertion fails', async () => {
      const targetOrderId = crypto.randomUUID();

      // Trigger a failure during item insertion (e.g., negative quantity violating CHECK constraint)
      await expect(
        orderRepository.createOrder({
          id: targetOrderId,
          customerId: testCustomerId,
          status: ORDER_STATUS.PENDING,
          totalAmount: '25.00',
          currency: 'USD',
          items: [
            { productId: testProductId1, quantity: 1, unitPrice: '25.00' },
            // Second item violates CHECK (quantity > 0)
            { productId: testProductId2, quantity: -1, unitPrice: '25.00' },
          ],
        }),
      ).rejects.toThrow();

      // Verify PostgreSQL atomicity
      const ordersInDb = await pool.query('SELECT * FROM orders WHERE id = $1;', [targetOrderId]);
      expect(ordersInDb.rows.length).toBe(0);

      const itemsInDb = await pool.query('SELECT * FROM order_items WHERE order_id = $1;', [
        targetOrderId,
      ]);
      expect(itemsInDb.rows.length).toBe(0);

      const outboxInDb = await pool.query('SELECT * FROM outbox_events WHERE aggregate_id = $1;', [
        targetOrderId,
      ]);
      expect(outboxInDb.rows.length).toBe(0);
    });
  });
});
