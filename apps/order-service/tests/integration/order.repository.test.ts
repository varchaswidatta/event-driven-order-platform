import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { createDatabasePool } from '../../src/db/client.js';
import { OrderRepository } from '../../src/repositories/order.repository.js';
import { runMigrations } from '../../src/db/migrate.js';
import { ORDER_STATUS } from '../../src/domain/order-status.js';
import { randomUUID } from 'node:crypto';

describe('OrderRepository (PostgreSQL Integration Tests)', () => {
  let pool: Pool;
  let repository: OrderRepository;

  // Deterministic UUIDs for test isolation and readability
  const customerIdA = '00000000-0000-4000-a000-000000000001';
  const customerIdB = '00000000-0000-4000-a000-000000000002';
  const productId1 = '00000000-0000-4000-b000-000000000001';
  const productId2 = '00000000-0000-4000-b000-000000000002';
  const productId3 = '00000000-0000-4000-b000-000000000003';

  beforeAll(async () => {
    pool = createDatabasePool({
      connectionTimeoutMillis: 3000,
    });

    // Verify PostgreSQL is reachable — fail loudly, do not silently skip
    const client = await pool.connect();
    await client.query('SELECT 1;');
    client.release();

    // Run migrations
    await runMigrations(pool);
    repository = new OrderRepository(pool);
  });

  beforeEach(async () => {
    // Clean test data between tests for deterministic isolation
    await pool.query('DELETE FROM outbox_events;');
    await pool.query('DELETE FROM order_items;');
    await pool.query('DELETE FROM orders;');
  });

  afterAll(async () => {
    if (pool) {
      // Clean up test data
      await pool.query('DELETE FROM outbox_events;').catch(() => {});
      await pool.query('DELETE FROM order_items;').catch(() => {});
      await pool.query('DELETE FROM orders;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  // TEST 1: Create a valid order with multiple items
  it('creates a valid order with multiple items in a single transaction', async () => {
    const created = await repository.createOrder({
      customerId: customerIdA,
      status: ORDER_STATUS.PENDING,
      totalAmount: '55.97',
      currency: 'USD',
      items: [
        { productId: productId1, quantity: 2, unitPrice: '19.99' },
        { productId: productId2, quantity: 1, unitPrice: '15.99' },
      ],
    });

    expect(created.id).toBeDefined();
    expect(created.customerId).toBe(customerIdA);
    expect(created.status).toBe(ORDER_STATUS.PENDING);
    expect(created.totalAmount).toBe('55.97');
    expect(created.currency).toBe('USD');
    expect(created.items).toHaveLength(2);
    expect(created.createdAt).toBeInstanceOf(Date);
    expect(created.updatedAt).toBeInstanceOf(Date);

    // Verify items
    const item1 = created.items.find((i) => i.productId === productId1);
    const item2 = created.items.find((i) => i.productId === productId2);
    expect(item1).toBeDefined();
    expect(item1?.quantity).toBe(2);
    expect(item1?.unitPrice).toBe('19.99');
    expect(item2).toBeDefined();
    expect(item2?.quantity).toBe(1);
    expect(item2?.unitPrice).toBe('15.99');

    // Verify data actually persisted in PostgreSQL
    const dbCheck = await pool.query('SELECT COUNT(*) as cnt FROM orders WHERE id = $1', [
      created.id,
    ]);
    expect(dbCheck.rows[0]?.cnt).toBe('1');

    const itemCheck = await pool.query(
      'SELECT COUNT(*) as cnt FROM order_items WHERE order_id = $1',
      [created.id],
    );
    expect(itemCheck.rows[0]?.cnt).toBe('2');
  });

  // TEST 2: Retrieve a created order by ID with all items
  it('retrieves a created order by ID with all its items', async () => {
    const created = await repository.createOrder({
      customerId: customerIdA,
      status: ORDER_STATUS.PENDING,
      totalAmount: '30.00',
      currency: 'EUR',
      items: [
        { productId: productId1, quantity: 1, unitPrice: '10.00' },
        { productId: productId2, quantity: 2, unitPrice: '10.00' },
      ],
    });

    const retrieved = await repository.findOrderById(created.id);

    expect(retrieved).not.toBeNull();
    expect(retrieved!.id).toBe(created.id);
    expect(retrieved!.customerId).toBe(customerIdA);
    expect(retrieved!.status).toBe(ORDER_STATUS.PENDING);
    expect(retrieved!.totalAmount).toBe('30.00');
    expect(retrieved!.currency).toBe('EUR');
    expect(retrieved!.items).toHaveLength(2);
    expect(retrieved!.createdAt).toBeInstanceOf(Date);
    expect(retrieved!.updatedAt).toBeInstanceOf(Date);

    // Verify item details survived the round trip
    for (const item of retrieved!.items) {
      expect(item.orderId).toBe(created.id);
      expect(item.id).toBeDefined();
      expect(item.createdAt).toBeInstanceOf(Date);
    }
  });

  it('returns null for a non-existent order ID', async () => {
    const result = await repository.findOrderById(randomUUID());
    expect(result).toBeNull();
  });

  // TEST 3: Retrieve orders by customer ID with isolation
  it('retrieves orders by customer ID without returning other customers orders', async () => {
    // Create orders for customer A
    const orderA1 = await repository.createOrder({
      customerId: customerIdA,
      status: ORDER_STATUS.PENDING,
      totalAmount: '10.00',
      currency: 'USD',
      items: [{ productId: productId1, quantity: 1, unitPrice: '10.00' }],
    });

    const orderA2 = await repository.createOrder({
      customerId: customerIdA,
      status: ORDER_STATUS.PENDING,
      totalAmount: '20.00',
      currency: 'USD',
      items: [{ productId: productId2, quantity: 2, unitPrice: '10.00' }],
    });

    // Create order for customer B
    await repository.createOrder({
      customerId: customerIdB,
      status: ORDER_STATUS.PENDING,
      totalAmount: '5.00',
      currency: 'GBP',
      items: [{ productId: productId3, quantity: 1, unitPrice: '5.00' }],
    });

    // Fetch customer A's orders
    const customerAOrders = await repository.findOrdersByCustomerId(customerIdA);
    expect(customerAOrders).toHaveLength(2);

    const orderIds = customerAOrders.map((o) => o.id);
    expect(orderIds).toContain(orderA1.id);
    expect(orderIds).toContain(orderA2.id);

    // Verify items are included
    for (const order of customerAOrders) {
      expect(order.items.length).toBeGreaterThanOrEqual(1);
    }

    // Fetch customer B's orders — should only get their own
    const customerBOrders = await repository.findOrdersByCustomerId(customerIdB);
    expect(customerBOrders).toHaveLength(1);
    expect(customerBOrders[0]!.currency).toBe('GBP');

    // Unknown customer should get empty array
    const unknownOrders = await repository.findOrdersByCustomerId(randomUUID());
    expect(unknownOrders).toHaveLength(0);
  });

  // TEST 4: Transaction rollback — partial order must not persist
  it('rolls back the entire transaction when an item insertion fails', async () => {
    const orderId = randomUUID();

    // Attempt to create an order with an item that violates the CHECK (quantity > 0) constraint
    await expect(
      repository.createOrder({
        id: orderId,
        customerId: customerIdA,
        status: ORDER_STATUS.PENDING,
        totalAmount: '10.00',
        currency: 'USD',
        items: [
          { productId: productId1, quantity: 1, unitPrice: '10.00' },
          { productId: productId2, quantity: -1, unitPrice: '5.00' }, // violates CHECK constraint
        ],
      }),
    ).rejects.toThrow();

    // Verify the order was NOT left in the database (transaction rolled back)
    const orderCheck = await pool.query('SELECT COUNT(*) as cnt FROM orders WHERE id = $1', [
      orderId,
    ]);
    expect(orderCheck.rows[0]?.cnt).toBe('0');

    // Verify no orphaned order items exist
    const itemCheck = await pool.query(
      'SELECT COUNT(*) as cnt FROM order_items WHERE order_id = $1',
      [orderId],
    );
    expect(itemCheck.rows[0]?.cnt).toBe('0');
  });

  // TEST 5: Database-level constraint enforcement
  it('rejects orders that violate database constraints', async () => {
    // Invalid currency length (constraint: length(currency) = 3)
    await expect(
      repository.createOrder({
        customerId: customerIdA,
        status: ORDER_STATUS.PENDING,
        totalAmount: '10.00',
        currency: 'TOOLONG',
        items: [{ productId: productId1, quantity: 1, unitPrice: '10.00' }],
      }),
    ).rejects.toThrow();

    // Negative total_amount (constraint: total_amount >= 0)
    await expect(
      repository.createOrder({
        customerId: customerIdA,
        status: ORDER_STATUS.PENDING,
        totalAmount: '-1.00',
        currency: 'USD',
        items: [{ productId: productId1, quantity: 1, unitPrice: '10.00' }],
      }),
    ).rejects.toThrow();
  });

  // TEST 6: Money precision — verify exact decimal values survive PostgreSQL round trip
  it('preserves exact monetary precision through PostgreSQL NUMERIC(12,2)', async () => {
    const precisionItems = [
      { productId: productId1, quantity: 1, unitPrice: '10.10' },
      { productId: productId2, quantity: 1, unitPrice: '10.20' },
      { productId: productId3, quantity: 1, unitPrice: '0.01' },
    ];
    // Total = 10.10 + 10.20 + 0.01 = 20.31

    const created = await repository.createOrder({
      customerId: customerIdA,
      status: ORDER_STATUS.PENDING,
      totalAmount: '20.31',
      currency: 'INR',
      items: precisionItems,
    });

    expect(created.totalAmount).toBe('20.31');

    // Verify via retrieval
    const retrieved = await repository.findOrderById(created.id);
    expect(retrieved).not.toBeNull();
    expect(retrieved!.totalAmount).toBe('20.31');

    // Verify individual item prices survived the round trip exactly
    const item1 = retrieved!.items.find((i) => i.productId === productId1);
    const item2 = retrieved!.items.find((i) => i.productId === productId2);
    const item3 = retrieved!.items.find((i) => i.productId === productId3);
    expect(item1?.unitPrice).toBe('10.10');
    expect(item2?.unitPrice).toBe('10.20');
    expect(item3?.unitPrice).toBe('0.01');

    // Also test 99.99
    const highPriceOrder = await repository.createOrder({
      customerId: customerIdA,
      status: ORDER_STATUS.PENDING,
      totalAmount: '99.99',
      currency: 'USD',
      items: [{ productId: productId1, quantity: 1, unitPrice: '99.99' }],
    });

    const highRetrieved = await repository.findOrderById(highPriceOrder.id);
    expect(highRetrieved!.totalAmount).toBe('99.99');
    expect(highRetrieved!.items[0]?.unitPrice).toBe('99.99');
  });
});
