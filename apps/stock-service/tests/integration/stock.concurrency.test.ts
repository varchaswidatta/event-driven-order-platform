import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { StockRepository } from '../../src/repositories/stock.repository.js';

describe('Stock Concurrency Integration Tests', () => {
  let pool: Pool;
  let repository: StockRepository;

  beforeAll(async () => {
    pool = createDatabasePool({ max: 20 });
    await runMigrations(pool);
    repository = new StockRepository(pool);
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM stock_reservation_items;');
    await pool.query('DELETE FROM stock_reservations;');
    await pool.query('DELETE FROM stock;');
    await pool.query('DELETE FROM products;');
  });

  afterAll(async () => {
    if (pool) {
      await pool.query('DELETE FROM stock_reservation_items;').catch(() => {});
      await pool.query('DELETE FROM stock_reservations;').catch(() => {});
      await pool.query('DELETE FROM stock;').catch(() => {});
      await pool.query('DELETE FROM products;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  it('prevents overselling when two concurrent requests compete for the same stock (available = 5, both request 4)', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-CONC-1',
      name: 'Concurrent Product 1',
      price: '19.99',
      availableQuantity: 5,
    });

    const orderIdA = crypto.randomUUID();
    const orderIdB = crypto.randomUUID();

    // Fire both requests concurrently
    const [resultA, resultB] = await Promise.all([
      repository.reserveStock({
        orderId: orderIdA,
        items: [{ productId: product.id, quantity: 4 }],
      }),
      repository.reserveStock({
        orderId: orderIdB,
        items: [{ productId: product.id, quantity: 4 }],
      }),
    ]);

    // Exactly one must succeed, and one must fail
    const succeeded = [resultA, resultB].filter((r) => r.success);
    const failed = [resultA, resultB].filter((r) => !r.success);

    expect(succeeded).toHaveLength(1);
    expect(failed).toHaveLength(1);

    expect(failed[0]!.failureReason).toBe('INSUFFICIENT_STOCK');

    // Verify database state: available = 1, reserved = 4 (never negative)
    const stock = await repository.getStock(product.id);
    expect(stock).not.toBeNull();
    expect(stock!.availableQuantity).toBe(1);
    expect(stock!.reservedQuantity).toBe(4);

    // Verify exactly one reservation record in database
    const resCount = await pool.query(
      'SELECT COUNT(*)::int as count FROM stock_reservations WHERE status = $1;',
      ['RESERVED'],
    );
    expect(resCount.rows[0].count).toBe(1);
  });

  it('handles high concurrency: 10 concurrent requests of quantity 1 competing for 5 available items', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-CONC-2',
      name: 'Concurrent Product 2',
      price: '9.99',
      availableQuantity: 5,
    });

    const requests = Array.from({ length: 10 }, () => ({
      orderId: crypto.randomUUID(),
      items: [{ productId: product.id, quantity: 1 }],
    }));

    // Fire 10 concurrent requests
    const results = await Promise.all(requests.map((req) => repository.reserveStock(req)));

    const successful = results.filter((r) => r.success);
    const failed = results.filter((r) => !r.success);

    // Exactly 5 should succeed and 5 should fail with INSUFFICIENT_STOCK
    expect(successful).toHaveLength(5);
    expect(failed).toHaveLength(5);
    for (const f of failed) {
      expect(f.failureReason).toBe('INSUFFICIENT_STOCK');
    }

    // Verify stock table: available = 0, reserved = 5
    const stock = await repository.getStock(product.id);
    expect(stock!.availableQuantity).toBe(0);
    expect(stock!.reservedQuantity).toBe(5);

    // Verify 5 reservation records created
    const resCount = await pool.query(
      'SELECT COUNT(*)::int as count FROM stock_reservations WHERE status = $1;',
      ['RESERVED'],
    );
    expect(resCount.rows[0].count).toBe(5);
  });

  it('Scenario A: concurrent transactions requesting products in opposite order [A, B] vs [B, A] do not deadlock and both succeed', async () => {
    const { product: prodA } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-A1',
      name: 'Scenario A - Product 1',
      price: '10.00',
      availableQuantity: 10,
    });

    const { product: prodB } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-A2',
      name: 'Scenario A - Product 2',
      price: '20.00',
      availableQuantity: 10,
    });

    const orderId1 = crypto.randomUUID();
    const orderId2 = crypto.randomUUID();

    // Transaction 1 requests [A, B]
    const req1 = repository.reserveStock({
      orderId: orderId1,
      items: [
        { productId: prodA.id, quantity: 2 },
        { productId: prodB.id, quantity: 3 },
      ],
    });

    // Transaction 2 requests [B, A] (inverted input order)
    const req2 = repository.reserveStock({
      orderId: orderId2,
      items: [
        { productId: prodB.id, quantity: 4 },
        { productId: prodA.id, quantity: 5 },
      ],
    });

    // Fire both simultaneously - must not deadlock
    const [res1, res2] = await Promise.all([req1, req2]);

    expect(res1.success).toBe(true);
    expect(res2.success).toBe(true);

    // Verify stock correctness
    const stockA = await repository.getStock(prodA.id);
    expect(stockA!.availableQuantity).toBe(3); // 10 - 2 - 5 = 3
    expect(stockA!.reservedQuantity).toBe(7);

    const stockB = await repository.getStock(prodB.id);
    expect(stockB!.availableQuantity).toBe(3); // 10 - 3 - 4 = 3
    expect(stockB!.reservedQuantity).toBe(7);
  });

  it('Scenario B: concurrent cyclic transactions [A, B], [B, C], [C, A] do not deadlock and all succeed', async () => {
    const { product: prodA } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-B1',
      name: 'Scenario B - Product A',
      price: '10.00',
      availableQuantity: 10,
    });

    const { product: prodB } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-B2',
      name: 'Scenario B - Product B',
      price: '20.00',
      availableQuantity: 10,
    });

    const { product: prodC } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-B3',
      name: 'Scenario B - Product C',
      price: '30.00',
      availableQuantity: 10,
    });

    // Tx 1: [A, B]
    const req1 = repository.reserveStock({
      orderId: crypto.randomUUID(),
      items: [
        { productId: prodA.id, quantity: 2 },
        { productId: prodB.id, quantity: 2 },
      ],
    });

    // Tx 2: [B, C]
    const req2 = repository.reserveStock({
      orderId: crypto.randomUUID(),
      items: [
        { productId: prodB.id, quantity: 2 },
        { productId: prodC.id, quantity: 2 },
      ],
    });

    // Tx 3: [C, A]
    const req3 = repository.reserveStock({
      orderId: crypto.randomUUID(),
      items: [
        { productId: prodC.id, quantity: 2 },
        { productId: prodA.id, quantity: 2 },
      ],
    });

    // Fire all three simultaneously - classic circular wait pattern without sorted locks
    const [res1, res2, res3] = await Promise.all([req1, req2, req3]);

    expect(res1.success).toBe(true);
    expect(res2.success).toBe(true);
    expect(res3.success).toBe(true);

    const stockA = await repository.getStock(prodA.id);
    expect(stockA!.availableQuantity).toBe(6); // 10 - 2 - 2
    expect(stockA!.reservedQuantity).toBe(4);

    const stockB = await repository.getStock(prodB.id);
    expect(stockB!.availableQuantity).toBe(6); // 10 - 2 - 2
    expect(stockB!.reservedQuantity).toBe(4);

    const stockC = await repository.getStock(prodC.id);
    expect(stockC!.availableQuantity).toBe(6); // 10 - 2 - 2
    expect(stockC!.reservedQuantity).toBe(4);
  });

  it('Scenario C: multiple concurrent reservations against overlapping product sets with scarce stock preserve correctness', async () => {
    const { product: prodA } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-C1',
      name: 'Scenario C - Product A',
      price: '15.00',
      availableQuantity: 6,
    });

    const { product: prodB } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-C2',
      name: 'Scenario C - Product B',
      price: '25.00',
      availableQuantity: 6,
    });

    const { product: prodC } = await repository.createProductWithStock({
      sku: 'SKU-SCENARIO-C3',
      name: 'Scenario C - Product C',
      price: '35.00',
      availableQuantity: 6,
    });

    // 12 concurrent requests with overlapping product pairs
    const overlappingRequests = [
      {
        items: [
          { productId: prodA.id, quantity: 2 },
          { productId: prodB.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodB.id, quantity: 2 },
          { productId: prodC.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodC.id, quantity: 2 },
          { productId: prodA.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodB.id, quantity: 2 },
          { productId: prodA.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodC.id, quantity: 2 },
          { productId: prodB.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodA.id, quantity: 2 },
          { productId: prodC.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodA.id, quantity: 3 },
          { productId: prodB.id, quantity: 1 },
        ],
      },
      {
        items: [
          { productId: prodB.id, quantity: 3 },
          { productId: prodC.id, quantity: 1 },
        ],
      },
      {
        items: [
          { productId: prodC.id, quantity: 3 },
          { productId: prodA.id, quantity: 1 },
        ],
      },
      {
        items: [
          { productId: prodA.id, quantity: 2 },
          { productId: prodB.id, quantity: 2 },
          { productId: prodC.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodC.id, quantity: 2 },
          { productId: prodB.id, quantity: 2 },
          { productId: prodA.id, quantity: 2 },
        ],
      },
      {
        items: [
          { productId: prodB.id, quantity: 1 },
          { productId: prodC.id, quantity: 2 },
        ],
      },
    ].map((r) => ({ orderId: crypto.randomUUID(), ...r }));

    const results = await Promise.all(
      overlappingRequests.map((req) => repository.reserveStock(req)),
    );

    // No deadlock errors thrown
    expect(results).toHaveLength(12);

    const stockA = await repository.getStock(prodA.id);
    const stockB = await repository.getStock(prodB.id);
    const stockC = await repository.getStock(prodC.id);

    // Invariants:
    // 1. Available quantities must never be negative
    expect(stockA!.availableQuantity).toBeGreaterThanOrEqual(0);
    expect(stockB!.availableQuantity).toBeGreaterThanOrEqual(0);
    expect(stockC!.availableQuantity).toBeGreaterThanOrEqual(0);

    // 2. Sum of available + reserved must strictly equal initial available (6)
    expect(stockA!.availableQuantity + stockA!.reservedQuantity).toBe(6);
    expect(stockB!.availableQuantity + stockB!.reservedQuantity).toBe(6);
    expect(stockC!.availableQuantity + stockC!.reservedQuantity).toBe(6);
  });
});
