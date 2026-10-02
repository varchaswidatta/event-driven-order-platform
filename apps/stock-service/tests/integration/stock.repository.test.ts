import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { StockRepository } from '../../src/repositories/stock.repository.js';

describe('StockRepository PostgreSQL Integration Tests', () => {
  let pool: Pool;
  let repository: StockRepository;

  beforeAll(async () => {
    pool = createDatabasePool({ max: 5 });
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

  it('successfully reserves stock, decreasing available and increasing reserved quantities', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-001',
      name: 'Test Product 1',
      price: '25.00',
      availableQuantity: 20,
    });

    const orderId = crypto.randomUUID();
    const result = await repository.reserveStock({
      orderId,
      items: [{ productId: product.id, quantity: 5 }],
    });

    expect(result.success).toBe(true);
    expect(result.reservationId).toBeDefined();
    expect(result.alreadyExisted).toBe(false);

    // Verify stock table in database
    const updatedStock = await repository.getStock(product.id);
    expect(updatedStock).not.toBeNull();
    expect(updatedStock!.availableQuantity).toBe(15); // 20 - 5
    expect(updatedStock!.reservedQuantity).toBe(5);

    // Verify stock_reservations record
    const reservation = await repository.getReservationByOrderId(orderId);
    expect(reservation).not.toBeNull();
    expect(reservation!.orderId).toBe(orderId);
    expect(reservation!.status).toBe('RESERVED');
    expect(reservation!.items).toHaveLength(1);
    expect(reservation!.items![0]!.quantity).toBe(5);
  });

  it('fails and causes NO stock mutation when requested quantity exceeds available stock', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-002',
      name: 'Test Product 2',
      price: '10.00',
      availableQuantity: 3,
    });

    const orderId = crypto.randomUUID();
    const result = await repository.reserveStock({
      orderId,
      items: [{ productId: product.id, quantity: 10 }], // Exceeds available 3
    });

    expect(result.success).toBe(false);
    expect(result.failureReason).toBe('INSUFFICIENT_STOCK');

    // Verify stock was completely untouched
    const stockAfter = await repository.getStock(product.id);
    expect(stockAfter!.availableQuantity).toBe(3);
    expect(stockAfter!.reservedQuantity).toBe(0);

    // Verify no reservation record was created
    const reservation = await repository.getReservationByOrderId(orderId);
    expect(reservation).toBeNull();
  });

  it('guarantees all-or-nothing atomicity for multi-item orders (no partial reservation)', async () => {
    const { product: prodA } = await repository.createProductWithStock({
      sku: 'SKU-A',
      name: 'Product A',
      price: '50.00',
      availableQuantity: 10,
    });

    const { product: prodB } = await repository.createProductWithStock({
      sku: 'SKU-B',
      name: 'Product B',
      price: '30.00',
      availableQuantity: 1, // Insufficient for requested 2
    });

    const orderId = crypto.randomUUID();
    const result = await repository.reserveStock({
      orderId,
      items: [
        { productId: prodA.id, quantity: 5 }, // Sufficient
        { productId: prodB.id, quantity: 2 }, // Insufficient
      ],
    });

    expect(result.success).toBe(false);
    expect(result.failureReason).toBe('INSUFFICIENT_STOCK');

    // Crucial check: Product A MUST remain completely unreserved (no partial reservation)
    const stockA = await repository.getStock(prodA.id);
    expect(stockA!.availableQuantity).toBe(10);
    expect(stockA!.reservedQuantity).toBe(0);

    const stockB = await repository.getStock(prodB.id);
    expect(stockB!.availableQuantity).toBe(1);
    expect(stockB!.reservedQuantity).toBe(0);

    // No reservation created
    const reservation = await repository.getReservationByOrderId(orderId);
    expect(reservation).toBeNull();
  });

  it('returns PRODUCT_NOT_FOUND when requesting non-existent product without mutating any stock', async () => {
    const { product: existingProd } = await repository.createProductWithStock({
      sku: 'SKU-EXISTING',
      name: 'Existing Product',
      price: '15.00',
      availableQuantity: 10,
    });

    const nonExistentId = crypto.randomUUID();
    const orderId = crypto.randomUUID();

    const result = await repository.reserveStock({
      orderId,
      items: [
        { productId: existingProd.id, quantity: 2 },
        { productId: nonExistentId, quantity: 1 },
      ],
    });

    expect(result.success).toBe(false);
    expect(result.failureReason).toBe('PRODUCT_NOT_FOUND');

    // Existing product remains unreserved
    const stock = await repository.getStock(existingProd.id);
    expect(stock!.availableQuantity).toBe(10);
    expect(stock!.reservedQuantity).toBe(0);
  });

  it('handles duplicate orderId requests idempotently without double-reserving stock', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-IDEMPOTENT',
      name: 'Idempotent Product',
      price: '40.00',
      availableQuantity: 20,
    });

    const orderId = crypto.randomUUID();

    // First call: reserves stock
    const firstResult = await repository.reserveStock({
      orderId,
      items: [{ productId: product.id, quantity: 4 }],
    });
    expect(firstResult.success).toBe(true);
    expect(firstResult.alreadyExisted).toBe(false);

    const stockAfterFirst = await repository.getStock(product.id);
    expect(stockAfterFirst!.availableQuantity).toBe(16);
    expect(stockAfterFirst!.reservedQuantity).toBe(4);

    // Second call: duplicate delivery for same orderId
    const secondResult = await repository.reserveStock({
      orderId,
      items: [{ productId: product.id, quantity: 4 }],
    });
    expect(secondResult.success).toBe(true);
    expect(secondResult.alreadyExisted).toBe(true);
    expect(secondResult.reservationId).toBe(firstResult.reservationId);

    // Crucial: Stock MUST NOT be decremented again!
    const stockAfterSecond = await repository.getStock(product.id);
    expect(stockAfterSecond!.availableQuantity).toBe(16); // Still 16, NOT 12
    expect(stockAfterSecond!.reservedQuantity).toBe(4); // Still 4, NOT 8

    // Verify only 1 reservation row exists
    const resCount = await pool.query(
      'SELECT COUNT(*)::int as count FROM stock_reservations WHERE order_id = $1;',
      [orderId],
    );
    expect(resCount.rows[0].count).toBe(1);
  });
});
