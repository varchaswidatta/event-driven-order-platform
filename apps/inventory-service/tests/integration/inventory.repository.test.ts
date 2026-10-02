import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import crypto from 'node:crypto';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { InventoryRepository } from '../../src/repositories/inventory.repository.js';
import { RESERVATION_STATUS } from '../../src/domain/reservation-status.js';

describe('InventoryRepository PostgreSQL Integration Tests', () => {
  let pool: Pool;
  let repository: InventoryRepository;

  beforeAll(async () => {
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);
    repository = new InventoryRepository(pool);
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM inventory_reservation_items;');
    await pool.query('DELETE FROM inventory_reservations;');
  });

  afterAll(async () => {
    if (pool) {
      await pool.query('DELETE FROM inventory_reservation_items;').catch(() => {});
      await pool.query('DELETE FROM inventory_reservations;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  it('creates an inventory reservation with status PENDING and creates reservation items', async () => {
    const orderId = crypto.randomUUID();
    const productId1 = crypto.randomUUID();
    const productId2 = crypto.randomUUID();

    const result = await repository.createReservation({
      orderId,
      items: [
        { productId: productId1, quantity: 4 },
        { productId: productId2, quantity: 1 },
      ],
    });

    expect(result.alreadyExisted).toBe(false);
    expect(result.reservation.id).toBeDefined();
    expect(result.reservation.orderId).toBe(orderId);
    expect(result.reservation.status).toBe(RESERVATION_STATUS.PENDING);
    expect(result.reservation.items).toHaveLength(2);
    expect(result.reservation.items[0]!.productId).toBe(productId1);
    expect(result.reservation.items[0]!.quantity).toBe(4);
    expect(result.reservation.items[1]!.productId).toBe(productId2);
    expect(result.reservation.items[1]!.quantity).toBe(1);

    // Verify row directly in inventory_reservations table
    const reservationRows = await pool.query(
      'SELECT id, order_id, status FROM inventory_reservations WHERE id = $1;',
      [result.reservation.id],
    );
    expect(reservationRows.rows.length).toBe(1);
    expect(reservationRows.rows[0].order_id).toBe(orderId);
    expect(reservationRows.rows[0].status).toBe('PENDING');

    // Verify rows in inventory_reservation_items table
    const itemRows = await pool.query(
      'SELECT id, reservation_id, product_id, quantity FROM inventory_reservation_items WHERE reservation_id = $1 ORDER BY created_at ASC;',
      [result.reservation.id],
    );
    expect(itemRows.rows.length).toBe(2);
    expect(itemRows.rows[0].quantity).toBe(4);
    expect(itemRows.rows[1].quantity).toBe(1);
  });

  it('atomically rolls back the reservation if item persistence fails', async () => {
    const orderId = crypto.randomUUID();

    // Passing invalid quantity (violates CHECK constraint quantity > 0)
    await expect(
      repository.createReservation({
        orderId,
        items: [
          { productId: crypto.randomUUID(), quantity: 5 },
          { productId: crypto.randomUUID(), quantity: -2 }, // Violates check constraint
        ],
      }),
    ).rejects.toThrow();

    // Verify no orphaned reservation was committed
    const reservationRows = await pool.query(
      'SELECT * FROM inventory_reservations WHERE order_id = $1;',
      [orderId],
    );
    expect(reservationRows.rows.length).toBe(0);

    const itemRows = await pool.query('SELECT * FROM inventory_reservation_items;');
    expect(itemRows.rows.length).toBe(0);
  });

  it('handles duplicate OrderCreated delivery idempotently without creating duplicate rows', async () => {
    const orderId = crypto.randomUUID();
    const productId = crypto.randomUUID();

    // First call: creates reservation
    const firstResult = await repository.createReservation({
      orderId,
      items: [{ productId, quantity: 3 }],
    });
    expect(firstResult.alreadyExisted).toBe(false);

    // Second call: duplicate event with same orderId
    const secondResult = await repository.createReservation({
      orderId,
      items: [{ productId, quantity: 3 }],
    });
    expect(secondResult.alreadyExisted).toBe(true);
    expect(secondResult.reservation.id).toBe(firstResult.reservation.id);
    expect(secondResult.reservation.orderId).toBe(orderId);
    expect(secondResult.reservation.items).toHaveLength(1);
    expect(secondResult.reservation.items[0]!.quantity).toBe(3);

    // Verify only 1 reservation row exists in database
    const totalReservations = await pool.query(
      'SELECT COUNT(*)::int as count FROM inventory_reservations WHERE order_id = $1;',
      [orderId],
    );
    expect(totalReservations.rows[0].count).toBe(1);

    // Verify only 1 reservation item row exists in database
    const totalItems = await pool.query(
      'SELECT COUNT(*)::int as count FROM inventory_reservation_items WHERE reservation_id = $1;',
      [firstResult.reservation.id],
    );
    expect(totalItems.rows[0].count).toBe(1);
  });

  it('can query reservation by order_id and by primary key id', async () => {
    const orderId = crypto.randomUUID();
    const productId = crypto.randomUUID();

    const created = await repository.createReservation({
      orderId,
      items: [{ productId, quantity: 7 }],
    });

    const byOrderId = await repository.findReservationByOrderId(orderId);
    expect(byOrderId).not.toBeNull();
    expect(byOrderId!.id).toBe(created.reservation.id);
    expect(byOrderId!.items).toHaveLength(1);
    expect(byOrderId!.items[0]!.quantity).toBe(7);

    const byId = await repository.findReservationById(created.reservation.id);
    expect(byId).not.toBeNull();
    expect(byId!.orderId).toBe(orderId);
    expect(byId!.items).toHaveLength(1);

    const nonExistent = await repository.findReservationByOrderId(crypto.randomUUID());
    expect(nonExistent).toBeNull();
  });
});
