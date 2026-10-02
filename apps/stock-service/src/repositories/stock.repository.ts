import { Pool } from 'pg';
import {
  Product,
  Stock,
  StockReservation,
  StockReservationItem,
  ReserveStockInput,
  ReserveStockResult,
} from '../domain/stock.js';
import { DatabaseOperationError } from '../errors/stock.errors.js';

export interface IStockRepository {
  reserveStock(input: ReserveStockInput): Promise<ReserveStockResult>;
  getStock(productId: string): Promise<Stock | null>;
  getProduct(productId: string): Promise<Product | null>;
  getReservationByOrderId(orderId: string): Promise<StockReservation | null>;
  createProductWithStock(data: {
    id?: string;
    sku: string;
    name: string;
    price: string;
    availableQuantity: number;
  }): Promise<{ product: Product; stock: Stock }>;
}

export class StockRepository implements IStockRepository {
  constructor(private readonly pool: Pool) {}

  /**
   * Atomically reserves stock for all requested items under row-level locking (SELECT ... FOR UPDATE).
   * Guarantees:
   * 1. Idempotency: Checks for an existing reservation for orderId first.
   * 2. All-or-nothing atomicity: If ANY requested item is insufficient or missing, rolls back completely.
   * 3. Concurrency safety: Product rows are locked in deterministic order (sorted by productId) to prevent deadlocks.
   */
  async reserveStock(input: ReserveStockInput): Promise<ReserveStockResult> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // 1. Idempotency guard: check if stock has already been reserved for this order
      const existingRes = await client.query<{ id: string }>(
        `SELECT id FROM stock_reservations WHERE order_id = $1 FOR UPDATE;`,
        [input.orderId],
      );

      if (existingRes.rows.length > 0) {
        await client.query('COMMIT');
        return {
          success: true,
          reservationId: existingRes.rows[0]!.id,
          alreadyExisted: true,
        };
      }

      // 2. Sort items by productId to enforce deterministic locking order and prevent deadlocks
      const sortedItems = [...input.items].sort((a, b) => a.productId.localeCompare(b.productId));
      const productIds = sortedItems.map((i) => i.productId);

      // 3. Acquire row-level locks on all requested stock records
      const stockRes = await client.query<{
        product_id: string;
        available_quantity: number;
        reserved_quantity: number;
      }>(
        `SELECT product_id, available_quantity, reserved_quantity
         FROM stock
         WHERE product_id = ANY($1::uuid[])
         ORDER BY product_id
         FOR UPDATE;`,
        [productIds],
      );

      const stockMap = new Map<string, { available: number; reserved: number }>();
      for (const row of stockRes.rows) {
        stockMap.set(row.product_id, {
          available: Number(row.available_quantity),
          reserved: Number(row.reserved_quantity),
        });
      }

      // 4. Verify all products exist in stock table
      for (const item of sortedItems) {
        if (!stockMap.has(item.productId)) {
          await client.query('ROLLBACK');
          return {
            success: false,
            failureReason: 'PRODUCT_NOT_FOUND',
          };
        }
      }

      // 5. Verify availability for all items (All-or-nothing check)
      for (const item of sortedItems) {
        const stockRecord = stockMap.get(item.productId)!;
        if (stockRecord.available < item.quantity) {
          await client.query('ROLLBACK');
          return {
            success: false,
            failureReason: 'INSUFFICIENT_STOCK',
          };
        }
      }

      // 6. Update available and reserved quantities
      for (const item of sortedItems) {
        await client.query(
          `UPDATE stock
           SET available_quantity = available_quantity - $1,
               reserved_quantity = reserved_quantity + $1,
               updated_at = NOW()
           WHERE product_id = $2;`,
          [item.quantity, item.productId],
        );
      }

      // 7. Insert reservation record
      const resResult = await client.query<{ id: string }>(
        `INSERT INTO stock_reservations (id, order_id, status, created_at, updated_at)
         VALUES (gen_random_uuid(), $1, 'RESERVED', NOW(), NOW())
         RETURNING id;`,
        [input.orderId],
      );
      const reservationId = resResult.rows[0]!.id;

      // 8. Insert reservation items
      for (const item of sortedItems) {
        await client.query(
          `INSERT INTO stock_reservation_items (id, reservation_id, product_id, quantity, created_at)
           VALUES (gen_random_uuid(), $1, $2, $3, NOW());`,
          [reservationId, item.productId, item.quantity],
        );
      }

      await client.query('COMMIT');

      return {
        success: true,
        reservationId,
        alreadyExisted: false,
      };
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      const message = err instanceof Error ? err.message : 'Unknown database error';
      throw new DatabaseOperationError(`Stock reservation failed: ${message}`);
    } finally {
      client.release();
    }
  }

  async getStock(productId: string): Promise<Stock | null> {
    try {
      const res = await this.pool.query<{
        id: string;
        product_id: string;
        available_quantity: number;
        reserved_quantity: number;
        updated_at: Date;
      }>(
        `SELECT id, product_id, available_quantity, reserved_quantity, updated_at
         FROM stock
         WHERE product_id = $1;`,
        [productId],
      );

      if (res.rows.length === 0) return null;
      const row = res.rows[0]!;
      return {
        id: row.id,
        productId: row.product_id,
        availableQuantity: Number(row.available_quantity),
        reservedQuantity: Number(row.reserved_quantity),
        updatedAt: row.updated_at,
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch stock: ${message}`);
    }
  }

  async getProduct(productId: string): Promise<Product | null> {
    try {
      const res = await this.pool.query<{
        id: string;
        sku: string;
        name: string;
        price: string;
        created_at: Date;
        updated_at: Date;
      }>(
        `SELECT id, sku, name, price, created_at, updated_at
         FROM products
         WHERE id = $1;`,
        [productId],
      );

      if (res.rows.length === 0) return null;
      const row = res.rows[0]!;
      return {
        id: row.id,
        sku: row.sku,
        name: row.name,
        price: row.price,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch product: ${message}`);
    }
  }

  async getReservationByOrderId(orderId: string): Promise<StockReservation | null> {
    try {
      const res = await this.pool.query<{
        id: string;
        order_id: string;
        status: string;
        created_at: Date;
        updated_at: Date;
      }>(
        `SELECT id, order_id, status, created_at, updated_at
         FROM stock_reservations
         WHERE order_id = $1;`,
        [orderId],
      );

      if (res.rows.length === 0) return null;
      const row = res.rows[0]!;

      const itemsRes = await this.pool.query<{
        id: string;
        reservation_id: string;
        product_id: string;
        quantity: number;
        created_at: Date;
      }>(
        `SELECT id, reservation_id, product_id, quantity, created_at
         FROM stock_reservation_items
         WHERE reservation_id = $1
         ORDER BY created_at ASC;`,
        [row.id],
      );

      const items: StockReservationItem[] = itemsRes.rows.map((r) => ({
        id: r.id,
        reservationId: r.reservation_id,
        productId: r.product_id,
        quantity: Number(r.quantity),
        createdAt: r.created_at,
      }));

      return {
        id: row.id,
        orderId: row.order_id,
        status: row.status as 'RESERVED' | 'RELEASED',
        items,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch stock reservation: ${message}`);
    }
  }

  async createProductWithStock(data: {
    id?: string;
    sku: string;
    name: string;
    price: string;
    availableQuantity: number;
  }): Promise<{ product: Product; stock: Stock }> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const prodRes = await client.query<{
        id: string;
        sku: string;
        name: string;
        price: string;
        created_at: Date;
        updated_at: Date;
      }>(
        `INSERT INTO products (id, sku, name, price, created_at, updated_at)
         VALUES (COALESCE($1, gen_random_uuid()), $2, $3, $4, NOW(), NOW())
         RETURNING id, sku, name, price, created_at, updated_at;`,
        [data.id ?? null, data.sku, data.name, data.price],
      );
      const product = prodRes.rows[0]!;

      const stockRes = await client.query<{
        id: string;
        product_id: string;
        available_quantity: number;
        reserved_quantity: number;
        updated_at: Date;
      }>(
        `INSERT INTO stock (id, product_id, available_quantity, reserved_quantity, updated_at)
         VALUES (gen_random_uuid(), $1, $2, 0, NOW())
         RETURNING id, product_id, available_quantity, reserved_quantity, updated_at;`,
        [product.id, data.availableQuantity],
      );
      const stock = stockRes.rows[0]!;

      await client.query('COMMIT');
      return {
        product: {
          id: product.id,
          sku: product.sku,
          name: product.name,
          price: product.price,
          createdAt: product.created_at,
          updatedAt: product.updated_at,
        },
        stock: {
          id: stock.id,
          productId: stock.product_id,
          availableQuantity: Number(stock.available_quantity),
          reservedQuantity: Number(stock.reserved_quantity),
          updatedAt: stock.updated_at,
        },
      };
    } catch (err) {
      await client.query('ROLLBACK');
      const message = err instanceof Error ? err.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to create product with stock: ${message}`);
    } finally {
      client.release();
    }
  }
}
