import { Pool } from 'pg';
import { Reservation, ReservationItem, CreateReservationInput } from '../domain/reservation.js';
import { ReservationStatus, RESERVATION_STATUS } from '../domain/reservation-status.js';
import { DatabaseOperationError } from '../errors/inventory.errors.js';
import {
  InsertOutboxEventInput,
  IOutboxRepository,
  InventoryOutboxRepository,
} from './outbox.repository.js';

export interface ReservationResult {
  reservation: Reservation;
  alreadyExisted: boolean;
}

export interface IInventoryRepository {
  createReservation(data: CreateReservationInput): Promise<ReservationResult>;
  findReservationByOrderId(orderId: string): Promise<Reservation | null>;
  findReservationById(id: string): Promise<Reservation | null>;
  updateReservationStatus(
    orderId: string,
    status: ReservationStatus,
    outboxEvent: InsertOutboxEventInput,
  ): Promise<Reservation>;
}

export class InventoryRepository implements IInventoryRepository {
  private readonly outboxRepository: IOutboxRepository;

  constructor(
    private readonly pool: Pool,
    outboxRepository?: IOutboxRepository,
  ) {
    this.outboxRepository = outboxRepository ?? new InventoryOutboxRepository(pool);
  }

  /**
   * Atomically creates a reservation and its items in a single PostgreSQL transaction.
   * If a reservation already exists for the given orderId (idempotent protection),
   * returns the existing reservation with alreadyExisted = true.
   */
  async createReservation(data: CreateReservationInput): Promise<ReservationResult> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // 1. Check whether a reservation already exists for this order (idempotent guard)
      const existingResult = await client.query<{
        id: string;
        order_id: string;
        status: string;
        created_at: Date;
        updated_at: Date;
      }>(
        `SELECT id, order_id, status, created_at, updated_at
         FROM inventory_reservations
         WHERE order_id = $1
         FOR UPDATE;`,
        [data.orderId],
      );

      if (existingResult.rows.length > 0) {
        // Reservation already exists — idempotent completion
        const existingRow = existingResult.rows[0]!;
        const existingItems = await this.queryReservationItems(existingRow.id, client);

        await client.query('COMMIT');

        return {
          reservation: {
            id: existingRow.id,
            orderId: existingRow.order_id,
            status: existingRow.status as ReservationStatus,
            items: existingItems,
            createdAt: existingRow.created_at,
            updatedAt: existingRow.updated_at,
          },
          alreadyExisted: true,
        };
      }

      // 2. Insert new reservation
      const insertReservationSql = `
        INSERT INTO inventory_reservations (id, order_id, status, created_at, updated_at)
        VALUES (gen_random_uuid(), $1, $2, NOW(), NOW())
        RETURNING id, order_id, status, created_at, updated_at;
      `;

      const reservationResult = await client.query<{
        id: string;
        order_id: string;
        status: string;
        created_at: Date;
        updated_at: Date;
      }>(insertReservationSql, [data.orderId, RESERVATION_STATUS.PENDING]);

      const reservationRow = reservationResult.rows[0];
      if (!reservationRow) {
        throw new DatabaseOperationError('Failed to retrieve inserted reservation record');
      }

      const reservationId = reservationRow.id;
      const createdItems: ReservationItem[] = [];

      // 3. Insert reservation items
      const insertItemSql = `
        INSERT INTO inventory_reservation_items (id, reservation_id, product_id, quantity, created_at)
        VALUES (gen_random_uuid(), $1, $2, $3, NOW())
        RETURNING id, reservation_id, product_id, quantity, created_at;
      `;

      for (const item of data.items) {
        const itemResult = await client.query<{
          id: string;
          reservation_id: string;
          product_id: string;
          quantity: number;
          created_at: Date;
        }>(insertItemSql, [reservationId, item.productId, item.quantity]);

        const itemRow = itemResult.rows[0];
        if (!itemRow) {
          throw new DatabaseOperationError('Failed to retrieve inserted reservation item record');
        }

        createdItems.push({
          id: itemRow.id,
          reservationId: itemRow.reservation_id,
          productId: itemRow.product_id,
          quantity: Number(itemRow.quantity),
          createdAt: itemRow.created_at,
        });
      }

      await client.query('COMMIT');

      return {
        reservation: {
          id: reservationRow.id,
          orderId: reservationRow.order_id,
          status: reservationRow.status as ReservationStatus,
          items: createdItems,
          createdAt: reservationRow.created_at,
          updatedAt: reservationRow.updated_at,
        },
        alreadyExisted: false,
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {
        // Rollback failure handler
      });
      if (error instanceof DatabaseOperationError) {
        throw error;
      }
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to create reservation: ${message}`);
    } finally {
      client.release();
    }
  }

  /**
   * Retrieves a reservation by order_id along with its items.
   */
  async findReservationByOrderId(orderId: string): Promise<Reservation | null> {
    try {
      const reservationSql = `
        SELECT id, order_id, status, created_at, updated_at
        FROM inventory_reservations
        WHERE order_id = $1;
      `;

      const reservationResult = await this.pool.query<{
        id: string;
        order_id: string;
        status: string;
        created_at: Date;
        updated_at: Date;
      }>(reservationSql, [orderId]);

      const row = reservationResult.rows[0];
      if (!row) {
        return null;
      }

      const items = await this.queryReservationItems(row.id);

      return {
        id: row.id,
        orderId: row.order_id,
        status: row.status as ReservationStatus,
        items,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch reservation by order ID: ${message}`);
    }
  }

  /**
   * Retrieves a reservation by its primary key along with its items.
   */
  async findReservationById(id: string): Promise<Reservation | null> {
    try {
      const reservationSql = `
        SELECT id, order_id, status, created_at, updated_at
        FROM inventory_reservations
        WHERE id = $1;
      `;

      const reservationResult = await this.pool.query<{
        id: string;
        order_id: string;
        status: string;
        created_at: Date;
        updated_at: Date;
      }>(reservationSql, [id]);

      const row = reservationResult.rows[0];
      if (!row) {
        return null;
      }

      const items = await this.queryReservationItems(row.id);

      return {
        id: row.id,
        orderId: row.order_id,
        status: row.status as ReservationStatus,
        items,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch reservation by ID: ${message}`);
    }
  }

  /**
   * Queries reservation items for a given reservation ID.
   * Accepts an optional client for use within an active transaction.
   */
  private async queryReservationItems(
    reservationId: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    executor?: { query: (...args: any[]) => Promise<any> },
  ): Promise<ReservationItem[]> {
    const queryExecutor = executor ?? this.pool;

    const itemsSql = `
      SELECT id, reservation_id, product_id, quantity, created_at
      FROM inventory_reservation_items
      WHERE reservation_id = $1
      ORDER BY created_at ASC;
    `;

    const itemsResult = await queryExecutor.query<{
      id: string;
      reservation_id: string;
      product_id: string;
      quantity: number;
      created_at: Date;
    }>(itemsSql, [reservationId]);

    return itemsResult.rows.map(
      (r: {
        id: string;
        reservation_id: string;
        product_id: string;
        quantity: number;
        created_at: Date;
      }) => ({
        id: r.id,
        reservationId: r.reservation_id,
        productId: r.product_id,
        quantity: Number(r.quantity),
        createdAt: r.created_at,
      }),
    );
  }

  /**
   * Atomically updates a reservation's status and inserts an outbox event
   * within a single PostgreSQL transaction, ensuring the outbox event
   * and the status change are never out of sync.
   */
  async updateReservationStatus(
    orderId: string,
    status: ReservationStatus,
    outboxEvent: InsertOutboxEventInput,
  ): Promise<Reservation> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // 1. Update reservation status
      const updateSql = `
        UPDATE inventory_reservations
        SET status = $2, updated_at = NOW()
        WHERE order_id = $1
        RETURNING id, order_id, status, created_at, updated_at;
      `;

      const updateResult = await client.query<{
        id: string;
        order_id: string;
        status: string;
        created_at: Date;
        updated_at: Date;
      }>(updateSql, [orderId, status]);

      const reservationRow = updateResult.rows[0];
      if (!reservationRow) {
        throw new DatabaseOperationError(`No reservation found for order_id ${orderId} to update`);
      }

      // 2. Insert outbox event within the same transaction
      if (!this.outboxRepository) {
        throw new DatabaseOperationError('Outbox repository not configured for status update');
      }

      await this.outboxRepository.insertEvent(outboxEvent, client);

      // 3. Query reservation items
      const items = await this.queryReservationItems(reservationRow.id, client);

      await client.query('COMMIT');

      return {
        id: reservationRow.id,
        orderId: reservationRow.order_id,
        status: reservationRow.status as ReservationStatus,
        items,
        createdAt: reservationRow.created_at,
        updatedAt: reservationRow.updated_at,
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {
        // Rollback failure handler
      });
      if (error instanceof DatabaseOperationError) {
        throw error;
      }
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to update reservation status: ${message}`);
    } finally {
      client.release();
    }
  }
}
