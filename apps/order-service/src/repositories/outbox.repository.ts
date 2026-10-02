import { Pool, PoolClient } from 'pg';
import { OutboxEventRecord, OrderCreatedPayload } from '../domain/outbox-event.js';
import { DatabaseOperationError } from '../errors/order.errors.js';

export interface InsertOutboxEventInput {
  id?: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  eventVersion?: number;
  correlationId?: string;
  payload: unknown;
}

export interface IOutboxRepository {
  insertEvent(data: InsertOutboxEventInput, client?: PoolClient): Promise<OutboxEventRecord>;
  findUnpublishedEvents(limit?: number): Promise<OutboxEventRecord[]>;
  markAsPublished(id: string, publishedAt?: Date): Promise<void>;
  incrementRetryCount(id: string): Promise<void>;
  findById(id: string): Promise<OutboxEventRecord | null>;
  findByAggregateId(aggregateId: string): Promise<OutboxEventRecord[]>;
}

export class OutboxRepository implements IOutboxRepository {
  constructor(private readonly pool: Pool) {}

  /**
   * Inserts an outbox event. If a PoolClient is supplied, executes within an active transaction.
   */
  async insertEvent(data: InsertOutboxEventInput, client?: PoolClient): Promise<OutboxEventRecord> {
    const executor = client ?? this.pool;

    const sql = `
      INSERT INTO outbox_events (
        id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, created_at, published_at, retry_count
      )
      VALUES (
        COALESCE($1, gen_random_uuid()), $2, $3, $4, COALESCE($5, 1), COALESCE($6, gen_random_uuid()), $7, NOW(), NULL, 0
      )
      RETURNING id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, created_at, published_at, retry_count;
    `;

    try {
      const result = await executor.query<{
        id: string;
        aggregate_type: string;
        aggregate_id: string;
        event_type: string;
        event_version: number;
        correlation_id: string;
        payload: OrderCreatedPayload;
        created_at: Date;
        published_at: Date | null;
        retry_count: number;
      }>(sql, [
        data.id ?? null,
        data.aggregateType,
        data.aggregateId,
        data.eventType,
        data.eventVersion ?? 1,
        data.correlationId ?? null,
        JSON.stringify(data.payload),
      ]);

      const row = result.rows[0];
      if (!row) {
        throw new DatabaseOperationError('Failed to retrieve inserted outbox event');
      }

      return {
        id: row.id,
        aggregateType: row.aggregate_type,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        eventVersion: row.event_version,
        correlationId: row.correlation_id,
        payload: row.payload,
        createdAt: row.created_at,
        publishedAt: row.published_at,
        retryCount: row.retry_count,
      };
    } catch (error) {
      if (error instanceof DatabaseOperationError) {
        throw error;
      }
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to insert outbox event: ${message}`);
    }
  }

  /**
   * Retrieves unpublished events ordered chronologically for FIFO message processing.
   */
  async findUnpublishedEvents(limit = 50): Promise<OutboxEventRecord[]> {
    const sql = `
      SELECT id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, created_at, published_at, retry_count
      FROM outbox_events
      WHERE published_at IS NULL
      ORDER BY created_at ASC
      LIMIT $1;
    `;

    try {
      const result = await this.pool.query<{
        id: string;
        aggregate_type: string;
        aggregate_id: string;
        event_type: string;
        event_version: number;
        correlation_id: string;
        payload: OrderCreatedPayload;
        created_at: Date;
        published_at: Date | null;
        retry_count: number;
      }>(sql, [limit]);

      return result.rows.map((row) => ({
        id: row.id,
        aggregateType: row.aggregate_type,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        eventVersion: row.event_version,
        correlationId: row.correlation_id,
        payload: row.payload,
        createdAt: row.created_at,
        publishedAt: row.published_at,
        retryCount: row.retry_count,
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to query unpublished outbox events: ${message}`);
    }
  }

  /**
   * Marks an outbox event as published with a completion timestamp.
   */
  async markAsPublished(id: string, publishedAt?: Date): Promise<void> {
    const sql = `
      UPDATE outbox_events
      SET published_at = COALESCE($2, NOW())
      WHERE id = $1;
    `;

    try {
      await this.pool.query(sql, [id, publishedAt ?? null]);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to mark outbox event as published: ${message}`);
    }
  }

  /**
   * Increments the retry count after an unsuccessful publishing attempt.
   */
  async incrementRetryCount(id: string): Promise<void> {
    const sql = `
      UPDATE outbox_events
      SET retry_count = retry_count + 1
      WHERE id = $1;
    `;

    try {
      await this.pool.query(sql, [id]);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(
        `Failed to increment retry count for outbox event: ${message}`,
      );
    }
  }

  /**
   * Finds an outbox event by its primary key ID.
   */
  async findById(id: string): Promise<OutboxEventRecord | null> {
    const sql = `
      SELECT id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, created_at, published_at, retry_count
      FROM outbox_events
      WHERE id = $1;
    `;

    try {
      const result = await this.pool.query<{
        id: string;
        aggregate_type: string;
        aggregate_id: string;
        event_type: string;
        event_version: number;
        correlation_id: string;
        payload: OrderCreatedPayload;
        created_at: Date;
        published_at: Date | null;
        retry_count: number;
      }>(sql, [id]);

      const row = result.rows[0];
      if (!row) {
        return null;
      }

      return {
        id: row.id,
        aggregateType: row.aggregate_type,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        eventVersion: row.event_version,
        correlationId: row.correlation_id,
        payload: row.payload,
        createdAt: row.created_at,
        publishedAt: row.published_at,
        retryCount: row.retry_count,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch outbox event by ID: ${message}`);
    }
  }

  /**
   * Finds all outbox events associated with a specific aggregate ID.
   */
  async findByAggregateId(aggregateId: string): Promise<OutboxEventRecord[]> {
    const sql = `
      SELECT id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, created_at, published_at, retry_count
      FROM outbox_events
      WHERE aggregate_id = $1
      ORDER BY created_at ASC;
    `;

    try {
      const result = await this.pool.query<{
        id: string;
        aggregate_type: string;
        aggregate_id: string;
        event_type: string;
        event_version: number;
        correlation_id: string;
        payload: OrderCreatedPayload;
        created_at: Date;
        published_at: Date | null;
        retry_count: number;
      }>(sql, [aggregateId]);

      return result.rows.map((row) => ({
        id: row.id,
        aggregateType: row.aggregate_type,
        aggregateId: row.aggregate_id,
        eventType: row.event_type,
        eventVersion: row.event_version,
        correlationId: row.correlation_id,
        payload: row.payload,
        createdAt: row.created_at,
        publishedAt: row.published_at,
        retryCount: row.retry_count,
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch outbox events by aggregate ID: ${message}`);
    }
  }
}
