import { Pool } from 'pg';
import { Order, OrderItem } from '../domain/order.js';
import { OrderStatus } from '../domain/order-status.js';
import { DatabaseOperationError, OrderNotFoundError } from '../errors/order.errors.js';
import {
  InsertOutboxEventInput,
  IOutboxRepository,
  OutboxRepository,
} from './outbox.repository.js';

export interface CreateOrderRepositoryInput {
  id?: string;
  customerId: string;
  status: OrderStatus;
  totalAmount: string;
  currency: string;
  items: Array<{
    id?: string;
    productId: string;
    quantity: number;
    unitPrice: string;
  }>;
  outboxEvent?: InsertOutboxEventInput;
}

export interface UpdateOrderStatusResult {
  order: Order;
  alreadyUpdated: boolean;
}

export interface IOrderRepository {
  createOrder(data: CreateOrderRepositoryInput): Promise<Order>;
  findOrderById(id: string): Promise<Order | null>;
  findOrdersByCustomerId(customerId: string): Promise<Order[]>;
  findAllOrders?(): Promise<Order[]>;
  updateOrderStatus(orderId: string, status: OrderStatus): Promise<UpdateOrderStatusResult>;
}

export class OrderRepository implements IOrderRepository {
  private readonly outboxRepository: IOutboxRepository;

  constructor(
    private readonly pool: Pool,
    outboxRepository?: IOutboxRepository,
  ) {
    this.outboxRepository = outboxRepository ?? new OutboxRepository(pool);
  }

  /**
   * Creates an order and its associated order items in a single PostgreSQL transaction.
   * If any insert fails, all operations are rolled back.
   * The checked-out client is guaranteed to be released in the finally block.
   */
  async createOrder(data: CreateOrderRepositoryInput): Promise<Order> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      const insertOrderSql = `
        INSERT INTO orders (id, customer_id, status, total_amount, currency, created_at, updated_at)
        VALUES (COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, NOW(), NOW())
        RETURNING id, customer_id, status, total_amount, currency, created_at, updated_at;
      `;

      const orderResult = await client.query<{
        id: string;
        customer_id: string;
        status: string;
        total_amount: string;
        currency: string;
        created_at: Date;
        updated_at: Date;
      }>(insertOrderSql, [
        data.id ?? null,
        data.customerId,
        data.status,
        data.totalAmount,
        data.currency,
      ]);

      const orderRow = orderResult.rows[0];
      if (!orderRow) {
        throw new DatabaseOperationError('Failed to retrieve inserted order record');
      }

      const orderId = orderRow.id;
      const createdItems: OrderItem[] = [];

      const insertItemSql = `
        INSERT INTO order_items (id, order_id, product_id, quantity, unit_price, created_at)
        VALUES (COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, NOW())
        RETURNING id, order_id, product_id, quantity, unit_price, created_at;
      `;

      for (const item of data.items) {
        const itemResult = await client.query<{
          id: string;
          order_id: string;
          product_id: string;
          quantity: number;
          unit_price: string;
          created_at: Date;
        }>(insertItemSql, [
          item.id ?? null,
          orderId,
          item.productId,
          item.quantity,
          item.unitPrice,
        ]);

        const itemRow = itemResult.rows[0];
        if (!itemRow) {
          throw new DatabaseOperationError('Failed to retrieve inserted order item record');
        }

        createdItems.push({
          id: itemRow.id,
          orderId: itemRow.order_id,
          productId: itemRow.product_id,
          quantity: Number(itemRow.quantity),
          unitPrice: String(itemRow.unit_price),
          createdAt: itemRow.created_at,
        });
      }

      // Persist the outbox event within the same atomic transaction
      if (data.outboxEvent) {
        await this.outboxRepository.insertEvent(
          {
            ...data.outboxEvent,
            aggregateId: data.outboxEvent.aggregateId || orderId,
          },
          client,
        );
      } else {
        await this.outboxRepository.insertEvent(
          {
            aggregateType: 'Order',
            aggregateId: orderId,
            eventType: 'OrderCreated',
            eventVersion: 1,
            payload: {
              orderId,
              customerId: data.customerId,
              items: data.items.map((it) => ({
                productId: it.productId,
                quantity: it.quantity,
              })),
            },
          },
          client,
        );
      }

      await client.query('COMMIT');

      return {
        id: orderRow.id,
        customerId: orderRow.customer_id,
        status: orderRow.status as OrderStatus,
        totalAmount: String(orderRow.total_amount),
        currency: orderRow.currency,
        items: createdItems,
        createdAt: orderRow.created_at,
        updatedAt: orderRow.updated_at,
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {
        // Rollback failure handler
      });
      if (error instanceof DatabaseOperationError) {
        throw error;
      }
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to create order: ${message}`);
    } finally {
      client.release();
    }
  }

  /**
   * Retrieves an order by ID along with its associated items.
   */
  async findOrderById(id: string): Promise<Order | null> {
    try {
      const orderSql = `
        SELECT id, customer_id, status, total_amount, currency, created_at, updated_at
        FROM orders
        WHERE id = $1;
      `;
      const orderResult = await this.pool.query<{
        id: string;
        customer_id: string;
        status: string;
        total_amount: string;
        currency: string;
        created_at: Date;
        updated_at: Date;
      }>(orderSql, [id]);

      const orderRow = orderResult.rows[0];
      if (!orderRow) {
        return null;
      }

      const itemsSql = `
        SELECT id, order_id, product_id, quantity, unit_price, created_at
        FROM order_items
        WHERE order_id = $1
        ORDER BY created_at ASC;
      `;
      const itemsResult = await this.pool.query<{
        id: string;
        order_id: string;
        product_id: string;
        quantity: number;
        unit_price: string;
        created_at: Date;
      }>(itemsSql, [id]);

      return {
        id: orderRow.id,
        customerId: orderRow.customer_id,
        status: orderRow.status as OrderStatus,
        totalAmount: String(orderRow.total_amount),
        currency: orderRow.currency,
        items: itemsResult.rows.map((r) => ({
          id: r.id,
          orderId: r.order_id,
          productId: r.product_id,
          quantity: Number(r.quantity),
          unitPrice: String(r.unit_price),
          createdAt: r.created_at,
        })),
        createdAt: orderRow.created_at,
        updatedAt: orderRow.updated_at,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch order: ${message}`);
    }
  }

  /**
   * Retrieves all orders for a customer with their items, avoiding N+1 queries.
   */
  async findOrdersByCustomerId(customerId: string): Promise<Order[]> {
    try {
      const ordersSql = `
        SELECT id, customer_id, status, total_amount, currency, created_at, updated_at
        FROM orders
        WHERE customer_id = $1
        ORDER BY created_at DESC;
      `;
      const ordersResult = await this.pool.query<{
        id: string;
        customer_id: string;
        status: string;
        total_amount: string;
        currency: string;
        created_at: Date;
        updated_at: Date;
      }>(ordersSql, [customerId]);

      if (ordersResult.rows.length === 0) {
        return [];
      }

      const orderIds = ordersResult.rows.map((row) => row.id);

      // Single batched query for all items belonging to these orders (avoids N+1)
      const itemsSql = `
        SELECT id, order_id, product_id, quantity, unit_price, created_at
        FROM order_items
        WHERE order_id = ANY($1::uuid[])
        ORDER BY created_at ASC;
      `;
      const itemsResult = await this.pool.query<{
        id: string;
        order_id: string;
        product_id: string;
        quantity: number;
        unit_price: string;
        created_at: Date;
      }>(itemsSql, [orderIds]);

      const itemsByOrderId = new Map<string, OrderItem[]>();
      for (const r of itemsResult.rows) {
        const item: OrderItem = {
          id: r.id,
          orderId: r.order_id,
          productId: r.product_id,
          quantity: Number(r.quantity),
          unitPrice: String(r.unit_price),
          createdAt: r.created_at,
        };
        const existing = itemsByOrderId.get(r.order_id) ?? [];
        existing.push(item);
        itemsByOrderId.set(r.order_id, existing);
      }

      return ordersResult.rows.map((orderRow) => ({
        id: orderRow.id,
        customerId: orderRow.customer_id,
        status: orderRow.status as OrderStatus,
        totalAmount: String(orderRow.total_amount),
        currency: orderRow.currency,
        items: itemsByOrderId.get(orderRow.id) ?? [],
        createdAt: orderRow.created_at,
        updatedAt: orderRow.updated_at,
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch orders by customer ID: ${message}`);
    }
  }

  /**
   * Retrieves all orders with their items, avoiding N+1 queries.
   */
  async findAllOrders(): Promise<Order[]> {
    try {
      const ordersSql = `
        SELECT id, customer_id, status, total_amount, currency, created_at, updated_at
        FROM orders
        ORDER BY created_at DESC;
      `;
      const ordersResult = await this.pool.query<{
        id: string;
        customer_id: string;
        status: string;
        total_amount: string;
        currency: string;
        created_at: Date;
        updated_at: Date;
      }>(ordersSql);

      if (ordersResult.rows.length === 0) {
        return [];
      }

      const orderIds = ordersResult.rows.map((row) => row.id);

      const itemsSql = `
        SELECT id, order_id, product_id, quantity, unit_price, created_at
        FROM order_items
        WHERE order_id = ANY($1::uuid[])
        ORDER BY created_at ASC;
      `;
      const itemsResult = await this.pool.query<{
        id: string;
        order_id: string;
        product_id: string;
        quantity: number;
        unit_price: string;
        created_at: Date;
      }>(itemsSql, [orderIds]);

      const itemsByOrderId = new Map<string, OrderItem[]>();
      for (const r of itemsResult.rows) {
        const item: OrderItem = {
          id: r.id,
          orderId: r.order_id,
          productId: r.product_id,
          quantity: Number(r.quantity),
          unitPrice: String(r.unit_price),
          createdAt: r.created_at,
        };
        const existing = itemsByOrderId.get(r.order_id) ?? [];
        existing.push(item);
        itemsByOrderId.set(r.order_id, existing);
      }

      return ordersResult.rows.map((orderRow) => ({
        id: orderRow.id,
        customerId: orderRow.customer_id,
        status: orderRow.status as OrderStatus,
        totalAmount: String(orderRow.total_amount),
        currency: orderRow.currency,
        items: itemsByOrderId.get(orderRow.id) ?? [],
        createdAt: orderRow.created_at,
        updatedAt: orderRow.updated_at,
      }));
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to fetch all orders: ${message}`);
    }
  }

  /**
   * Updates an order's status. Returns the updated order and whether the status
   * was already set (idempotent guard). Only updates from PENDING status to avoid
   * conflicting concurrent transitions.
   */
  async updateOrderStatus(orderId: string, status: OrderStatus): Promise<UpdateOrderStatusResult> {
    const client = await this.pool.connect();

    try {
      await client.query('BEGIN');

      // Lock the order row and check current status
      const lockSql = `
        SELECT id, customer_id, status, total_amount, currency, created_at, updated_at
        FROM orders
        WHERE id = $1
        FOR UPDATE;
      `;
      const lockResult = await client.query<{
        id: string;
        customer_id: string;
        status: string;
        total_amount: string;
        currency: string;
        created_at: Date;
        updated_at: Date;
      }>(lockSql, [orderId]);

      const orderRow = lockResult.rows[0];
      if (!orderRow) {
        await client.query('ROLLBACK');
        throw new OrderNotFoundError(orderId);
      }

      // Idempotent: if already in the target status, return without update
      if (orderRow.status === status) {
        await client.query('COMMIT');

        const itemsSql = `
          SELECT id, order_id, product_id, quantity, unit_price, created_at
          FROM order_items
          WHERE order_id = $1
          ORDER BY created_at ASC;
        `;
        const itemsResult = await client.query<{
          id: string;
          order_id: string;
          product_id: string;
          quantity: number;
          unit_price: string;
          created_at: Date;
        }>(itemsSql, [orderId]);

        return {
          order: {
            id: orderRow.id,
            customerId: orderRow.customer_id,
            status: orderRow.status as OrderStatus,
            totalAmount: String(orderRow.total_amount),
            currency: orderRow.currency,
            items: itemsResult.rows.map((r) => ({
              id: r.id,
              orderId: r.order_id,
              productId: r.product_id,
              quantity: Number(r.quantity),
              unitPrice: String(r.unit_price),
              createdAt: r.created_at,
            })),
            createdAt: orderRow.created_at,
            updatedAt: orderRow.updated_at,
          },
          alreadyUpdated: true,
        };
      }

      // Explicit domain status transition rules
      const validTransitions: Record<string, string[]> = {
        PENDING: ['INVENTORY_PROCESSING', 'CONFIRMED', 'INVENTORY_FAILED'],
        INVENTORY_PROCESSING: ['CONFIRMED', 'INVENTORY_FAILED'],
        CONFIRMED: [],
        INVENTORY_FAILED: [],
      };

      const allowed = validTransitions[orderRow.status];
      if (!allowed || !allowed.includes(status)) {
        await client.query('COMMIT');
        console.warn(
          `[OrderRepository] Disallowed status transition from ${orderRow.status} to ${status} for order ${orderId}`,
        );

        const itemsSql = `
          SELECT id, order_id, product_id, quantity, unit_price, created_at
          FROM order_items
          WHERE order_id = $1
          ORDER BY created_at ASC;
        `;
        const itemsResult = await client.query<{
          id: string;
          order_id: string;
          product_id: string;
          quantity: number;
          unit_price: string;
          created_at: Date;
        }>(itemsSql, [orderId]);

        return {
          order: {
            id: orderRow.id,
            customerId: orderRow.customer_id,
            status: orderRow.status as OrderStatus,
            totalAmount: String(orderRow.total_amount),
            currency: orderRow.currency,
            items: itemsResult.rows.map((r) => ({
              id: r.id,
              orderId: r.order_id,
              productId: r.product_id,
              quantity: Number(r.quantity),
              unitPrice: String(r.unit_price),
              createdAt: r.created_at,
            })),
            createdAt: orderRow.created_at,
            updatedAt: orderRow.updated_at,
          },
          alreadyUpdated: true,
        };
      }

      // Update the status
      const updateSql = `
        UPDATE orders
        SET status = $2, updated_at = NOW()
        WHERE id = $1
        RETURNING id, customer_id, status, total_amount, currency, created_at, updated_at;
      `;
      const updateResult = await client.query<{
        id: string;
        customer_id: string;
        status: string;
        total_amount: string;
        currency: string;
        created_at: Date;
        updated_at: Date;
      }>(updateSql, [orderId, status]);

      const updatedRow = updateResult.rows[0];
      if (!updatedRow) {
        throw new DatabaseOperationError(`Failed to update order status for ID "${orderId}"`);
      }

      const itemsSql = `
        SELECT id, order_id, product_id, quantity, unit_price, created_at
        FROM order_items
        WHERE order_id = $1
        ORDER BY created_at ASC;
      `;
      const itemsResult = await client.query<{
        id: string;
        order_id: string;
        product_id: string;
        quantity: number;
        unit_price: string;
        created_at: Date;
      }>(itemsSql, [orderId]);

      await client.query('COMMIT');

      return {
        order: {
          id: updatedRow.id,
          customerId: updatedRow.customer_id,
          status: updatedRow.status as OrderStatus,
          totalAmount: String(updatedRow.total_amount),
          currency: updatedRow.currency,
          items: itemsResult.rows.map((r) => ({
            id: r.id,
            orderId: r.order_id,
            productId: r.product_id,
            quantity: Number(r.quantity),
            unitPrice: String(r.unit_price),
            createdAt: r.created_at,
          })),
          createdAt: updatedRow.created_at,
          updatedAt: updatedRow.updated_at,
        },
        alreadyUpdated: false,
      };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {
        // Rollback failure handler
      });
      if (error instanceof DatabaseOperationError) {
        throw error;
      }
      const message = error instanceof Error ? error.message : 'Unknown database error';
      throw new DatabaseOperationError(`Failed to update order status: ${message}`);
    } finally {
      client.release();
    }
  }
}
