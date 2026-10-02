import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { FastifyInstance } from 'fastify';
import { createDatabasePool } from '../../src/db/client.js';
import { OrderRepository } from '../../src/repositories/order.repository.js';
import { OrderService } from '../../src/services/order.service.js';
import { buildHttpApp } from '../../src/http/app.js';
import { runMigrations } from '../../src/db/migrate.js';

describe('Order Service HTTP API (Integration Tests with PostgreSQL)', () => {
  let pool: Pool;
  let app: FastifyInstance;
  const testCustomerId = '3fa85f64-5717-4562-b3fc-2c963f66afa6';
  const testProductId1 = 'c9a646d3-9c61-4cd7-bf17-0f829f04653a';
  const testProductId2 = 'd9a646d3-9c61-4cd7-bf17-0f829f04653b';

  beforeAll(async () => {
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);

    const repository = new OrderRepository(pool);
    const orderService = new OrderService(repository);
    app = buildHttpApp({ orderService });
    await app.ready();
  });

  afterAll(async () => {
    if (app) {
      await app.close();
    }
    if (pool) {
      await pool.end();
    }
  });

  it('GET /health returns ok status', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/health',
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.status).toBe('ok');
    expect(body.service).toBe('order-service');
  });

  it('POST /orders creates an order and persists to PostgreSQL', async () => {
    const createPayload = {
      customerId: testCustomerId,
      currency: 'USD',
      items: [
        { productId: testProductId1, quantity: 2, unitPrice: '45.50' },
        { productId: testProductId2, quantity: 1, unitPrice: '10.00' },
      ],
    };

    const response = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: createPayload,
    });

    expect(response.statusCode).toBe(201);
    const createdOrder = JSON.parse(response.body);

    expect(createdOrder).toHaveProperty('id');
    expect(createdOrder.customerId).toBe(testCustomerId);
    expect(createdOrder.status).toBe('PENDING');
    expect(createdOrder.totalAmount).toBe('101.00'); // (2 * 45.50) + (1 * 10.00)
    expect(createdOrder.currency).toBe('USD');
    expect(createdOrder.items).toHaveLength(2);

    // Verify in PostgreSQL directly
    const pgOrder = await pool.query('SELECT * FROM orders WHERE id = $1', [createdOrder.id]);
    expect(pgOrder.rows).toHaveLength(1);
    expect(pgOrder.rows[0].customer_id).toBe(testCustomerId);
    expect(pgOrder.rows[0].total_amount).toBe('101.00');

    const pgItems = await pool.query(
      'SELECT * FROM order_items WHERE order_id = $1 ORDER BY unit_price DESC',
      [createdOrder.id],
    );
    expect(pgItems.rows).toHaveLength(2);
  });

  it('GET /orders/:id returns existing order', async () => {
    // First create an order
    const createResponse = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: {
        customerId: testCustomerId,
        items: [{ productId: testProductId1, quantity: 1, unitPrice: '25.00' }],
      },
    });

    const created = JSON.parse(createResponse.body);

    // Fetch by ID
    const fetchResponse = await app.inject({
      method: 'GET',
      url: `/orders/${created.id}`,
    });

    expect(fetchResponse.statusCode).toBe(200);
    const fetched = JSON.parse(fetchResponse.body);
    expect(fetched.id).toBe(created.id);
    expect(fetched.totalAmount).toBe('25.00');
    expect(fetched.items).toHaveLength(1);
  });

  it('GET /orders/:id returns 404 for unknown order ID', async () => {
    const unknownId = '00000000-0000-0000-0000-000000000000';
    const response = await app.inject({
      method: 'GET',
      url: `/orders/${unknownId}`,
    });

    expect(response.statusCode).toBe(404);
    const body = JSON.parse(response.body);
    expect(body.error).toBe('ORDER_NOT_FOUND');
  });

  it('GET /orders?customerId=... returns matching orders for customer', async () => {
    const customerId = '4fa85f64-5717-4562-b3fc-2c963f66afa7';

    // Create 2 orders for this customer
    await app.inject({
      method: 'POST',
      url: '/orders',
      payload: {
        customerId,
        items: [{ productId: testProductId1, quantity: 1, unitPrice: '15.00' }],
      },
    });

    await app.inject({
      method: 'POST',
      url: '/orders',
      payload: {
        customerId,
        items: [{ productId: testProductId2, quantity: 3, unitPrice: '20.00' }],
      },
    });

    const response = await app.inject({
      method: 'GET',
      url: `/orders?customerId=${customerId}`,
    });

    expect(response.statusCode).toBe(200);
    const orders = JSON.parse(response.body);
    expect(Array.isArray(orders)).toBe(true);
    expect(orders.length).toBeGreaterThanOrEqual(2);
    expect(orders.every((o: { customerId: string }) => o.customerId === customerId)).toBe(true);
  });

  it('POST /orders rejects invalid input with 400 validation error', async () => {
    const invalidPayload = {
      customerId: 'not-a-uuid',
      items: [],
    };

    const response = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: invalidPayload,
    });

    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.error).toBe('VALIDATION_ERROR');
    expect(Array.isArray(body.issues)).toBe(true);
    expect(body.issues.length).toBeGreaterThan(0);
  });
});
