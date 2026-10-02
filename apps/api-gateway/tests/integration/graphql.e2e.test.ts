import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Pool } from 'pg';
import { FastifyInstance } from 'fastify';
import { buildHttpApp } from '../../../order-service/src/http/app.js';
import { OrderRepository } from '../../../order-service/src/repositories/order.repository.js';
import { OrderService } from '../../../order-service/src/services/order.service.js';
import { createDatabasePool } from '../../../order-service/src/db/client.js';
import { runMigrations } from '../../../order-service/src/db/migrate.js';
import { OrderServiceClient } from '../../src/clients/order-service.client.js';
import { startGatewayServer, RunningGatewayServer } from '../../src/server.js';

describe('GraphQL API Gateway -> Order Service HTTP -> PostgreSQL (End-to-End Flow)', () => {
  let pool: Pool;
  let orderApp: FastifyInstance;
  let gateway: RunningGatewayServer;
  let gatewayUrl: string;

  const testCustomerId = 'e82a32c2-849a-4c28-97fb-c5bb20d436a5';
  const testProductId1 = 'f82a32c2-849a-4c28-97fb-c5bb20d436a6';
  const testProductId2 = 'a82a32c2-849a-4c28-97fb-c5bb20d436a7';

  beforeAll(async () => {
    // 1. Initialize PostgreSQL database and apply migrations
    pool = createDatabasePool({ max: 5 });
    await runMigrations(pool);

    // 2. Start Order Service Fastify HTTP app on an available port
    const repository = new OrderRepository(pool);
    const orderService = new OrderService(repository);
    orderApp = buildHttpApp({ orderService });
    await orderApp.listen({ port: 4101, host: '127.0.0.1' });

    // 3. Start API Gateway with OrderServiceClient pointing to Order Service
    const orderServiceClient = new OrderServiceClient({
      baseUrl: 'http://127.0.0.1:4101',
    });
    gateway = await startGatewayServer(4100, { orderServiceClient });
    gatewayUrl = 'http://127.0.0.1:4100/graphql';
  });

  afterAll(async () => {
    if (gateway) {
      await gateway.stop();
    }
    if (orderApp) {
      await orderApp.close();
    }
    if (pool) {
      await pool.end();
    }
  });

  const sendGraphQL = async (query: string, variables?: Record<string, unknown>) => {
    const res = await fetch(gatewayUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ query, variables }),
    });
    return (await res.json()) as {
      data?: Record<string, unknown>;
      errors?: Array<{ message: string; extensions?: { code?: string } }>;
    };
  };

  it('executes createOrder mutation and verifies persistence in PostgreSQL', async () => {
    const CREATE_ORDER_MUTATION = `#graphql
      mutation CreateOrder($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id
          customerId
          status
          totalAmount
          currency
          items {
            productId
            quantity
            unitPrice
          }
          createdAt
          updatedAt
        }
      }
    `;

    const result = await sendGraphQL(CREATE_ORDER_MUTATION, {
      input: {
        customerId: testCustomerId,
        items: [
          {
            productId: testProductId1,
            quantity: 2,
            unitPrice: '100.00',
          },
          {
            productId: testProductId2,
            quantity: 1,
            unitPrice: '49.99',
          },
        ],
      },
    });

    expect(result.errors).toBeUndefined();
    expect(result.data).toBeDefined();

    const createdOrder = result.data!.createOrder as {
      id: string;
      customerId: string;
      status: string;
      totalAmount: string;
      currency: string;
      items: Array<{ productId: string; quantity: number; unitPrice: string }>;
      createdAt: string;
      updatedAt: string;
    };

    expect(createdOrder.id).toBeDefined();
    expect(createdOrder.customerId).toBe(testCustomerId);
    expect(createdOrder.status).toBe('PENDING');
    expect(createdOrder.totalAmount).toBe('249.99');
    expect(createdOrder.currency).toBe('USD');
    expect(createdOrder.items).toHaveLength(2);

    // Verify persistence directly in PostgreSQL
    const dbOrder = await pool.query('SELECT * FROM orders WHERE id = $1', [createdOrder.id]);
    expect(dbOrder.rows).toHaveLength(1);
    expect(dbOrder.rows[0].customer_id).toBe(testCustomerId);
    expect(dbOrder.rows[0].status).toBe('PENDING');
    expect(dbOrder.rows[0].total_amount).toBe('249.99');

    const dbItems = await pool.query(
      'SELECT * FROM order_items WHERE order_id = $1 ORDER BY unit_price DESC',
      [createdOrder.id],
    );
    expect(dbItems.rows).toHaveLength(2);
    expect(dbItems.rows[0].unit_price).toBe('100.00');

    // Query the created order through GraphQL order(id)
    const GET_ORDER_QUERY = `#graphql
      query GetOrder($id: ID!) {
        order(id: $id) {
          id
          customerId
          status
          totalAmount
          currency
          items {
            productId
            quantity
            unitPrice
          }
        }
      }
    `;

    const queryResult = await sendGraphQL(GET_ORDER_QUERY, { id: createdOrder.id });
    expect(queryResult.errors).toBeUndefined();
    const fetchedOrder = queryResult.data!.order as typeof createdOrder;
    expect(fetchedOrder.id).toBe(createdOrder.id);
    expect(fetchedOrder.totalAmount).toBe('249.99');
    expect(fetchedOrder.items).toHaveLength(2);

    // Query orders by customerId through GraphQL orders(customerId)
    const GET_ORDERS_QUERY = `#graphql
      query GetOrders($customerId: ID) {
        orders(customerId: $customerId) {
          id
          customerId
          status
          totalAmount
        }
      }
    `;

    const customerQueryResult = await sendGraphQL(GET_ORDERS_QUERY, {
      customerId: testCustomerId,
    });
    expect(customerQueryResult.errors).toBeUndefined();
    const customerOrders = customerQueryResult.data!.orders as Array<{ id: string }>;
    expect(customerOrders.length).toBeGreaterThanOrEqual(1);
    expect(customerOrders.some((o) => o.id === createdOrder.id)).toBe(true);
  });

  it('order query returns null for non-existent order ID', async () => {
    const unknownId = '99999999-9999-9999-9999-999999999999';
    const GET_ORDER_QUERY = `#graphql
      query GetOrder($id: ID!) {
        order(id: $id) {
          id
          totalAmount
        }
      }
    `;

    const result = await sendGraphQL(GET_ORDER_QUERY, { id: unknownId });
    expect(result.errors).toBeUndefined();
    expect(result.data?.order).toBeNull();
  });

  it('rejects invalid order input with a GraphQL validation error', async () => {
    const CREATE_ORDER_MUTATION = `#graphql
      mutation CreateOrder($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id
        }
      }
    `;

    const result = await sendGraphQL(CREATE_ORDER_MUTATION, {
      input: {
        customerId: 'not-a-valid-uuid',
        items: [
          {
            productId: testProductId1,
            quantity: 1,
            unitPrice: '10.00',
          },
        ],
      },
    });

    expect(result.errors).toBeDefined();
    expect(result.errors![0]?.message).toContain('Customer ID must be a valid UUID');
    expect(result.errors![0]?.extensions?.code).toBe('BAD_USER_INPUT');
  });
});
