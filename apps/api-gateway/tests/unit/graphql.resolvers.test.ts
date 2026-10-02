import { describe, it, expect, vi } from 'vitest';
import { ApolloServer } from '@apollo/server';
import { createApolloServer } from '../../src/server.js';
import {
  IOrderServiceClient,
  OrderServiceValidationError,
} from '../../src/clients/order-service.client.js';
import { GraphQLContext } from '../../src/graphql/resolvers/order.resolver.js';
import { OrderDto } from '../../src/types/order.types.js';

describe('GraphQL Resolvers & Scalars (Unit Tests with Mocked OrderServiceClient)', () => {
  const sampleOrderId = '11111111-1111-1111-1111-111111111111';
  const sampleCustomerId = '22222222-2222-2222-2222-222222222222';
  const sampleProductId = '33333333-3333-3333-3333-333333333333';
  const sampleIsoDate = '2026-10-02T10:00:00.000Z';

  const mockOrder: OrderDto = {
    id: sampleOrderId,
    customerId: sampleCustomerId,
    status: 'PENDING',
    totalAmount: '150.00',
    currency: 'USD',
    items: [
      {
        id: '44444444-4444-4444-4444-444444444444',
        orderId: sampleOrderId,
        productId: sampleProductId,
        quantity: 3,
        unitPrice: '50.00',
        createdAt: sampleIsoDate,
      },
    ],
    createdAt: sampleIsoDate,
    updatedAt: sampleIsoDate,
  };

  const createMockClient = (): IOrderServiceClient => ({
    createOrder: vi.fn(),
    getOrderById: vi.fn(),
    listOrders: vi.fn(),
  });

  it('createOrder mutation delegates to OrderServiceClient and preserves Decimal / DateTime', async () => {
    const mockClient = createMockClient();
    vi.mocked(mockClient.createOrder).mockResolvedValue(mockOrder);

    const server: ApolloServer<GraphQLContext> = createApolloServer();

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

    const response = await server.executeOperation(
      {
        query: CREATE_ORDER_MUTATION,
        variables: {
          input: {
            customerId: sampleCustomerId,
            items: [
              {
                productId: sampleProductId,
                quantity: 3,
                unitPrice: '50.00',
              },
            ],
          },
        },
      },
      {
        contextValue: { orderServiceClient: mockClient },
      },
    );

    expect(response.body.kind).toBe('single');
    if (response.body.kind === 'single') {
      expect(response.body.singleResult.errors).toBeUndefined();
      const data = response.body.singleResult.data as { createOrder: OrderDto };
      expect(data.createOrder.id).toBe(sampleOrderId);
      expect(data.createOrder.status).toBe('PENDING');
      expect(data.createOrder.totalAmount).toBe('150.00');
      expect(data.createOrder.createdAt).toBe(sampleIsoDate);
      expect(data.createOrder.items[0]?.unitPrice).toBe('50.00');
    }

    expect(mockClient.createOrder).toHaveBeenCalledWith({
      customerId: sampleCustomerId,
      items: [
        {
          productId: sampleProductId,
          quantity: 3,
          unitPrice: '50.00',
        },
      ],
    });
  });

  it('order query returns an order when found', async () => {
    const mockClient = createMockClient();
    vi.mocked(mockClient.getOrderById).mockResolvedValue(mockOrder);

    const server = createApolloServer();
    const ORDER_QUERY = `#graphql
      query GetOrder($id: ID!) {
        order(id: $id) {
          id
          customerId
          status
          totalAmount
        }
      }
    `;

    const response = await server.executeOperation(
      {
        query: ORDER_QUERY,
        variables: { id: sampleOrderId },
      },
      {
        contextValue: { orderServiceClient: mockClient },
      },
    );

    expect(response.body.kind).toBe('single');
    if (response.body.kind === 'single') {
      expect(response.body.singleResult.errors).toBeUndefined();
      const data = response.body.singleResult.data as { order: OrderDto };
      expect(data.order.id).toBe(sampleOrderId);
      expect(data.order.totalAmount).toBe('150.00');
    }
  });

  it('order query returns null when order does not exist', async () => {
    const mockClient = createMockClient();
    vi.mocked(mockClient.getOrderById).mockResolvedValue(null);

    const server = createApolloServer();
    const ORDER_QUERY = `#graphql
      query GetOrder($id: ID!) {
        order(id: $id) {
          id
        }
      }
    `;

    const response = await server.executeOperation(
      {
        query: ORDER_QUERY,
        variables: { id: 'non-existent-order-id' },
      },
      {
        contextValue: { orderServiceClient: mockClient },
      },
    );

    expect(response.body.kind).toBe('single');
    if (response.body.kind === 'single') {
      expect(response.body.singleResult.errors).toBeUndefined();
      const data = response.body.singleResult.data as { order: null };
      expect(data.order).toBeNull();
    }
  });

  it('orders query returns matching orders', async () => {
    const mockClient = createMockClient();
    vi.mocked(mockClient.listOrders).mockResolvedValue([mockOrder]);

    const server = createApolloServer();
    const ORDERS_QUERY = `#graphql
      query GetOrders($customerId: ID) {
        orders(customerId: $customerId) {
          id
          customerId
          status
        }
      }
    `;

    const response = await server.executeOperation(
      {
        query: ORDERS_QUERY,
        variables: { customerId: sampleCustomerId },
      },
      {
        contextValue: { orderServiceClient: mockClient },
      },
    );

    expect(response.body.kind).toBe('single');
    if (response.body.kind === 'single') {
      expect(response.body.singleResult.errors).toBeUndefined();
      const data = response.body.singleResult.data as { orders: OrderDto[] };
      expect(data.orders).toHaveLength(1);
      expect(data.orders[0]?.id).toBe(sampleOrderId);
    }
  });

  it('translates OrderServiceValidationError into BAD_USER_INPUT GraphQLError', async () => {
    const mockClient = createMockClient();
    vi.mocked(mockClient.createOrder).mockRejectedValue(
      new OrderServiceValidationError('Invalid order input data', [
        'items: Order items must not be empty',
      ]),
    );

    const server = createApolloServer();
    const CREATE_ORDER_MUTATION = `#graphql
      mutation CreateOrder($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id
        }
      }
    `;

    const response = await server.executeOperation(
      {
        query: CREATE_ORDER_MUTATION,
        variables: {
          input: {
            customerId: sampleCustomerId,
            items: [],
          },
        },
      },
      {
        contextValue: { orderServiceClient: mockClient },
      },
    );

    expect(response.body.kind).toBe('single');
    if (response.body.kind === 'single') {
      expect(response.body.singleResult.errors).toBeDefined();
      const error = response.body.singleResult.errors![0];
      expect(error?.message).toContain('Invalid order input data');
      expect(error?.extensions?.code).toBe('BAD_USER_INPUT');
      expect(error?.extensions?.issues).toEqual(['items: Order items must not be empty']);
    }
  });

  it('rejects invalid Decimal scalar inputs', async () => {
    const mockClient = createMockClient();
    const server = createApolloServer();

    const CREATE_ORDER_MUTATION = `#graphql
      mutation CreateOrder($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id
        }
      }
    `;

    const response = await server.executeOperation(
      {
        query: CREATE_ORDER_MUTATION,
        variables: {
          input: {
            customerId: sampleCustomerId,
            items: [
              {
                productId: sampleProductId,
                quantity: 1,
                unitPrice: 'invalid-decimal-price',
              },
            ],
          },
        },
      },
      {
        contextValue: { orderServiceClient: mockClient },
      },
    );

    expect(response.body.kind).toBe('single');
    if (response.body.kind === 'single') {
      expect(response.body.singleResult.errors).toBeDefined();
      const error = response.body.singleResult.errors![0];
      expect(error?.message).toContain('Invalid Decimal value');
    }
  });
});
