import { GraphQLError } from 'graphql';
import {
  IOrderServiceClient,
  OrderServiceValidationError,
} from '../../clients/order-service.client.js';
import { CreateOrderInput, OrderDto } from '../../types/order.types.js';

export interface GraphQLContext {
  orderServiceClient: IOrderServiceClient;
}

export const orderResolvers = {
  Query: {
    order: async (
      _parent: unknown,
      args: { id: string },
      context: GraphQLContext,
    ): Promise<OrderDto | null> => {
      try {
        return await context.orderServiceClient.getOrderById(args.id);
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to query order';
        throw new GraphQLError(message, {
          extensions: { code: 'INTERNAL_SERVER_ERROR' },
        });
      }
    },

    orders: async (
      _parent: unknown,
      args: { customerId?: string },
      context: GraphQLContext,
    ): Promise<OrderDto[]> => {
      try {
        return await context.orderServiceClient.listOrders(args.customerId);
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to query orders';
        throw new GraphQLError(message, {
          extensions: { code: 'INTERNAL_SERVER_ERROR' },
        });
      }
    },
  },

  Mutation: {
    createOrder: async (
      _parent: unknown,
      args: { input: CreateOrderInput },
      context: GraphQLContext,
    ): Promise<OrderDto> => {
      try {
        return await context.orderServiceClient.createOrder(args.input);
      } catch (err) {
        if (err instanceof OrderServiceValidationError) {
          throw new GraphQLError(err.message, {
            extensions: {
              code: 'BAD_USER_INPUT',
              issues: err.issues ?? [],
            },
          });
        }
        const message = err instanceof Error ? err.message : 'Failed to create order';
        throw new GraphQLError(message, {
          extensions: { code: 'INTERNAL_SERVER_ERROR' },
        });
      }
    },
  },
};
