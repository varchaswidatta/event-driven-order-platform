import { FastifyInstance } from 'fastify';
import { OrderService } from '../../services/order.service.js';
import { InvalidOrderInputError, OrderNotFoundError } from '../../errors/order.errors.js';
import { CreateOrderInput } from '../../domain/order.js';

interface OrderRouteOptions {
  orderService: OrderService;
}

export const registerOrderRoutes = async (
  app: FastifyInstance,
  options: OrderRouteOptions,
): Promise<void> => {
  const { orderService } = options;

  // Health check endpoint
  app.get('/health', async () => {
    return { status: 'ok', service: 'order-service' };
  });

  // POST /orders - Creates a new order
  app.post<{ Body: CreateOrderInput }>('/orders', async (request, reply) => {
    try {
      const order = await orderService.createOrder(request.body);
      return reply.status(201).send(order);
    } catch (error) {
      if (error instanceof InvalidOrderInputError) {
        return reply.status(400).send({
          error: 'VALIDATION_ERROR',
          message: error.message,
          issues: error.issues,
        });
      }
      const message = error instanceof Error ? error.message : 'Unknown internal error';
      return reply.status(500).send({
        error: 'INTERNAL_SERVER_ERROR',
        message,
      });
    }
  });

  // GET /orders/:id - Retrieves order by ID
  app.get<{ Params: { id: string } }>('/orders/:id', async (request, reply) => {
    const { id } = request.params;
    try {
      const order = await orderService.getOrderById(id);
      return reply.status(200).send(order);
    } catch (error) {
      if (error instanceof OrderNotFoundError) {
        return reply.status(404).send({
          error: 'ORDER_NOT_FOUND',
          message: error.message,
        });
      }
      const message = error instanceof Error ? error.message : 'Unknown internal error';
      return reply.status(500).send({
        error: 'INTERNAL_SERVER_ERROR',
        message,
      });
    }
  });

  // GET /orders - Retrieves orders, optionally filtered by customerId
  app.get<{ Querystring: { customerId?: string } }>('/orders', async (request, reply) => {
    const { customerId } = request.query;
    try {
      const orders = await orderService.getOrders(customerId);
      return reply.status(200).send(orders);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown internal error';
      return reply.status(500).send({
        error: 'INTERNAL_SERVER_ERROR',
        message,
      });
    }
  });
};
