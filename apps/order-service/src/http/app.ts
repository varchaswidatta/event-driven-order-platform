import fastify, { FastifyInstance, FastifyServerOptions } from 'fastify';
import { OrderService } from '../services/order.service.js';
import { registerOrderRoutes } from './routes/order.routes.js';

export interface BuildAppOptions extends FastifyServerOptions {
  orderService: OrderService;
}

export function buildHttpApp(options: BuildAppOptions): FastifyInstance {
  const { orderService, ...fastifyOptions } = options;

  const app = fastify({
    logger: false,
    ...fastifyOptions,
  });

  registerOrderRoutes(app, { orderService });

  return app;
}
