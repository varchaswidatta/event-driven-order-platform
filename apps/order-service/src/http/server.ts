import { FastifyInstance } from 'fastify';
import { env } from '../config/env.js';
import { getDatabasePool, closeDatabasePool } from '../db/client.js';
import { OrderRepository } from '../repositories/order.repository.js';
import { OrderService } from '../services/order.service.js';
import { buildHttpApp } from './app.js';

export interface RunningServer {
  app: FastifyInstance;
  port: number;
  close: () => Promise<void>;
}

export async function startHttpServer(customPort?: number): Promise<RunningServer> {
  const port = customPort ?? env.ORDER_SERVICE_PORT;
  const pool = getDatabasePool();
  const repository = new OrderRepository(pool);
  const orderService = new OrderService(repository);

  const app = buildHttpApp({ orderService });

  await app.listen({ port, host: '0.0.0.0' });
  console.log(`[OrderService] HTTP server running on port ${port}`);

  const close = async (): Promise<void> => {
    await app.close();
    await closeDatabasePool();
  };

  return { app, port, close };
}
