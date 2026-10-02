import { FastifyInstance } from 'fastify';
import { env } from '../config/env.js';
import { getDatabasePool, closeDatabasePool } from '../db/client.js';
import { OrderRepository } from '../repositories/order.repository.js';
import { OutboxRepository } from '../repositories/outbox.repository.js';
import { OrderService } from '../services/order.service.js';
import { buildHttpApp } from './app.js';
import { KafkaOrderProducer } from '../messaging/kafka/kafka.producer.js';
import { OutboxPublisher } from '../messaging/outbox.publisher.js';

export interface ServerOptions {
  port?: number;
  startPublisher?: boolean;
}

export interface RunningServer {
  app: FastifyInstance;
  port: number;
  publisher?: OutboxPublisher;
  close: () => Promise<void>;
}

export async function startHttpServer(
  portOrOptions?: number | ServerOptions,
): Promise<RunningServer> {
  const options: ServerOptions =
    typeof portOrOptions === 'number'
      ? { port: portOrOptions, startPublisher: true }
      : {
          port: portOrOptions?.port ?? env.ORDER_SERVICE_PORT,
          startPublisher: portOrOptions?.startPublisher ?? true,
        };

  const port = options.port!;
  const pool = getDatabasePool();
  const outboxRepository = new OutboxRepository(pool);
  const repository = new OrderRepository(pool, outboxRepository);
  const orderService = new OrderService(repository);

  const app = buildHttpApp({ orderService });

  await app.listen({ port, host: '0.0.0.0' });
  console.log(`[OrderService] HTTP server running on port ${port}`);

  let publisher: OutboxPublisher | undefined;
  if (options.startPublisher) {
    try {
      const kafkaProducer = new KafkaOrderProducer();
      publisher = new OutboxPublisher(outboxRepository, kafkaProducer);
      await publisher.start();
      console.log('[OrderService] OutboxPublisher started successfully');
    } catch (err) {
      console.error('[OrderService] Failed to start OutboxPublisher:', err);
    }
  }

  const close = async (): Promise<void> => {
    if (publisher) {
      await publisher.stop();
    }
    await app.close();
    await closeDatabasePool();
  };

  return { app, port, publisher, close };
}
