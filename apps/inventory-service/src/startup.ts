import { env } from './config/env.js';
import { getDatabasePool, closeDatabasePool } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { InventoryRepository } from './repositories/inventory.repository.js';
import { InventoryOutboxRepository } from './repositories/outbox.repository.js';
import { InventoryService } from './services/inventory.service.js';
import { createKafkaClient } from './messaging/kafka/kafka.client.js';
import { OrderEventsConsumer } from './messaging/kafka/order-events.consumer.js';
import { KafkaInventoryProducer } from './messaging/kafka/kafka.producer.js';
import { InventoryOutboxPublisher } from './messaging/outbox.publisher.js';
import { StockServiceClient, IStockServiceClient } from './clients/stock-service.client.js';

export interface RunningInventoryService {
  consumer: OrderEventsConsumer;
  outboxPublisher: InventoryOutboxPublisher;
  stockClient: IStockServiceClient;
  close: () => Promise<void>;
}

export interface StartInventoryServiceOptions {
  kafkaGroupId?: string;
  stockClient?: IStockServiceClient;
  connectStockClient?: boolean;
}

/**
 * Initializes and starts the Inventory Service lifecycle:
 * 1. Connects to PostgreSQL inventory_db
 * 2. Runs database migrations
 * 3. Creates Stock Service gRPC client
 * 4. Creates Kafka consumer for order.events
 * 5. Creates Kafka producer + outbox publisher for inventory.events
 * 6. Starts consuming messages and publishing outbox events
 */
export async function startInventoryService(
  options?: StartInventoryServiceOptions,
): Promise<RunningInventoryService> {
  // 1. Connect to PostgreSQL and run migrations
  const pool = getDatabasePool();
  await runMigrations(pool);
  console.log('[InventoryService] Database connected and migrations applied');

  // 2. Initialize Stock Service gRPC client
  const stockClient = options?.stockClient ?? new StockServiceClient();

  // 3. Initialize repositories with outbox support
  const outboxRepository = new InventoryOutboxRepository(pool);
  const repository = new InventoryRepository(pool, outboxRepository);
  const inventoryService = new InventoryService(repository, stockClient);

  // 4. Create and start Kafka consumer
  const kafka = createKafkaClient();
  const consumer = new OrderEventsConsumer(kafka, inventoryService, {
    groupId: options?.kafkaGroupId ?? env.KAFKA_GROUP_ID,
  });

  await consumer.start();

  // 5. Create and start outbox publisher for inventory.events
  const kafkaProducer = new KafkaInventoryProducer(kafka);
  const outboxPublisher = new InventoryOutboxPublisher(outboxRepository, kafkaProducer);
  await outboxPublisher.start();
  console.log('[InventoryService] Outbox publisher started');

  // 6. Define clean shutdown procedure
  const close = async (): Promise<void> => {
    await outboxPublisher.stop();
    await consumer.stop();
    stockClient.close();
    await closeDatabasePool();
    console.log('[InventoryService] Shutdown complete');
  };

  return { consumer, outboxPublisher, stockClient, close };
}
