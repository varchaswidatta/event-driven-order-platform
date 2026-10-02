import { env } from './config/env.js';
import { getDatabasePool, closeDatabasePool } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { InventoryRepository } from './repositories/inventory.repository.js';
import { InventoryService } from './services/inventory.service.js';
import { createKafkaClient } from './messaging/kafka/kafka.client.js';
import { OrderEventsConsumer } from './messaging/kafka/order-events.consumer.js';

export interface RunningInventoryService {
  consumer: OrderEventsConsumer;
  close: () => Promise<void>;
}

export interface StartInventoryServiceOptions {
  kafkaGroupId?: string;
}

/**
 * Initializes and starts the Inventory Service lifecycle:
 * 1. Connects to PostgreSQL inventory_db
 * 2. Runs database migrations
 * 3. Creates Kafka consumer for order.events
 * 4. Starts consuming messages
 */
export async function startInventoryService(
  options?: StartInventoryServiceOptions,
): Promise<RunningInventoryService> {
  // 1. Connect to PostgreSQL and run migrations
  const pool = getDatabasePool();
  await runMigrations(pool);
  console.log('[InventoryService] Database connected and migrations applied');

  // 2. Initialize service layer
  const repository = new InventoryRepository(pool);
  const inventoryService = new InventoryService(repository);

  // 3. Create and start Kafka consumer
  const kafka = createKafkaClient();
  const consumer = new OrderEventsConsumer(kafka, inventoryService, {
    groupId: options?.kafkaGroupId ?? env.KAFKA_GROUP_ID,
  });

  await consumer.start();

  // 4. Define clean shutdown procedure
  const close = async (): Promise<void> => {
    await consumer.stop();
    await closeDatabasePool();
    console.log('[InventoryService] Shutdown complete');
  };

  return { consumer, close };
}
