// Domain exports
export * from './domain/reservation-status.js';
export * from './domain/reservation.js';
export * from './domain/events.js';

// Error exports
export * from './errors/inventory.errors.js';

// Validation exports
export * from './validation/event.schema.js';

// Repository exports
export * from './repositories/inventory.repository.js';
export * from './repositories/outbox.repository.js';

// Service exports
export * from './services/inventory.service.js';

// Database exports
export * from './db/client.js';
export * from './db/migrate.js';

// Configuration exports
export * from './config/env.js';

// Messaging exports
export * from './messaging/kafka/kafka.client.js';
export * from './messaging/kafka/kafka.producer.js';
export * from './messaging/kafka/order-events.consumer.js';
export * from './messaging/outbox.publisher.js';

// gRPC Client exports
export * from './clients/stock-service.client.js';

// Startup exports
export * from './startup.js';

import { startInventoryService } from './startup.js';

// Auto-start service if executed directly via Node
if (typeof require !== 'undefined' && require.main === module) {
  const service = startInventoryService();

  const shutdown = async (): Promise<void> => {
    console.log('[InventoryService] Received shutdown signal...');
    const running = await service;
    await running.close();
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());

  service.catch((err) => {
    console.error('[InventoryService] Fatal error starting service:', err);
    process.exit(1);
  });
}
