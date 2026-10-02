// Domain exports
export * from './domain/stock.js';

// Error exports
export * from './errors/stock.errors.js';

// Validation exports
export * from './validation/stock.schema.js';

// Repository exports
export * from './repositories/stock.repository.js';

// Service exports
export * from './services/stock.service.js';

// Database exports
export * from './db/client.js';
export * from './db/migrate.js';
export * from './db/seed.js';

// Configuration exports
export * from './config/env.js';

// gRPC exports
export * from './grpc/proto.loader.js';
export * from './grpc/stock.handler.js';
export * from './grpc/server.js';

// Startup exports
export * from './startup.js';

import { startStockService } from './startup.js';

// Auto-start service if executed directly via Node
if (typeof require !== 'undefined' && require.main === module) {
  const servicePromise = startStockService();

  const shutdown = async (): Promise<void> => {
    console.log('[StockService] Received shutdown signal...');
    const running = await servicePromise;
    await running.close();
    process.exit(0);
  };

  process.on('SIGINT', () => void shutdown());
  process.on('SIGTERM', () => void shutdown());

  servicePromise.catch((err) => {
    console.error('[StockService] Fatal error starting service:', err);
    process.exit(1);
  });
}
