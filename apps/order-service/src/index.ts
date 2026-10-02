// Domain exports
export * from './domain/order-status.js';
export * from './domain/order.js';
export * from './domain/money.js';

// Error exports
export * from './errors/order.errors.js';

// Validation exports
export * from './validation/order.schema.js';

// Repository exports
export * from './repositories/order.repository.js';

// Service exports
export * from './services/order.service.js';

// Database exports
export * from './db/client.js';
export * from './db/migrate.js';

// Configuration exports
export * from './config/env.js';

// HTTP server exports
export * from './http/app.js';
export * from './http/server.js';
export * from './http/routes/order.routes.js';

import { startHttpServer } from './http/server.js';

// Auto-start server if executed directly via Node
if (typeof require !== 'undefined' && require.main === module) {
  startHttpServer().catch((err) => {
    console.error('[OrderService] Fatal error starting server:', err);
    process.exit(1);
  });
}
