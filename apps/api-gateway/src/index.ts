export * from './config/env.js';
export * from './types/order.types.js';
export * from './clients/order-service.client.js';
export * from './graphql/schema/typeDefs.js';
export * from './graphql/scalars/index.js';
export * from './graphql/resolvers/index.js';
export * from './server.js';

import { startGatewayServer } from './server.js';

// Auto-start server if executed directly via Node
if (typeof require !== 'undefined' && require.main === module) {
  startGatewayServer().catch((err) => {
    console.error('[ApiGateway] Fatal error starting GraphQL server:', err);
    process.exit(1);
  });
}
