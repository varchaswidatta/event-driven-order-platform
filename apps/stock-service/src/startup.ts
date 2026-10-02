import { getDatabasePool } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { startGrpcServer, RunningGrpcServer, StartGrpcServerOptions } from './grpc/server.js';

export interface RunningStockService {
  server: RunningGrpcServer;
  close: () => Promise<void>;
}

export async function startStockService(
  options?: StartGrpcServerOptions,
): Promise<RunningStockService> {
  // 1. Apply database migrations to stock_db
  const pool = getDatabasePool();
  await runMigrations(pool);
  console.log('[StockService] Database migrations verified/applied');

  // 2. Start gRPC server
  const server = await startGrpcServer(options);

  return {
    server,
    close: server.close,
  };
}
