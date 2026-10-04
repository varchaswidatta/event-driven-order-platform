import { getDatabasePool } from './db/client.js';
import { runMigrations } from './db/migrate.js';
import { seedStockDatabase } from './db/seed.js';
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

  // 2. Ensure stock database has seed data if empty
  const countRes = await pool.query<{ count: string }>('SELECT COUNT(*) AS count FROM products;');
  if (parseInt(countRes.rows[0]?.count ?? '0', 10) === 0) {
    await seedStockDatabase(pool);
    console.log('[StockService] Seeded default product and stock records');
  }

  // 3. Start gRPC server
  const server = await startGrpcServer(options);

  return {
    server,
    close: server.close,
  };
}
