import { Pool, PoolConfig } from 'pg';
import { env } from '../config/env.js';

export function createDatabasePool(overrideConfig?: Partial<PoolConfig>): Pool {
  return new Pool({
    host: env.POSTGRES_HOST,
    port: env.POSTGRES_PORT,
    user: env.POSTGRES_USER,
    password: env.POSTGRES_PASSWORD,
    database: env.INVENTORY_DB_NAME,
    max: 10, // Sane pool size for local development and microservice instance
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    ...overrideConfig,
  });
}

// Singleton pool instance for application lifecycle
let defaultPool: Pool | null = null;

export function getDatabasePool(): Pool {
  if (!defaultPool) {
    defaultPool = createDatabasePool();
  }
  return defaultPool;
}

export async function closeDatabasePool(): Promise<void> {
  if (defaultPool) {
    await defaultPool.end();
    defaultPool = null;
  }
}
