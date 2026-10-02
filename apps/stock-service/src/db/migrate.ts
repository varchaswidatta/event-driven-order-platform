import fs from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';
import { getDatabasePool, closeDatabasePool } from './client.js';

export interface MigrationResult {
  applied: string[];
  skipped: string[];
}

export async function runMigrations(pool: Pool): Promise<MigrationResult> {
  const client = await pool.connect();
  const applied: string[] = [];
  const skipped: string[] = [];

  try {
    // 1. Ensure migrations tracking table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL UNIQUE,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    // 2. Fetch already applied migrations
    const existingRes = await client.query<{ name: string }>(
      'SELECT name FROM schema_migrations ORDER BY id ASC;',
    );
    const appliedSet = new Set(existingRes.rows.map((r) => r.name));

    // 3. Locate migrations directory
    const candidates = [
      path.resolve(process.cwd(), 'src/db/migrations'),
      path.resolve(process.cwd(), 'apps/stock-service/src/db/migrations'),
      path.resolve(__dirname, 'migrations'),
      path.resolve(__dirname, '../../src/db/migrations'),
    ];

    const migrationsDir = candidates.find((dir) => fs.existsSync(dir));
    if (!migrationsDir) {
      throw new Error(
        `Migrations directory not found. Checked candidate paths:\n${candidates.join('\n')}`,
      );
    }

    // 4. Read and sort SQL migration files deterministically
    const files = fs
      .readdirSync(migrationsDir)
      .filter((file) => file.endsWith('.sql'))
      .sort((a, b) => a.localeCompare(b));

    // 5. Apply pending migrations within transactions
    for (const file of files) {
      if (appliedSet.has(file)) {
        skipped.push(file);
        continue;
      }

      const filePath = path.join(migrationsDir, file);
      const sql = fs.readFileSync(filePath, 'utf-8');

      console.log(`[StockService Migration] Applying migration: ${file}...`);
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1);', [file]);
        await client.query('COMMIT');
        applied.push(file);
        console.log(`[StockService Migration] Successfully applied: ${file}`);
      } catch (migrationError) {
        await client.query('ROLLBACK');
        console.error(`[StockService Migration] Failed applying migration: ${file}`);
        throw migrationError;
      }
    }

    return { applied, skipped };
  } finally {
    client.release();
  }
}

// Allow direct execution via CLI
if (process.argv[1]?.endsWith('migrate.ts') || process.argv[1]?.endsWith('migrate.js')) {
  const pool = getDatabasePool();
  runMigrations(pool)
    .then((result) => {
      console.log(
        `[StockService Migration] Complete. Applied: ${result.applied.length}, Skipped: ${result.skipped.length}`,
      );
      return closeDatabasePool();
    })
    .then(() => process.exit(0))
    .catch(async (error) => {
      console.error('[StockService Migration] Migration failed with error:', error);
      await closeDatabasePool();
      process.exit(1);
    });
}
