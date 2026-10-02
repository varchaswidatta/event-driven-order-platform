import { Pool } from 'pg';
import { getDatabasePool, closeDatabasePool } from './client.js';

export interface SeedProduct {
  id: string;
  sku: string;
  name: string;
  price: string;
  availableQuantity: number;
}

export const DEFAULT_SEED_PRODUCTS: SeedProduct[] = [
  {
    id: '88888888-aaaa-4bbb-8ccc-000000000001',
    sku: 'PROD-A',
    name: 'Wireless Ergonomic Keyboard',
    price: '99.99',
    availableQuantity: 100,
  },
  {
    id: '88888888-aaaa-4bbb-8ccc-000000000002',
    sku: 'PROD-B',
    name: 'Precision Optical Mouse',
    price: '49.50',
    availableQuantity: 50,
  },
  {
    id: '88888888-aaaa-4bbb-8ccc-000000000003',
    sku: 'PROD-C',
    name: 'Noise-Cancelling Headphones',
    price: '199.00',
    availableQuantity: 20,
  },
];

export async function seedStockDatabase(
  pool: Pool,
  products: SeedProduct[] = DEFAULT_SEED_PRODUCTS,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    for (const prod of products) {
      await client.query(
        `INSERT INTO products (id, sku, name, price, created_at, updated_at)
         VALUES ($1, $2, $3, $4, NOW(), NOW())
         ON CONFLICT (id) DO UPDATE SET
           name = EXCLUDED.name,
           price = EXCLUDED.price,
           updated_at = NOW();`,
        [prod.id, prod.sku, prod.name, prod.price],
      );

      await client.query(
        `INSERT INTO stock (product_id, available_quantity, reserved_quantity, updated_at)
         VALUES ($1, $2, 0, NOW())
         ON CONFLICT (product_id) DO UPDATE SET
           available_quantity = EXCLUDED.available_quantity,
           reserved_quantity = 0,
           updated_at = NOW();`,
        [prod.id, prod.availableQuantity],
      );
    }
    await client.query('COMMIT');
    console.log(`[StockService Seed] Successfully seeded ${products.length} products with stock.`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('[StockService Seed] Failed to seed database:', err);
    throw err;
  } finally {
    client.release();
  }
}

// Allow direct execution via CLI
if (process.argv[1]?.endsWith('seed.ts') || process.argv[1]?.endsWith('seed.js')) {
  const pool = getDatabasePool();
  seedStockDatabase(pool)
    .then(() => closeDatabasePool())
    .then(() => process.exit(0))
    .catch(async (err) => {
      await closeDatabasePool();
      console.error(err);
      process.exit(1);
    });
}
