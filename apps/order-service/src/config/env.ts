import fs from 'node:fs';
import path from 'node:path';
import { z } from 'zod';

// Attempt to load .env if available without throwing when missing
const envCandidates = [
  path.resolve(process.cwd(), '.env'),
  path.resolve(process.cwd(), '../../.env'),
];

for (const candidate of envCandidates) {
  if (fs.existsSync(candidate)) {
    try {
      if (typeof process.loadEnvFile === 'function') {
        process.loadEnvFile(candidate);
      }
    } catch {
      // Continue if unreadable or already set
    }
    break;
  }
}

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  POSTGRES_HOST: z.string().min(1, 'POSTGRES_HOST cannot be empty').default('localhost'),
  POSTGRES_PORT: z.coerce.number().int().positive().default(5432),
  POSTGRES_USER: z.string().min(1, 'POSTGRES_USER cannot be empty').default('app'),
  POSTGRES_PASSWORD: z.string().default('local_password'),
  ORDER_DB_NAME: z.string().min(1, 'ORDER_DB_NAME cannot be empty').default('order_db'),
  ORDER_SERVICE_PORT: z.coerce.number().int().positive().default(4001),
});

function loadEnv(): z.infer<typeof envSchema> {
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    const errorDetails = result.error.issues
      .map((issue) => ` - ${issue.path.join('.')}: ${issue.message}`)
      .join('\n');
    throw new Error(`[OrderService] Invalid environment configuration:\n${errorDetails}`);
  }
  return result.data;
}

export const env = loadEnv();
export type EnvConfig = z.infer<typeof envSchema>;
