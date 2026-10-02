import fs from 'node:fs';
import path from 'node:path';
import { z } from 'zod';

// Attempt to load .env if available
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
      // Continue if unreadable
    }
    break;
  }
}

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  API_GATEWAY_PORT: z.coerce.number().int().positive().default(4000),
  ORDER_SERVICE_URL: z.string().url().default('http://localhost:4001'),
});

function loadEnv(): z.infer<typeof envSchema> {
  const result = envSchema.safeParse(process.env);
  if (!result.success) {
    const errorDetails = result.error.issues
      .map((issue) => ` - ${issue.path.join('.')}: ${issue.message}`)
      .join('\n');
    throw new Error(`[ApiGateway] Invalid environment configuration:\n${errorDetails}`);
  }
  return result.data;
}

export const env = loadEnv();
export type EnvConfig = z.infer<typeof envSchema>;
