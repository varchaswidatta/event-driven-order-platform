import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import crypto from 'node:crypto';
import * as grpc from '@grpc/grpc-js';
import { createDatabasePool } from '../../src/db/client.js';
import { runMigrations } from '../../src/db/migrate.js';
import { StockRepository } from '../../src/repositories/stock.repository.js';
import { startGrpcServer, RunningGrpcServer } from '../../src/grpc/server.js';
import { loadStockProtoDefinition } from '../../src/grpc/proto.loader.js';
import { StockService } from '../../src/services/stock.service.js';

describe('Stock gRPC Server Integration Tests', () => {
  let pool: Pool;
  let repository: StockRepository;
  let grpcServer: RunningGrpcServer;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let client: any;

  beforeAll(async () => {
    pool = createDatabasePool({ max: 10 });
    await runMigrations(pool);
    repository = new StockRepository(pool);

    // Start gRPC server on dynamic port (0)
    grpcServer = await startGrpcServer({
      port: 0,
      host: '127.0.0.1',
      pool,
    });

    // Create gRPC client
    const protoDef = loadStockProtoDefinition();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const StockServiceDef = (protoDef as any).stock.StockService;
    client = new StockServiceDef(`127.0.0.1:${grpcServer.port}`, grpc.credentials.createInsecure());
  });

  beforeEach(async () => {
    await pool.query('DELETE FROM stock_reservation_items;');
    await pool.query('DELETE FROM stock_reservations;');
    await pool.query('DELETE FROM stock;');
    await pool.query('DELETE FROM products;');
  });

  afterAll(async () => {
    if (client) {
      client.close();
    }
    if (grpcServer) {
      await grpcServer.close();
    }
    if (pool) {
      await pool.query('DELETE FROM stock_reservation_items;').catch(() => {});
      await pool.query('DELETE FROM stock_reservations;').catch(() => {});
      await pool.query('DELETE FROM stock;').catch(() => {});
      await pool.query('DELETE FROM products;').catch(() => {});
      await pool.end().catch(() => {});
    }
  });

  it('handles successful ReserveStock request and returns valid reservation_id', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-GRPC-1',
      name: 'gRPC Product 1',
      price: '15.50',
      availableQuantity: 25,
    });

    const orderId = crypto.randomUUID();
    const req = {
      order_id: orderId,
      items: [{ product_id: product.id, quantity: 5 }],
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await new Promise((resolve, reject) => {
      client.ReserveStock(req, (err: unknown, res: unknown) => {
        if (err) reject(err);
        else resolve(res);
      });
    });

    expect(response.success).toBe(true);
    expect(response.reservation_id).toBeDefined();
    expect(response.reservation_id.length).toBeGreaterThan(0);
    expect(response.failure_reason).toBe('');

    // Check database mutation
    const stock = await repository.getStock(product.id);
    expect(stock!.availableQuantity).toBe(20);
    expect(stock!.reservedQuantity).toBe(5);
  });

  it('handles insufficient stock and returns failure response without mutating stock', async () => {
    const { product } = await repository.createProductWithStock({
      sku: 'SKU-GRPC-2',
      name: 'gRPC Product 2',
      price: '20.00',
      availableQuantity: 2,
    });

    const orderId = crypto.randomUUID();
    const req = {
      order_id: orderId,
      items: [{ product_id: product.id, quantity: 10 }],
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await new Promise((resolve, reject) => {
      client.ReserveStock(req, (err: unknown, res: unknown) => {
        if (err) reject(err);
        else resolve(res);
      });
    });

    expect(response.success).toBe(false);
    expect(response.reservation_id).toBe('');
    expect(response.failure_reason).toBe('INSUFFICIENT_STOCK');

    const stock = await repository.getStock(product.id);
    expect(stock!.availableQuantity).toBe(2);
    expect(stock!.reservedQuantity).toBe(0);
  });

  it('returns PRODUCT_NOT_FOUND when requesting non-existent product UUID', async () => {
    const nonExistentId = crypto.randomUUID();
    const orderId = crypto.randomUUID();
    const req = {
      order_id: orderId,
      items: [{ product_id: nonExistentId, quantity: 2 }],
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await new Promise((resolve, reject) => {
      client.ReserveStock(req, (err: unknown, res: unknown) => {
        if (err) reject(err);
        else resolve(res);
      });
    });

    expect(response.success).toBe(false);
    expect(response.failure_reason).toBe('PRODUCT_NOT_FOUND');
  });

  it('returns INVALID_REQUEST for malformed or non-positive quantity payloads', async () => {
    const orderId = crypto.randomUUID();
    const req = {
      order_id: orderId,
      items: [{ product_id: crypto.randomUUID(), quantity: -5 }], // Negative quantity
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await new Promise((resolve, reject) => {
      client.ReserveStock(req, (err: unknown, res: unknown) => {
        if (err) reject(err);
        else resolve(res);
      });
    });

    expect(response.success).toBe(false);
    expect(response.failure_reason).toBe('INVALID_REQUEST');
  });

  it('returns INVALID_REQUEST for empty items array', async () => {
    const orderId = crypto.randomUUID();
    const req = {
      order_id: orderId,
      items: [],
    };

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response: any = await new Promise((resolve, reject) => {
      client.ReserveStock(req, (err: unknown, res: unknown) => {
        if (err) reject(err);
        else resolve(res);
      });
    });

    expect(response.success).toBe(false);
    expect(response.failure_reason).toBe('INVALID_REQUEST');
  });

  it('returns gRPC INTERNAL error status and does not leak database errors when service throws unexpectedly', async () => {
    // Create a mock service that throws an unexpected internal error
    const brokenService = {
      reserveStock: async () => {
        throw new Error('FATAL: raw database connection refused at 10.0.0.1:5432');
      },
    } as unknown as StockService;

    const brokenServer = await startGrpcServer({
      port: 0,
      host: '127.0.0.1',
      stockService: brokenService,
      pool,
    });

    const protoDef = loadStockProtoDefinition();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const StockServiceDef = (protoDef as any).stock.StockService;
    const brokenClient = new StockServiceDef(
      `127.0.0.1:${brokenServer.port}`,
      grpc.credentials.createInsecure(),
    );

    try {
      await new Promise((resolve, reject) => {
        brokenClient.ReserveStock(
          {
            order_id: crypto.randomUUID(),
            items: [{ product_id: crypto.randomUUID(), quantity: 1 }],
          },
          (err: grpc.ServiceError, res: unknown) => {
            if (err) reject(err);
            else resolve(res);
          },
        );
      });
      expect.fail('Expected gRPC INTERNAL error to be thrown');
    } catch (err: unknown) {
      const grpcErr = err as grpc.ServiceError;
      expect(grpcErr.code).toBe(grpc.status.INTERNAL);
      // Ensure raw error details (such as passwords, SQL, internal IPs) are NOT leaked
      expect(grpcErr.message).not.toContain('10.0.0.1:5432');
      expect(grpcErr.details).toBe('Internal server error processing stock reservation');
    } finally {
      brokenClient.close();
      await brokenServer.close();
    }
  });

  it('fails with DEADLINE_EXCEEDED when deadline is already in the past', async () => {
    const expiredDeadline = new Date(Date.now() - 5000);
    const req = {
      order_id: crypto.randomUUID(),
      items: [{ product_id: crypto.randomUUID(), quantity: 1 }],
    };

    try {
      await new Promise((resolve, reject) => {
        client.ReserveStock(
          req,
          { deadline: expiredDeadline },
          (err: grpc.ServiceError, res: unknown) => {
            if (err) reject(err);
            else resolve(res);
          },
        );
      });
      expect.fail('Expected DEADLINE_EXCEEDED error');
    } catch (err: unknown) {
      const grpcErr = err as grpc.ServiceError;
      expect(grpcErr.code).toBe(grpc.status.DEADLINE_EXCEEDED);
    }
  });

  it('fails with UNAVAILABLE when connecting to an inactive port', async () => {
    const protoDef = loadStockProtoDefinition();
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const StockServiceDef = (protoDef as any).stock.StockService;
    // Port 49999 has no running server
    const deadClient = new StockServiceDef('127.0.0.1:49999', grpc.credentials.createInsecure());

    try {
      await new Promise((resolve, reject) => {
        deadClient.ReserveStock(
          {
            order_id: crypto.randomUUID(),
            items: [{ product_id: crypto.randomUUID(), quantity: 1 }],
          },
          { deadline: new Date(Date.now() + 1000) },
          (err: grpc.ServiceError, res: unknown) => {
            if (err) reject(err);
            else resolve(res);
          },
        );
      });
      expect.fail('Expected UNAVAILABLE or DEADLINE_EXCEEDED error');
    } catch (err: unknown) {
      const grpcErr = err as grpc.ServiceError;
      expect([grpc.status.UNAVAILABLE, grpc.status.DEADLINE_EXCEEDED]).toContain(grpcErr.code);
    } finally {
      deadClient.close();
    }
  });

  it('returns UNIMPLEMENTED status for ReleaseStock RPC in Phase 6', async () => {
    const req = { reservation_id: crypto.randomUUID() };

    try {
      await new Promise((resolve, reject) => {
        client.ReleaseStock(req, (err: grpc.ServiceError, res: unknown) => {
          if (err) reject(err);
          else resolve(res);
        });
      });
      expect.fail('Expected UNIMPLEMENTED error');
    } catch (err: unknown) {
      const grpcErr = err as grpc.ServiceError;
      expect(grpcErr.code).toBe(grpc.status.UNIMPLEMENTED);
      expect(grpcErr.details).toContain('ReleaseStock is not implemented in Phase 6');
    }
  });
});
