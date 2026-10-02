import * as grpc from '@grpc/grpc-js';
import { Pool } from 'pg';
import { env } from '../config/env.js';
import { getDatabasePool, closeDatabasePool } from '../db/client.js';
import { StockRepository } from '../repositories/stock.repository.js';
import { StockService } from '../services/stock.service.js';
import { loadStockProtoDefinition } from './proto.loader.js';
import { StockGrpcHandler } from './stock.handler.js';

export interface RunningGrpcServer {
  server: grpc.Server;
  port: number;
  close: () => Promise<void>;
}

export interface StartGrpcServerOptions {
  port?: number;
  host?: string;
  stockService?: StockService;
  pool?: Pool;
}

export async function startGrpcServer(
  portOrOptions?: number | StartGrpcServerOptions,
): Promise<RunningGrpcServer> {
  const options: StartGrpcServerOptions =
    typeof portOrOptions === 'number'
      ? { port: portOrOptions }
      : {
          port: portOrOptions?.port ?? env.STOCK_SERVICE_GRPC_PORT,
          host: portOrOptions?.host ?? env.STOCK_SERVICE_GRPC_HOST,
          stockService: portOrOptions?.stockService,
          pool: portOrOptions?.pool,
        };

  const port = options.port ?? 50051;
  const host = options.host ?? '0.0.0.0';

  const pool = options.pool ?? getDatabasePool();
  const repository = new StockRepository(pool);
  const stockService = options.stockService ?? new StockService(repository);
  const handler = new StockGrpcHandler(stockService);

  const protoDef = loadStockProtoDefinition();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stockPackage = (protoDef as any).stock;

  const server = new grpc.Server();

  server.addService(stockPackage.StockService.service, {
    ReserveStock: handler.reserveStock.bind(handler),
    ReleaseStock: handler.releaseStock.bind(handler),
  });

  const bindAddress = `${host}:${port}`;

  const boundPort = await new Promise<number>((resolve, reject) => {
    server.bindAsync(bindAddress, grpc.ServerCredentials.createInsecure(), (err, actualPort) => {
      if (err) {
        reject(err);
      } else {
        resolve(actualPort);
      }
    });
  });

  console.log(`[StockService] gRPC server running at ${host}:${boundPort}`);

  const close = async (): Promise<void> => {
    await new Promise<void>((resolve) => {
      server.tryShutdown((err) => {
        if (err) {
          console.warn('[StockService] tryShutdown failed, forcing server kill:', err);
          server.forceShutdown();
        }
        resolve();
      });
    });
    if (!options.pool) {
      await closeDatabasePool();
    }
    console.log('[StockService] gRPC server stopped');
  };

  return { server, port: boundPort, close };
}
