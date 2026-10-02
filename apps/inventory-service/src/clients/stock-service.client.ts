import path from 'node:path';
import fs from 'node:fs';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { env } from '../config/env.js';
import {
  StockServiceUnavailableError,
  StockServiceTimeoutError,
  StockServiceError,
} from '../errors/inventory.errors.js';

export interface StockReservationResponse {
  success: boolean;
  reservationId?: string;
  failureReason?: string;
}

export interface StockServiceClientOptions {
  host?: string;
  port?: number;
  timeoutMs?: number;
}

export interface IStockServiceClient {
  reserveStock(
    orderId: string,
    items: Array<{ productId: string; quantity: number }>,
    timeoutMs?: number,
  ): Promise<StockReservationResponse>;
  close(): void;
}

function getStockProtoPath(): string {
  const candidates = [
    path.resolve(process.cwd(), 'proto/stock.proto'),
    path.resolve(process.cwd(), '../../proto/stock.proto'),
    path.resolve(__dirname, '../../../proto/stock.proto'),
    path.resolve(__dirname, '../../../../proto/stock.proto'),
  ];

  const found = candidates.find((p) => fs.existsSync(p));
  if (!found) {
    throw new Error(
      `stock.proto not found for Inventory gRPC client. Checked locations:\n${candidates.join('\n')}`,
    );
  }
  return found;
}

export class StockServiceClient implements IStockServiceClient {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private readonly client: any;
  private readonly defaultTimeoutMs: number;

  constructor(options?: StockServiceClientOptions) {
    const host = options?.host ?? env.STOCK_SERVICE_GRPC_HOST;
    const port = options?.port ?? env.STOCK_SERVICE_GRPC_PORT;
    this.defaultTimeoutMs = options?.timeoutMs ?? 5000;

    const protoPath = getStockProtoPath();
    const packageDefinition = protoLoader.loadSync(protoPath, {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const proto = grpc.loadPackageDefinition(packageDefinition) as any;
    const StockServiceDefinition = proto.stock.StockService;

    const target = `${host}:${port}`;
    this.client = new StockServiceDefinition(target, grpc.credentials.createInsecure());
  }

  /**
   * Calls the StockService.ReserveStock RPC synchronously with a deadline.
   * Distinguishes business failures (INSUFFICIENT_STOCK) from infrastructure failures (UNAVAILABLE, TIMEOUT).
   *
   * @throws StockServiceTimeoutError if deadline expires
   * @throws StockServiceUnavailableError if service cannot be reached
   * @throws StockServiceError if server encounters an internal error
   */
  async reserveStock(
    orderId: string,
    items: Array<{ productId: string; quantity: number }>,
    timeoutMs?: number,
  ): Promise<StockReservationResponse> {
    const timeout = timeoutMs ?? this.defaultTimeoutMs;
    const deadline = new Date(Date.now() + timeout);

    const requestPayload = {
      order_id: orderId,
      items: items.map((i) => ({
        product_id: i.productId,
        quantity: i.quantity,
      })),
    };

    return new Promise<StockReservationResponse>((resolve, reject) => {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      this.client.ReserveStock(requestPayload, { deadline }, (err: any, response: any) => {
        if (err) {
          if (err.code === grpc.status.DEADLINE_EXCEEDED) {
            return reject(
              new StockServiceTimeoutError(
                `ReserveStock call timed out after ${timeout}ms: ${err.details ?? err.message}`,
              ),
            );
          }
          if (err.code === grpc.status.UNAVAILABLE) {
            return reject(
              new StockServiceUnavailableError(
                `Stock Service is unreachable: ${err.details ?? err.message}`,
              ),
            );
          }
          return reject(
            new StockServiceError(`gRPC error (code ${err.code}): ${err.details ?? err.message}`),
          );
        }

        resolve({
          success: Boolean(response.success),
          reservationId: response.reservation_id || undefined,
          failureReason: response.failure_reason || undefined,
        });
      });
    });
  }

  close(): void {
    if (this.client && typeof this.client.close === 'function') {
      this.client.close();
    }
  }
}
