import * as grpc from '@grpc/grpc-js';
import { IStockService } from '../services/stock.service.js';
import { InvalidStockRequestError } from '../errors/stock.errors.js';

export interface ProtoStockItem {
  product_id: string;
  quantity: number;
}

export interface ProtoReserveStockRequest {
  order_id: string;
  items: ProtoStockItem[];
}

export interface ProtoReserveStockResponse {
  success: boolean;
  reservation_id: string;
  failure_reason: string;
}

export interface ProtoReleaseStockRequest {
  reservation_id: string;
}

export interface ProtoReleaseStockResponse {
  success: boolean;
}

export class StockGrpcHandler {
  constructor(private readonly stockService: IStockService) {}

  /**
   * Implements the ReserveStock RPC method.
   * Handles incoming gRPC requests, validates payloads, delegates to domain service,
   * and returns protobuf-compatible responses.
   */
  async reserveStock(
    call: grpc.ServerUnaryCall<ProtoReserveStockRequest, ProtoReserveStockResponse>,
    callback: grpc.sendUnaryData<ProtoReserveStockResponse>,
  ): Promise<void> {
    const req = call.request;

    console.log(`[StockService] ReserveStock request received for order: ${req?.order_id}`);

    try {
      // Map protobuf snake_case fields to internal domain shape
      const rawInput = {
        orderId: req?.order_id,
        items: req?.items?.map((i) => ({
          productId: i.product_id,
          quantity: i.quantity,
        })),
      };

      const result = await this.stockService.reserveStock(rawInput);

      if (result.success) {
        console.log(
          `[StockService] Stock reservation succeeded: reservationId=${result.reservationId} (orderId=${req.order_id}, alreadyExisted=${result.alreadyExisted})`,
        );
        callback(null, {
          success: true,
          reservation_id: result.reservationId ?? '',
          failure_reason: '',
        });
      } else {
        console.warn(
          `[StockService] Stock reservation business failure: reason=${result.failureReason} (orderId=${req.order_id})`,
        );
        callback(null, {
          success: false,
          reservation_id: '',
          failure_reason: result.failureReason ?? 'INSUFFICIENT_STOCK',
        });
      }
    } catch (err) {
      if (err instanceof InvalidStockRequestError) {
        console.warn(
          `[StockService] Invalid ReserveStock request: ${err.message} (orderId=${req?.order_id})`,
        );
        callback(null, {
          success: false,
          reservation_id: '',
          failure_reason: 'INVALID_REQUEST',
        });
        return;
      }

      // Infrastructure / database error: return gRPC error to prevent false client success
      console.error('[StockService] Internal error during ReserveStock:', err);
      callback({
        code: grpc.status.INTERNAL,
        message: 'Internal server error processing stock reservation',
      });
    }
  }

  /**
   * Implements the ReleaseStock RPC method.
   * Explicitly returns UNIMPLEMENTED in Phase 6 as full compensation/saga workflow is scheduled for Phase 7.
   */
  async releaseStock(
    _call: grpc.ServerUnaryCall<ProtoReleaseStockRequest, ProtoReleaseStockResponse>,
    callback: grpc.sendUnaryData<ProtoReleaseStockResponse>,
  ): Promise<void> {
    console.warn(
      '[StockService] ReleaseStock called but is UNIMPLEMENTED in Phase 6 (scheduled for Phase 7 Saga compensation)',
    );
    callback({
      code: grpc.status.UNIMPLEMENTED,
      message: 'ReleaseStock is not implemented in Phase 6 (deferred to Phase 7 Saga compensation)',
    });
  }
}
