import { describe, it, expect, vi } from 'vitest';
import { StockService } from '../../src/services/stock.service.js';
import { IStockRepository } from '../../src/repositories/stock.repository.js';
import { InvalidStockRequestError } from '../../src/errors/stock.errors.js';

describe('StockService Unit Tests', () => {
  const dummyRepo: IStockRepository = {
    reserveStock: vi.fn(),
    getStock: vi.fn(),
    getProduct: vi.fn(),
    getReservationByOrderId: vi.fn(),
    createProductWithStock: vi.fn(),
  };

  const service = new StockService(dummyRepo);

  const validOrderId = '11111111-1111-4111-8111-111111111111';
  const validProductId1 = '22222222-2222-4222-8222-222222222222';
  const validProductId2 = '33333333-3333-4333-8333-333333333333';

  it('validates and delegates a valid reservation request to repository', async () => {
    vi.mocked(dummyRepo.reserveStock).mockResolvedValueOnce({
      success: true,
      reservationId: '99999999-9999-4999-8999-999999999999',
      alreadyExisted: false,
    });

    const result = await service.reserveStock({
      orderId: validOrderId,
      items: [
        { productId: validProductId1, quantity: 2 },
        { productId: validProductId2, quantity: 5 },
      ],
    });

    expect(result.success).toBe(true);
    expect(result.reservationId).toBe('99999999-9999-4999-8999-999999999999');
    expect(dummyRepo.reserveStock).toHaveBeenCalledWith({
      orderId: validOrderId,
      items: [
        { productId: validProductId1, quantity: 2 },
        { productId: validProductId2, quantity: 5 },
      ],
    });
  });

  it('returns business failure response when stock is insufficient', async () => {
    vi.mocked(dummyRepo.reserveStock).mockResolvedValueOnce({
      success: false,
      failureReason: 'INSUFFICIENT_STOCK',
    });

    const result = await service.reserveStock({
      orderId: validOrderId,
      items: [{ productId: validProductId1, quantity: 50 }],
    });

    expect(result.success).toBe(false);
    expect(result.failureReason).toBe('INSUFFICIENT_STOCK');
  });

  it('returns business failure response when product is not found', async () => {
    vi.mocked(dummyRepo.reserveStock).mockResolvedValueOnce({
      success: false,
      failureReason: 'PRODUCT_NOT_FOUND',
    });

    const result = await service.reserveStock({
      orderId: validOrderId,
      items: [{ productId: validProductId1, quantity: 1 }],
    });

    expect(result.success).toBe(false);
    expect(result.failureReason).toBe('PRODUCT_NOT_FOUND');
  });

  it('rejects invalid order UUID', async () => {
    await expect(
      service.reserveStock({
        orderId: 'not-a-uuid',
        items: [{ productId: validProductId1, quantity: 1 }],
      }),
    ).rejects.toThrow(InvalidStockRequestError);
  });

  it('rejects invalid product UUID', async () => {
    await expect(
      service.reserveStock({
        orderId: validOrderId,
        items: [{ productId: 'invalid-prod-uuid', quantity: 1 }],
      }),
    ).rejects.toThrow(InvalidStockRequestError);
  });

  it('rejects empty items list', async () => {
    await expect(
      service.reserveStock({
        orderId: validOrderId,
        items: [],
      }),
    ).rejects.toThrow(InvalidStockRequestError);
  });

  it('rejects non-positive quantity', async () => {
    await expect(
      service.reserveStock({
        orderId: validOrderId,
        items: [{ productId: validProductId1, quantity: 0 }],
      }),
    ).rejects.toThrow(InvalidStockRequestError);

    await expect(
      service.reserveStock({
        orderId: validOrderId,
        items: [{ productId: validProductId1, quantity: -5 }],
      }),
    ).rejects.toThrow(InvalidStockRequestError);
  });
});
