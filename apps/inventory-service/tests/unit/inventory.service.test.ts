import { describe, it, expect, vi } from 'vitest';
import { InventoryService } from '../../src/services/inventory.service.js';
import {
  IInventoryRepository,
  ReservationResult,
} from '../../src/repositories/inventory.repository.js';
import { OrderCreatedEvent } from '../../src/domain/events.js';
import { RESERVATION_STATUS } from '../../src/domain/reservation-status.js';

describe('Inventory Service Unit Tests', () => {
  const sampleEvent: OrderCreatedEvent = {
    eventId: '11111111-1111-4111-8111-111111111111',
    eventType: 'OrderCreated',
    eventVersion: 1,
    occurredAt: '2026-10-02T12:00:00.000Z',
    aggregateType: 'Order',
    aggregateId: '22222222-2222-4222-8222-222222222222',
    correlationId: '33333333-3333-4333-8333-333333333333',
    payload: {
      orderId: '22222222-2222-4222-8222-222222222222',
      customerId: '44444444-4444-4444-8444-444444444444',
      items: [
        {
          productId: '55555555-5555-4555-8555-555555555555',
          quantity: 2,
        },
        {
          productId: '66666666-6666-4666-8666-666666666666',
          quantity: 5,
        },
      ],
    },
  };

  it('correctly maps event payload to repository input and creates new reservation', async () => {
    const mockCreatedReservation: ReservationResult = {
      reservation: {
        id: '77777777-7777-4777-8777-777777777777',
        orderId: sampleEvent.payload.orderId,
        status: RESERVATION_STATUS.PENDING,
        items: [
          {
            id: '88888888-8888-4888-8888-888888888888',
            reservationId: '77777777-7777-4777-8777-777777777777',
            productId: '55555555-5555-4555-8555-555555555555',
            quantity: 2,
            createdAt: new Date(),
          },
          {
            id: '99999999-9999-4999-8999-999999999999',
            reservationId: '77777777-7777-4777-8777-777777777777',
            productId: '66666666-6666-4666-8666-666666666666',
            quantity: 5,
            createdAt: new Date(),
          },
        ],
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      alreadyExisted: false,
    };

    const mockRepo: IInventoryRepository = {
      createReservation: vi.fn().mockResolvedValue(mockCreatedReservation),
      findReservationByOrderId: vi.fn(),
      findReservationById: vi.fn(),
    };

    const service = new InventoryService(mockRepo);
    const result = await service.processOrderCreatedEvent(sampleEvent);

    expect(mockRepo.createReservation).toHaveBeenCalledTimes(1);
    expect(mockRepo.createReservation).toHaveBeenCalledWith({
      orderId: sampleEvent.payload.orderId,
      items: [
        { productId: '55555555-5555-4555-8555-555555555555', quantity: 2 },
        { productId: '66666666-6666-4666-8666-666666666666', quantity: 5 },
      ],
    });

    expect(result.alreadyExisted).toBe(false);
    expect(result.reservation.status).toBe('PENDING');
    expect(result.reservation.items).toHaveLength(2);
  });

  it('handles duplicate order idempotently by returning alreadyExisted = true', async () => {
    const mockExistingReservation: ReservationResult = {
      reservation: {
        id: '77777777-7777-4777-8777-777777777777',
        orderId: sampleEvent.payload.orderId,
        status: RESERVATION_STATUS.PENDING,
        items: [],
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      alreadyExisted: true,
    };

    const mockRepo: IInventoryRepository = {
      createReservation: vi.fn().mockResolvedValue(mockExistingReservation),
      findReservationByOrderId: vi.fn(),
      findReservationById: vi.fn(),
    };

    const service = new InventoryService(mockRepo);
    const result = await service.processOrderCreatedEvent(sampleEvent);

    expect(result.alreadyExisted).toBe(true);
    expect(result.reservation.id).toBe(mockExistingReservation.reservation.id);
  });

  it('propagates repository errors to allow transaction failure / consumer retry', async () => {
    const mockRepo: IInventoryRepository = {
      createReservation: vi.fn().mockRejectedValue(new Error('PostgreSQL connection timeout')),
      findReservationByOrderId: vi.fn(),
      findReservationById: vi.fn(),
    };

    const service = new InventoryService(mockRepo);
    await expect(service.processOrderCreatedEvent(sampleEvent)).rejects.toThrow(
      'PostgreSQL connection timeout',
    );
  });
  it('delegates to StockServiceClient and includes successful stock reservation in result', async () => {
    const mockUpdatedReservation = {
      id: '77777777-7777-4777-8777-777777777777',
      orderId: sampleEvent.payload.orderId,
      status: RESERVATION_STATUS.RESERVED,
      items: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const mockRepo: IInventoryRepository = {
      createReservation: vi.fn().mockResolvedValue({
        reservation: {
          id: '77777777-7777-4777-8777-777777777777',
          orderId: sampleEvent.payload.orderId,
          status: RESERVATION_STATUS.PENDING,
          items: [],
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        alreadyExisted: false,
      }),
      findReservationByOrderId: vi.fn(),
      findReservationById: vi.fn(),
      updateReservationStatus: vi.fn().mockResolvedValue(mockUpdatedReservation),
    };

    const mockStockClient = {
      reserveStock: vi.fn().mockResolvedValue({
        success: true,
        reservationId: 'stock-res-123',
      }),
      close: vi.fn(),
    };

    const service = new InventoryService(mockRepo, mockStockClient);
    const result = await service.processOrderCreatedEvent(sampleEvent);

    expect(mockStockClient.reserveStock).toHaveBeenCalledWith(
      sampleEvent.payload.orderId,
      sampleEvent.payload.items,
    );
    expect(mockRepo.updateReservationStatus).toHaveBeenCalledWith(
      sampleEvent.payload.orderId,
      RESERVATION_STATUS.RESERVED,
      expect.objectContaining({
        eventType: 'InventoryReserved',
        aggregateId: sampleEvent.payload.orderId,
        correlationId: sampleEvent.correlationId,
      }),
    );
    expect(result.reservation.status).toBe(RESERVATION_STATUS.RESERVED);
    expect(result.stockReservation).toEqual({
      success: true,
      reservationId: 'stock-res-123',
    });
  });

  it('captures business failure (INSUFFICIENT_STOCK) without throwing and emits InventoryReservationFailed', async () => {
    const mockUpdatedReservation = {
      id: '77777777-7777-4777-8777-777777777777',
      orderId: sampleEvent.payload.orderId,
      status: RESERVATION_STATUS.FAILED,
      items: [],
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const mockRepo: IInventoryRepository = {
      createReservation: vi.fn().mockResolvedValue({
        reservation: {
          id: '77777777-7777-4777-8777-777777777777',
          orderId: sampleEvent.payload.orderId,
          status: RESERVATION_STATUS.PENDING,
          items: [],
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        alreadyExisted: false,
      }),
      findReservationByOrderId: vi.fn(),
      findReservationById: vi.fn(),
      updateReservationStatus: vi.fn().mockResolvedValue(mockUpdatedReservation),
    };

    const mockStockClient = {
      reserveStock: vi.fn().mockResolvedValue({
        success: false,
        failureReason: 'INSUFFICIENT_STOCK',
      }),
      close: vi.fn(),
    };

    const service = new InventoryService(mockRepo, mockStockClient);
    const result = await service.processOrderCreatedEvent(sampleEvent);

    expect(mockRepo.updateReservationStatus).toHaveBeenCalledWith(
      sampleEvent.payload.orderId,
      RESERVATION_STATUS.FAILED,
      expect.objectContaining({
        eventType: 'InventoryReservationFailed',
        aggregateId: sampleEvent.payload.orderId,
        correlationId: sampleEvent.correlationId,
        payload: expect.objectContaining({
          reason: 'INSUFFICIENT_STOCK',
        }),
      }),
    );
    expect(result.reservation.status).toBe(RESERVATION_STATUS.FAILED);
    expect(result.stockReservation).toEqual({
      success: false,
      failureReason: 'INSUFFICIENT_STOCK',
    });
  });

  it('re-throws infrastructure errors (e.g. StockServiceUnavailableError) to prevent Kafka acknowledgment', async () => {
    const mockRepo: IInventoryRepository = {
      createReservation: vi.fn().mockResolvedValue({
        reservation: {
          id: '77777777-7777-4777-8777-777777777777',
          orderId: sampleEvent.payload.orderId,
          status: RESERVATION_STATUS.PENDING,
          items: [],
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        alreadyExisted: false,
      }),
      findReservationByOrderId: vi.fn(),
      findReservationById: vi.fn(),
    };

    const mockStockClient = {
      reserveStock: vi
        .fn()
        .mockRejectedValue(new Error('Stock Service is unreachable: UNAVAILABLE')),
      close: vi.fn(),
    };

    const service = new InventoryService(mockRepo, mockStockClient);
    await expect(service.processOrderCreatedEvent(sampleEvent)).rejects.toThrow(
      'Stock Service is unreachable: UNAVAILABLE',
    );
  });
});
