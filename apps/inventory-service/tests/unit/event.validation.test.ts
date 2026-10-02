import { describe, it, expect } from 'vitest';
import { InventoryService } from '../../src/services/inventory.service.js';
import { IInventoryRepository } from '../../src/repositories/inventory.repository.js';
import {
  InvalidEventError,
  UnsupportedEventTypeError,
  UnsupportedEventVersionError,
} from '../../src/errors/inventory.errors.js';

describe('Event Validation and Parsing Unit Tests', () => {
  const dummyRepo: IInventoryRepository = {
    createReservation: async () => {
      throw new Error('Not implemented in validation tests');
    },
    findReservationByOrderId: async () => null,
    findReservationById: async () => null,
  };

  const service = new InventoryService(dummyRepo);

  const validOrderCreatedPayload = {
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
      ],
    },
  };

  it('accepts and parses a valid OrderCreated event', () => {
    const parsed = service.validateAndParseEvent(validOrderCreatedPayload);

    expect(parsed.eventId).toBe(validOrderCreatedPayload.eventId);
    expect(parsed.eventType).toBe('OrderCreated');
    expect(parsed.eventVersion).toBe(1);
    expect(parsed.aggregateType).toBe('Order');
    expect(parsed.aggregateId).toBe(validOrderCreatedPayload.aggregateId);
    expect(parsed.correlationId).toBe(validOrderCreatedPayload.correlationId);
    expect(parsed.payload.orderId).toBe(validOrderCreatedPayload.payload.orderId);
    expect(parsed.payload.items).toHaveLength(1);
    expect(parsed.payload.items[0]).toEqual({
      productId: '55555555-5555-4555-8555-555555555555',
      quantity: 2,
    });
  });

  it('rejects an invalid envelope missing required fields', () => {
    const invalidEnvelope = {
      // Missing eventId, aggregateType, correlationId
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: '2026-10-02T12:00:00.000Z',
      payload: {},
    };

    expect(() => service.validateAndParseEvent(invalidEnvelope)).toThrow(InvalidEventError);
  });

  it('rejects non-UUID eventId and aggregateId', () => {
    const invalidUuids = {
      ...validOrderCreatedPayload,
      eventId: 'not-a-uuid',
      aggregateId: 'invalid-id',
    };

    expect(() => service.validateAndParseEvent(invalidUuids)).toThrow(InvalidEventError);
  });

  it('rejects unsupported event types with UnsupportedEventTypeError', () => {
    const unsupportedEvent = {
      ...validOrderCreatedPayload,
      eventType: 'OrderCancelled',
    };

    try {
      service.validateAndParseEvent(unsupportedEvent);
      expect.fail('Should have thrown UnsupportedEventTypeError');
    } catch (error) {
      expect(error).toBeInstanceOf(UnsupportedEventTypeError);
      expect((error as UnsupportedEventTypeError).eventType).toBe('OrderCancelled');
    }
  });

  it('rejects mismatched aggregateType', () => {
    const mismatchedAggregate = {
      ...validOrderCreatedPayload,
      aggregateType: 'Customer',
    };

    expect(() => service.validateAndParseEvent(mismatchedAggregate)).toThrow(InvalidEventError);
  });

  it('rejects unsupported event versions with UnsupportedEventVersionError', () => {
    const futureVersionEvent = {
      ...validOrderCreatedPayload,
      eventVersion: 2,
    };

    try {
      service.validateAndParseEvent(futureVersionEvent);
      expect.fail('Should have thrown UnsupportedEventVersionError');
    } catch (error) {
      expect(error).toBeInstanceOf(UnsupportedEventVersionError);
      expect((error as UnsupportedEventVersionError).eventVersion).toBe(2);
      expect((error as UnsupportedEventVersionError).eventType).toBe('OrderCreated');
    }
  });

  it('rejects payload with empty items array', () => {
    const emptyItemsEvent = {
      ...validOrderCreatedPayload,
      payload: {
        ...validOrderCreatedPayload.payload,
        items: [],
      },
    };

    expect(() => service.validateAndParseEvent(emptyItemsEvent)).toThrow(InvalidEventError);
  });

  it('rejects payload with non-positive item quantity', () => {
    const zeroQuantityEvent = {
      ...validOrderCreatedPayload,
      payload: {
        ...validOrderCreatedPayload.payload,
        items: [
          {
            productId: '55555555-5555-4555-8555-555555555555',
            quantity: 0,
          },
        ],
      },
    };

    expect(() => service.validateAndParseEvent(zeroQuantityEvent)).toThrow(InvalidEventError);

    const negativeQuantityEvent = {
      ...validOrderCreatedPayload,
      payload: {
        ...validOrderCreatedPayload.payload,
        items: [
          {
            productId: '55555555-5555-4555-8555-555555555555',
            quantity: -3,
          },
        ],
      },
    };

    expect(() => service.validateAndParseEvent(negativeQuantityEvent)).toThrow(InvalidEventError);
  });

  it('rejects payload with invalid product UUID', () => {
    const invalidProductIdEvent = {
      ...validOrderCreatedPayload,
      payload: {
        ...validOrderCreatedPayload.payload,
        items: [
          {
            productId: 'invalid-product-uuid',
            quantity: 1,
          },
        ],
      },
    };

    expect(() => service.validateAndParseEvent(invalidProductIdEvent)).toThrow(InvalidEventError);
  });
});
