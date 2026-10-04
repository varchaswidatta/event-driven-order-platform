import { describe, it, expect, vi, beforeEach } from 'vitest';
import { EachMessagePayload, Kafka } from 'kafkajs';
import {
  InventoryEventsConsumer,
  INVENTORY_EVENTS_TOPIC,
} from '../../src/messaging/kafka/inventory-events.consumer.js';
import { IOrderRepository } from '../../src/repositories/order.repository.js';
import { ORDER_STATUS } from '../../src/domain/order-status.js';
import { DatabaseOperationError } from '../../src/errors/order.errors.js';

describe('InventoryEventsConsumer (Unit Tests)', () => {
  let mockOrderRepo: IOrderRepository;
  let mockKafka: Kafka;
  let consumer: InventoryEventsConsumer;

  const testOrderId = '11111111-1111-4111-8111-111111111111';
  const testReservationId = '22222222-2222-4222-8222-222222222222';
  const testEventId = '33333333-3333-4333-8333-333333333333';
  const testCorrelationId = '44444444-4444-4444-8444-444444444444';

  const createPayload = (valueObj: unknown, key: string = testOrderId): EachMessagePayload => ({
    topic: INVENTORY_EVENTS_TOPIC,
    partition: 0,
    message: {
      key: Buffer.from(key),
      value: valueObj ? Buffer.from(JSON.stringify(valueObj)) : (null as unknown as Buffer),
      timestamp: String(Date.now()),
      attributes: 0,
      offset: '1',
    },
    heartbeat: vi.fn(),
    pause: vi.fn(),
  });

  beforeEach(() => {
    vi.clearAllMocks();

    mockOrderRepo = {
      createOrder: vi.fn(),
      findOrderById: vi.fn(),
      findOrdersByCustomerId: vi.fn(),
      updateOrderStatus: vi.fn().mockResolvedValue({
        order: {
          id: testOrderId,
          customerId: 'cust-1',
          status: ORDER_STATUS.CONFIRMED,
          totalAmount: '100.00',
          currency: 'USD',
          items: [],
          createdAt: new Date(),
          updatedAt: new Date(),
        },
        alreadyUpdated: false,
      }),
    };

    mockKafka = {
      consumer: vi.fn().mockReturnValue({
        connect: vi.fn().mockResolvedValue(undefined),
        disconnect: vi.fn().mockResolvedValue(undefined),
        subscribe: vi.fn().mockResolvedValue(undefined),
        run: vi.fn().mockResolvedValue(undefined),
      }),
    } as unknown as Kafka;

    consumer = new InventoryEventsConsumer(mockKafka, mockOrderRepo, {
      groupId: 'order-service-unit-test',
    });
  });

  // 1. InventoryReserved → CONFIRMED
  it('updates order status to CONFIRMED when InventoryReserved event is consumed', async () => {
    const event = {
      eventId: testEventId,
      eventType: 'InventoryReserved',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        reservationId: testReservationId,
      },
    };

    await consumer.handleMessage(createPayload(event));

    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledTimes(1);
    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledWith(
      testOrderId,
      ORDER_STATUS.CONFIRMED,
    );
  });

  // 2. InventoryReservationFailed → INVENTORY_FAILED
  it('updates order status to INVENTORY_FAILED when InventoryReservationFailed event is consumed', async () => {
    const event = {
      eventId: testEventId,
      eventType: 'InventoryReservationFailed',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        reason: 'INSUFFICIENT_STOCK',
      },
    };

    await consumer.handleMessage(createPayload(event));

    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledTimes(1);
    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledWith(
      testOrderId,
      ORDER_STATUS.INVENTORY_FAILED,
    );
  });

  // 3. Duplicate InventoryReserved
  it('handles duplicate InventoryReserved delivery idempotently', async () => {
    vi.mocked(mockOrderRepo.updateOrderStatus).mockResolvedValueOnce({
      order: {
        id: testOrderId,
        customerId: 'cust-1',
        status: ORDER_STATUS.CONFIRMED,
        totalAmount: '100.00',
        currency: 'USD',
        items: [],
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      alreadyUpdated: true,
    });

    const event = {
      eventId: testEventId,
      eventType: 'InventoryReserved',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        reservationId: testReservationId,
      },
    };

    await expect(consumer.handleMessage(createPayload(event))).resolves.not.toThrow();
    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledTimes(1);
  });

  // 4. Duplicate InventoryReservationFailed
  it('handles duplicate InventoryReservationFailed delivery idempotently', async () => {
    vi.mocked(mockOrderRepo.updateOrderStatus).mockResolvedValueOnce({
      order: {
        id: testOrderId,
        customerId: 'cust-1',
        status: ORDER_STATUS.INVENTORY_FAILED,
        totalAmount: '100.00',
        currency: 'USD',
        items: [],
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      alreadyUpdated: true,
    });

    const event = {
      eventId: testEventId,
      eventType: 'InventoryReservationFailed',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        reason: 'INSUFFICIENT_STOCK',
      },
    };

    await expect(consumer.handleMessage(createPayload(event))).resolves.not.toThrow();
    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledTimes(1);
  });

  // 5. Invalid status transition handled safely
  it('handles invalid status transition without throwing and preserves state', async () => {
    vi.mocked(mockOrderRepo.updateOrderStatus).mockResolvedValueOnce({
      order: {
        id: testOrderId,
        customerId: 'cust-1',
        status: ORDER_STATUS.CONFIRMED, // Already in terminal state
        totalAmount: '100.00',
        currency: 'USD',
        items: [],
        createdAt: new Date(),
        updatedAt: new Date(),
      },
      alreadyUpdated: true,
    });

    // Attempting to fail an already CONFIRMED order
    const event = {
      eventId: testEventId,
      eventType: 'InventoryReservationFailed',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        reason: 'LATE_FAILURE',
      },
    };

    await expect(consumer.handleMessage(createPayload(event))).resolves.not.toThrow();
    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledWith(
      testOrderId,
      ORDER_STATUS.INVENTORY_FAILED,
    );
  });

  // 6. Malformed event
  it('safely skips malformed JSON without crashing the consumer', async () => {
    const payload: EachMessagePayload = {
      topic: INVENTORY_EVENTS_TOPIC,
      partition: 0,
      message: {
        key: Buffer.from(testOrderId),
        value: Buffer.from('{ malformed json !!'),
        timestamp: String(Date.now()),
        attributes: 0,
        offset: '1',
      },
      heartbeat: vi.fn(),
      pause: vi.fn(),
    };

    await expect(consumer.handleMessage(payload)).resolves.not.toThrow();
    expect(mockOrderRepo.updateOrderStatus).not.toHaveBeenCalled();
  });

  it('safely skips empty or missing message values', async () => {
    const payload: EachMessagePayload = {
      topic: INVENTORY_EVENTS_TOPIC,
      partition: 0,
      message: {
        key: Buffer.from(testOrderId),
        value: null,
        timestamp: String(Date.now()),
        attributes: 0,
        offset: '1',
      },
      heartbeat: vi.fn(),
      pause: vi.fn(),
    };

    await expect(consumer.handleMessage(payload)).resolves.not.toThrow();
    expect(mockOrderRepo.updateOrderStatus).not.toHaveBeenCalled();
  });

  // 7. Unsupported event
  it('safely skips unsupported event types without crashing', async () => {
    const event = {
      eventId: testEventId,
      eventType: 'SomeOtherEvent',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'SomeAggregate',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
      },
    };

    await expect(consumer.handleMessage(createPayload(event))).resolves.not.toThrow();
    expect(mockOrderRepo.updateOrderStatus).not.toHaveBeenCalled();
  });

  // 8. Database failure causes retry (propagates error)
  it('re-throws database errors to prevent Kafka offset commit and trigger redelivery', async () => {
    vi.mocked(mockOrderRepo.updateOrderStatus).mockRejectedValueOnce(
      new DatabaseOperationError('PostgreSQL connection dropped'),
    );

    const event = {
      eventId: testEventId,
      eventType: 'InventoryReserved',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        reservationId: testReservationId,
      },
    };

    await expect(consumer.handleMessage(createPayload(event))).rejects.toThrow(
      'PostgreSQL connection dropped',
    );
  });

  // 9. Event correlation by orderId
  it('extracts and correlates the exact orderId from the event payload', async () => {
    const specificOrderId = '99999999-9999-4999-8999-999999999999';
    const event = {
      eventId: testEventId,
      eventType: 'InventoryReserved',
      eventVersion: 1,
      occurredAt: new Date().toISOString(),
      aggregateType: 'InventoryReservation',
      aggregateId: specificOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: specificOrderId,
        reservationId: testReservationId,
      },
    };

    await consumer.handleMessage(createPayload(event, specificOrderId));

    expect(mockOrderRepo.updateOrderStatus).toHaveBeenCalledWith(
      specificOrderId,
      ORDER_STATUS.CONFIRMED,
    );
  });
});
