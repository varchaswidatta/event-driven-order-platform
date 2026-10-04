import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  InventoryOutboxPublisher,
  INVENTORY_EVENT_TOPIC_MAPPING,
} from '../../src/messaging/outbox.publisher.js';
import { IOutboxRepository, OutboxEventRecord } from '../../src/repositories/outbox.repository.js';
import { IKafkaProducer } from '../../src/messaging/kafka/kafka.producer.js';

describe('InventoryOutboxPublisher (Unit Tests)', () => {
  let mockOutboxRepo: IOutboxRepository;
  let mockKafkaProducer: IKafkaProducer;
  let publisher: InventoryOutboxPublisher;

  const testEventId = 'a1111111-1111-4111-8111-111111111111';
  const testOrderId = 'b2222222-2222-4222-8222-222222222222';
  const testReservationId = 'c3333333-3333-4333-8333-333333333333';
  const testCorrelationId = 'd4444444-4444-4444-8444-444444444444';

  const createSampleRecord = (overrides?: Partial<OutboxEventRecord>): OutboxEventRecord => ({
    id: testEventId,
    aggregateType: 'InventoryReservation',
    aggregateId: testOrderId,
    eventType: 'InventoryReserved',
    eventVersion: 1,
    correlationId: testCorrelationId,
    payload: {
      orderId: testOrderId,
      reservationId: testReservationId,
      items: [{ productId: 'p1', quantity: 2 }],
    },
    createdAt: new Date('2026-10-04T12:00:00.000Z'),
    publishedAt: null,
    retryCount: 0,
    ...overrides,
  });

  beforeEach(() => {
    vi.clearAllMocks();

    mockOutboxRepo = {
      insertEvent: vi.fn(),
      findUnpublishedEvents: vi.fn().mockResolvedValue([]),
      markAsPublished: vi.fn().mockResolvedValue(undefined),
      incrementRetryCount: vi.fn().mockResolvedValue(undefined),
      findById: vi.fn(),
      findByAggregateId: vi.fn(),
    };

    mockKafkaProducer = {
      connect: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
      publish: vi.fn().mockResolvedValue(undefined),
      isConnected: vi.fn().mockReturnValue(true),
    };

    publisher = new InventoryOutboxPublisher(mockOutboxRepo, mockKafkaProducer, {
      pollIntervalMs: 5000,
      batchSize: 10,
    });
  });

  afterEach(async () => {
    await publisher.stop();
  });

  // 1. Unpublished event retrieval
  it('finds unpublished events via OutboxRepository with the configured batch size', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    const count = await publisher.publishPendingEvents();

    expect(count).toBe(1);
    expect(mockOutboxRepo.findUnpublishedEvents).toHaveBeenCalledWith(10);
  });

  // 2. Successful Kafka publish
  it('publishes InventoryReserved event to inventory.events topic', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    expect(mockKafkaProducer.publish).toHaveBeenCalledTimes(1);
    const publishCall = vi.mocked(mockKafkaProducer.publish).mock.calls[0][0];
    expect(publishCall.topic).toBe(INVENTORY_EVENT_TOPIC_MAPPING.InventoryReserved);
    expect(publishCall.topic).toBe('inventory.events');
  });

  it('publishes InventoryReservationFailed event to inventory.events topic', async () => {
    const sampleRecord = createSampleRecord({
      eventType: 'InventoryReservationFailed',
      payload: {
        orderId: testOrderId,
        reason: 'INSUFFICIENT_STOCK',
      },
    });
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    expect(mockKafkaProducer.publish).toHaveBeenCalledTimes(1);
    const publishCall = vi.mocked(mockKafkaProducer.publish).mock.calls[0][0];
    expect(publishCall.topic).toBe('inventory.events');
  });

  // 3. published_at update
  it('marks event as published only AFTER successful Kafka publish receipt', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledTimes(1);
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith(testEventId);
    expect(mockOutboxRepo.incrementRetryCount).not.toHaveBeenCalled();
  });

  // 4. Publish failure
  it('does NOT mark event as published if Kafka publish throws', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);
    vi.mocked(mockKafkaProducer.publish).mockRejectedValueOnce(new Error('Broker disconnected'));

    const count = await publisher.publishPendingEvents();

    expect(count).toBe(0);
    expect(mockOutboxRepo.markAsPublished).not.toHaveBeenCalled();
  });

  // 5. retry_count increment
  it('increments retry_count when Kafka publishing fails', async () => {
    const sampleRecord = createSampleRecord({ retryCount: 2 });
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);
    vi.mocked(mockKafkaProducer.publish).mockRejectedValueOnce(new Error('Leader not available'));

    await publisher.publishPendingEvents();

    expect(mockOutboxRepo.incrementRetryCount).toHaveBeenCalledTimes(1);
    expect(mockOutboxRepo.incrementRetryCount).toHaveBeenCalledWith(testEventId);
  });

  // 6. Recovery after Kafka becomes available
  it('successfully publishes and marks event on subsequent poll after broker recovery', async () => {
    const sampleRecord = createSampleRecord();

    // First attempt: Kafka fails
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);
    vi.mocked(mockKafkaProducer.publish).mockRejectedValueOnce(new Error('Broker network blip'));

    const firstCount = await publisher.publishPendingEvents();
    expect(firstCount).toBe(0);
    expect(mockOutboxRepo.incrementRetryCount).toHaveBeenCalledWith(testEventId);
    expect(mockOutboxRepo.markAsPublished).not.toHaveBeenCalled();

    // Second attempt: Kafka succeeds
    const retriedRecord = createSampleRecord({ retryCount: 1 });
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([retriedRecord]);
    vi.mocked(mockKafkaProducer.publish).mockResolvedValueOnce(undefined);

    const secondCount = await publisher.publishPendingEvents();
    expect(secondCount).toBe(1);
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith(testEventId);
  });

  // 7. Message key equals orderId
  it('sets the Kafka message key strictly to orderId (aggregateId) for partition ordering', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    const publishCall = vi.mocked(mockKafkaProducer.publish).mock.calls[0][0];
    expect(publishCall.key).toBe(testOrderId);
  });
});
