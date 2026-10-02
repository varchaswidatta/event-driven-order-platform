import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { OutboxPublisher, EVENT_TOPIC_MAPPING } from '../../src/messaging/outbox.publisher.js';
import { IOutboxRepository } from '../../src/repositories/outbox.repository.js';
import { IKafkaProducer } from '../../src/messaging/kafka/kafka.producer.js';
import { OutboxEventRecord } from '../../src/domain/outbox-event.js';

describe('OutboxPublisher (Unit Tests)', () => {
  let mockOutboxRepo: IOutboxRepository;
  let mockKafkaProducer: IKafkaProducer;
  let publisher: OutboxPublisher;

  const testEventId = 'a1111111-1111-4111-8111-111111111111';
  const testOrderId = 'b2222222-2222-4222-8222-222222222222';
  const testCustomerId = 'c3333333-3333-4333-8333-333333333333';
  const testCorrelationId = 'd4444444-4444-4444-8444-444444444444';

  const createSampleRecord = (overrides?: Partial<OutboxEventRecord>): OutboxEventRecord => ({
    id: testEventId,
    aggregateType: 'Order',
    aggregateId: testOrderId,
    eventType: 'OrderCreated',
    eventVersion: 1,
    correlationId: testCorrelationId,
    payload: {
      orderId: testOrderId,
      customerId: testCustomerId,
      items: [{ productId: 'p1', quantity: 2 }],
    },
    createdAt: new Date('2026-10-02T12:00:00.000Z'),
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

    publisher = new OutboxPublisher(mockOutboxRepo, mockKafkaProducer, {
      pollIntervalMs: 5000,
      batchSize: 10,
    });
  });

  afterEach(async () => {
    await publisher.stop();
  });

  // A. Outbox publisher finds unpublished events
  it('finds unpublished events via OutboxRepository with the configured batch size', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    const count = await publisher.publishPendingEvents();

    expect(count).toBe(1);
    expect(mockOutboxRepo.findUnpublishedEvents).toHaveBeenCalledWith(10);
  });

  // B. OrderCreated is published to the correct Kafka topic
  it('publishes OrderCreated event to order.events topic', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    expect(mockKafkaProducer.publish).toHaveBeenCalledTimes(1);
    const publishCall = vi.mocked(mockKafkaProducer.publish).mock.calls[0][0];
    expect(publishCall.topic).toBe(EVENT_TOPIC_MAPPING.OrderCreated);
    expect(publishCall.topic).toBe('order.events');
  });

  // C. Kafka key is orderId
  it('sets the Kafka message key to orderId (aggregateId)', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    const publishCall = vi.mocked(mockKafkaProducer.publish).mock.calls[0][0];
    expect(publishCall.key).toBe(testOrderId);
  });

  // D. Event envelope is preserved
  it('preserves the full EventEnvelope structure during Kafka serialization', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    const publishCall = vi.mocked(mockKafkaProducer.publish).mock.calls[0][0];
    const parsedEnvelope = JSON.parse(publishCall.value);

    expect(parsedEnvelope).toEqual({
      eventId: testEventId,
      eventType: 'OrderCreated',
      eventVersion: 1,
      occurredAt: '2026-10-02T12:00:00.000Z',
      aggregateType: 'Order',
      aggregateId: testOrderId,
      correlationId: testCorrelationId,
      payload: {
        orderId: testOrderId,
        customerId: testCustomerId,
        items: [{ productId: 'p1', quantity: 2 }],
      },
    });

    expect(publishCall.headers).toEqual({
      'event-type': 'OrderCreated',
      'correlation-id': testCorrelationId,
    });
  });

  // E. Successful Kafka publication causes published_at to be set
  it('marks the outbox event as published only AFTER successful Kafka acknowledgment', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);

    await publisher.publishPendingEvents();

    expect(mockKafkaProducer.publish).toHaveBeenCalled();
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith(testEventId);
    expect(mockOutboxRepo.incrementRetryCount).not.toHaveBeenCalled();
  });

  // F & G. Failed Kafka publication does NOT set published_at and increments retry_count
  it('does NOT set published_at and increments retry_count when Kafka publish fails', async () => {
    const sampleRecord = createSampleRecord();
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);
    vi.mocked(mockKafkaProducer.publish).mockRejectedValueOnce(
      new Error('Kafka broker connection timeout'),
    );

    const count = await publisher.publishPendingEvents();

    expect(count).toBe(0);
    expect(mockOutboxRepo.markAsPublished).not.toHaveBeenCalled();
    expect(mockOutboxRepo.incrementRetryCount).toHaveBeenCalledWith(testEventId);
  });

  // H. Unpublished events are retried on a later cycle
  it('retries unpublished events on subsequent polling cycles', async () => {
    const sampleRecord = createSampleRecord();

    // Cycle 1: Kafka fails
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([sampleRecord]);
    vi.mocked(mockKafkaProducer.publish).mockRejectedValueOnce(new Error('Broker unreachable'));

    const count1 = await publisher.publishPendingEvents();
    expect(count1).toBe(0);
    expect(mockOutboxRepo.incrementRetryCount).toHaveBeenCalledTimes(1);
    expect(mockOutboxRepo.markAsPublished).not.toHaveBeenCalled();

    // Cycle 2: Same event is fetched again (retry) and succeeds
    const retriedRecord = createSampleRecord({ retryCount: 1 });
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([retriedRecord]);
    vi.mocked(mockKafkaProducer.publish).mockResolvedValueOnce(undefined);

    const count2 = await publisher.publishPendingEvents();
    expect(count2).toBe(1);
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith(testEventId);
  });

  // I. Multiple unpublished events can be processed in batches
  it('processes multiple unpublished events sequentially in batches', async () => {
    const event1 = createSampleRecord({ id: 'id-1', aggregateId: 'order-1' });
    const event2 = createSampleRecord({ id: 'id-2', aggregateId: 'order-2' });
    const event3 = createSampleRecord({ id: 'id-3', aggregateId: 'order-3' });

    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([event1, event2, event3]);

    const count = await publisher.publishPendingEvents();

    expect(count).toBe(3);
    expect(mockKafkaProducer.publish).toHaveBeenCalledTimes(3);
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith('id-1');
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith('id-2');
    expect(mockOutboxRepo.markAsPublished).toHaveBeenCalledWith('id-3');
  });

  // J. Already-published events are not republished by normal polling
  it('returns 0 and does not publish when no unpublished events remain', async () => {
    vi.mocked(mockOutboxRepo.findUnpublishedEvents).mockResolvedValueOnce([]);

    const count = await publisher.publishPendingEvents();

    expect(count).toBe(0);
    expect(mockKafkaProducer.publish).not.toHaveBeenCalled();
    expect(mockOutboxRepo.markAsPublished).not.toHaveBeenCalled();
  });

  // K. Publisher shutdown disconnects Kafka cleanly
  it('disconnects Kafka producer and cleans up timers on stop()', async () => {
    await publisher.start();
    expect(publisher.isActive()).toBe(true);
    expect(mockKafkaProducer.connect).toHaveBeenCalled();

    await publisher.stop();
    expect(publisher.isActive()).toBe(false);
    expect(mockKafkaProducer.disconnect).toHaveBeenCalled();
  });
});
