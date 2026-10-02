import { IOutboxRepository } from '../repositories/outbox.repository.js';
import { IKafkaProducer } from './kafka/kafka.producer.js';
import { EventEnvelope } from '../domain/outbox-event.js';
import { env } from '../config/env.js';

export const EVENT_TOPIC_MAPPING: Record<string, string> = {
  OrderCreated: 'order.events',
};

export interface OutboxPublisherOptions {
  pollIntervalMs?: number;
  batchSize?: number;
}

/**
 * Periodically polls unpublished domain events from PostgreSQL outbox_events,
 * dispatches them reliably to Apache Kafka, and marks them as published.
 * Implements at-least-once delivery semantics with automated retry tracking.
 */
export class OutboxPublisher {
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private isRunning = false;
  private isPolling = false;
  private isStopping = false;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly outboxRepository: IOutboxRepository,
    private readonly kafkaProducer: IKafkaProducer,
    options?: OutboxPublisherOptions,
  ) {
    this.pollIntervalMs = options?.pollIntervalMs ?? env.OUTBOX_POLL_INTERVAL_MS;
    this.batchSize = options?.batchSize ?? env.OUTBOX_BATCH_SIZE;
  }

  /**
   * Starts the periodic outbox polling and publishing lifecycle.
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;
    this.isStopping = false;
    await this.kafkaProducer.connect();

    // Trigger immediate initial pass, then schedule continuous polling
    this.scheduleNextPoll(0);
  }

  /**
   * Gracefully shuts down the outbox publisher and closes Kafka connections.
   */
  async stop(): Promise<void> {
    this.isRunning = false;
    this.isStopping = true;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // Await any in-flight publishing batch to conclude cleanly
    while (this.isPolling) {
      await new Promise((resolve) => setTimeout(resolve, 50));
    }

    await this.kafkaProducer.disconnect();
    this.isStopping = false;
  }

  /**
   * Executes a single polling and dispatch batch.
   * Exposed publicly to facilitate deterministic testing and manual triggers.
   */
  async publishPendingEvents(): Promise<number> {
    if (this.isPolling) {
      return 0;
    }

    this.isPolling = true;
    let publishedCount = 0;

    try {
      const unpublishedEvents = await this.outboxRepository.findUnpublishedEvents(this.batchSize);

      if (unpublishedEvents.length === 0) {
        return 0;
      }

      for (const event of unpublishedEvents) {
        // Halt processing immediately if shutdown was signaled
        if (this.isStopping) {
          break;
        }

        const topic = EVENT_TOPIC_MAPPING[event.eventType];
        if (!topic) {
          console.warn(
            `[OutboxPublisher] Unrecognized event type without topic mapping: ${event.eventType}`,
            { eventId: event.id, aggregateId: event.aggregateId },
          );
          await this.outboxRepository.incrementRetryCount(event.id);
          continue;
        }

        // Construct standard serialized application event envelope
        const envelope: EventEnvelope = {
          eventId: event.id,
          eventType: event.eventType,
          eventVersion: event.eventVersion,
          occurredAt: event.createdAt.toISOString(),
          aggregateType: event.aggregateType,
          aggregateId: event.aggregateId,
          correlationId: event.correlationId,
          payload: typeof event.payload === 'string' ? JSON.parse(event.payload) : event.payload,
        };

        try {
          // 1. Publish to Kafka with orderId (aggregateId) as message key
          await this.kafkaProducer.publish({
            topic,
            key: event.aggregateId,
            value: JSON.stringify(envelope),
            headers: {
              'event-type': event.eventType,
              'correlation-id': event.correlationId,
            },
          });

          // 2. ONLY mark as published after Kafka broker acknowledges receipt
          await this.outboxRepository.markAsPublished(event.id);
          publishedCount++;
        } catch (publishError) {
          // 3. Increment retry count; event remains unpublished for subsequent retry
          await this.outboxRepository.incrementRetryCount(event.id);

          console.error('[OutboxPublisher] Failed to publish outbox event to Kafka:', {
            eventId: event.id,
            eventType: event.eventType,
            aggregateId: event.aggregateId,
            retryCount: event.retryCount + 1,
            error: publishError instanceof Error ? publishError.message : String(publishError),
          });
        }
      }
    } finally {
      this.isPolling = false;
    }

    return publishedCount;
  }

  private scheduleNextPoll(delayMs: number): void {
    if (!this.isRunning) {
      return;
    }

    this.timer = setTimeout(async () => {
      try {
        await this.publishPendingEvents();
      } catch (err) {
        console.error('[OutboxPublisher] Unhandled error during poll cycle:', err);
      } finally {
        if (this.isRunning) {
          this.scheduleNextPoll(this.pollIntervalMs);
        }
      }
    }, delayMs);
  }

  isActive(): boolean {
    return this.isRunning;
  }
}
