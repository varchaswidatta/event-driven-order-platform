import { Consumer, Kafka, EachMessagePayload } from 'kafkajs';
import { IInventoryService } from '../../services/inventory.service.js';
import {
  InvalidEventError,
  UnsupportedEventTypeError,
  UnsupportedEventVersionError,
} from '../../errors/inventory.errors.js';

export const ORDER_EVENTS_TOPIC = 'order.events';

export interface OrderEventsConsumerOptions {
  groupId: string;
  topic?: string;
}

/**
 * Kafka consumer that subscribes to order.events and delegates processing
 * to the InventoryService. Keeps Kafka transport concerns separate from
 * business logic.
 *
 * Message processing semantics:
 * - Valid OrderCreated events are processed and committed.
 * - Duplicate OrderCreated events are handled idempotently and committed.
 * - Unsupported event types are logged and skipped (committed) to avoid poison-pill blocking.
 * - Invalid envelopes/payloads are logged and skipped (committed) to avoid poison-pill blocking.
 * - Unsupported event versions are logged and skipped (committed).
 * - Database/infrastructure failures throw, preventing offset commit and triggering redelivery.
 */
export class OrderEventsConsumer {
  private consumer: Consumer;
  private isRunning = false;

  constructor(
    kafka: Kafka,
    private readonly inventoryService: IInventoryService,
    private readonly options: OrderEventsConsumerOptions,
  ) {
    this.consumer = kafka.consumer({
      groupId: options.groupId,
      maxWaitTimeInMs: 100,
    });
  }

  /**
   * Connects to Kafka, subscribes to order.events, and starts message consumption.
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.options.topic ?? ORDER_EVENTS_TOPIC,
      fromBeginning: false,
    });

    await this.consumer.run({
      eachMessage: async (messagePayload: EachMessagePayload) => {
        await this.handleMessage(messagePayload);
      },
    });

    this.isRunning = true;
    console.log(
      `[InventoryService] Kafka consumer started (group: ${this.options.groupId}, topic: ${this.options.topic ?? ORDER_EVENTS_TOPIC})`,
    );
  }

  /**
   * Gracefully stops the consumer and disconnects from Kafka.
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    await this.consumer.disconnect();
    console.log('[InventoryService] Kafka consumer stopped');
  }

  /**
   * Processes a single Kafka message.
   * Validation and business logic errors that are non-retryable (invalid envelope,
   * unsupported event type/version) are logged and the message is skipped to prevent
   * poison-pill blocking. Infrastructure errors (database failures) are propagated
   * to prevent offset commit and trigger redelivery.
   */
  private async handleMessage(messagePayload: EachMessagePayload): Promise<void> {
    const { message, partition, topic } = messagePayload;
    const messageKey = message.key?.toString() ?? 'unknown';

    if (!message.value) {
      console.warn('[InventoryService] Received Kafka message with empty value', {
        topic,
        partition,
        offset: message.offset,
        key: messageKey,
      });
      return;
    }

    // 1. Parse JSON
    let parsedValue: unknown;
    try {
      parsedValue = JSON.parse(message.value.toString());
    } catch {
      console.error('[InventoryService] Failed to parse Kafka message as JSON', {
        topic,
        partition,
        offset: message.offset,
        key: messageKey,
      });
      // Non-retryable: malformed JSON will never become valid on retry
      return;
    }

    // 2. Validate and route event
    try {
      const event = this.inventoryService.validateAndParseEvent(parsedValue);

      const result = await this.inventoryService.processOrderCreatedEvent(event);

      if (result.alreadyExisted) {
        console.log('[InventoryService] Duplicate OrderCreated event handled idempotently', {
          eventId: event.eventId,
          orderId: event.payload.orderId,
          correlationId: event.correlationId,
          reservationId: result.reservation.id,
        });
      } else {
        console.log('[InventoryService] OrderCreated event processed successfully', {
          eventId: event.eventId,
          orderId: event.payload.orderId,
          correlationId: event.correlationId,
          reservationId: result.reservation.id,
          status: result.reservation.status,
          itemCount: result.reservation.items.length,
        });
      }
    } catch (error) {
      if (error instanceof UnsupportedEventTypeError) {
        // Non-retryable: this consumer only handles OrderCreated events.
        // Unknown event types on order.events are safely skipped to avoid blocking.
        console.log('[InventoryService] Skipping unsupported event type', {
          eventType: error.eventType,
          key: messageKey,
          partition,
          offset: message.offset,
        });
        return;
      }

      if (error instanceof UnsupportedEventVersionError) {
        // Non-retryable: this consumer only supports eventVersion 1.
        // Future versions require consumer code updates before processing.
        console.warn('[InventoryService] Skipping unsupported event version', {
          eventType: error.eventType,
          eventVersion: error.eventVersion,
          key: messageKey,
          partition,
          offset: message.offset,
        });
        return;
      }

      if (error instanceof InvalidEventError) {
        // Non-retryable: structurally invalid messages will never become valid on retry.
        console.error('[InventoryService] Skipping invalid event', {
          issues: error.issues,
          key: messageKey,
          partition,
          offset: message.offset,
        });
        return;
      }

      // Infrastructure/database errors ARE retryable — propagate to prevent offset commit
      console.error('[InventoryService] Failed to process Kafka message', {
        key: messageKey,
        partition,
        offset: message.offset,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  isActive(): boolean {
    return this.isRunning;
  }
}
