import { Consumer, Kafka, EachMessagePayload } from 'kafkajs';
import { IOrderRepository } from '../../repositories/order.repository.js';
import { ORDER_STATUS } from '../../domain/order-status.js';
import { OrderNotFoundError } from '../../errors/order.errors.js';

export const INVENTORY_EVENTS_TOPIC = 'inventory.events';

/**
 * Inventory event envelope structure consumed from inventory.events Kafka topic.
 */
interface InventoryEventEnvelope {
  eventId: string;
  eventType: string;
  eventVersion: number;
  occurredAt: string;
  aggregateType: string;
  aggregateId: string;
  correlationId: string;
  payload: {
    orderId: string;
    reservationId?: string;
    reason?: string;
    items?: Array<{ productId: string; quantity: number }>;
  };
}

export interface InventoryEventsConsumerOptions {
  groupId: string;
  topic?: string;
}

/**
 * Kafka consumer that subscribes to inventory.events and updates order status
 * based on InventoryReserved / InventoryReservationFailed events.
 *
 * Message processing semantics:
 * - InventoryReserved → order status updated to CONFIRMED
 * - InventoryReservationFailed → order status updated to INVENTORY_FAILED
 * - Unsupported event types are logged and skipped (committed) to avoid poison-pill blocking.
 * - Invalid messages are logged and skipped (committed).
 * - Database/infrastructure failures throw, preventing offset commit and triggering redelivery.
 * - Duplicate events are handled idempotently via updateOrderStatus guard.
 */
export class InventoryEventsConsumer {
  private consumer: Consumer;
  private isRunning = false;

  constructor(
    kafka: Kafka,
    private readonly orderRepository: IOrderRepository,
    private readonly options: InventoryEventsConsumerOptions,
  ) {
    this.consumer = kafka.consumer({
      groupId: options.groupId,
      maxWaitTimeInMs: 100,
    });
  }

  /**
   * Connects to Kafka, subscribes to inventory.events, and starts message consumption.
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.options.topic ?? INVENTORY_EVENTS_TOPIC,
      fromBeginning: false,
    });

    await this.consumer.run({
      eachMessage: async (messagePayload: EachMessagePayload) => {
        await this.handleMessage(messagePayload);
      },
    });

    this.isRunning = true;
    console.log(
      `[OrderService] Inventory events consumer started (group: ${this.options.groupId}, topic: ${this.options.topic ?? INVENTORY_EVENTS_TOPIC})`,
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
    console.log('[OrderService] Inventory events consumer stopped');
  }

  /**
   * Processes a single Kafka message from inventory.events.
   * Validation errors are logged and skipped (committed). Infrastructure errors
   * are propagated to prevent offset commit and trigger redelivery.
   */
  async handleMessage(messagePayload: EachMessagePayload): Promise<void> {
    const { message, partition, topic } = messagePayload;
    const messageKey = message.key?.toString() ?? 'unknown';

    if (!message.value) {
      console.warn('[OrderService] Received inventory event with empty value', {
        topic,
        partition,
        offset: message.offset,
        key: messageKey,
      });
      return;
    }

    // 1. Parse JSON
    let envelope: InventoryEventEnvelope;
    try {
      envelope = JSON.parse(message.value.toString()) as InventoryEventEnvelope;
    } catch {
      console.error('[OrderService] Failed to parse inventory event as JSON', {
        topic,
        partition,
        offset: message.offset,
        key: messageKey,
      });
      // Non-retryable: malformed JSON will never become valid on retry
      return;
    }

    // 2. Validate minimal envelope structure
    if (!envelope.eventType || !envelope.payload?.orderId) {
      console.error('[OrderService] Invalid inventory event envelope', {
        topic,
        partition,
        offset: message.offset,
        key: messageKey,
        eventType: envelope.eventType,
      });
      return;
    }

    try {
      // 3. Route by event type
      switch (envelope.eventType) {
        case 'InventoryReserved': {
          const result = await this.orderRepository.updateOrderStatus(
            envelope.payload.orderId,
            ORDER_STATUS.CONFIRMED,
          );

          if (result.alreadyUpdated) {
            console.log('[OrderService] Duplicate InventoryReserved event handled idempotently', {
              eventId: envelope.eventId,
              orderId: envelope.payload.orderId,
              correlationId: envelope.correlationId,
            });
          } else {
            console.log('[OrderService] Order confirmed via InventoryReserved event', {
              eventId: envelope.eventId,
              orderId: envelope.payload.orderId,
              correlationId: envelope.correlationId,
              newStatus: ORDER_STATUS.CONFIRMED,
            });
          }
          break;
        }

        case 'InventoryReservationFailed': {
          const result = await this.orderRepository.updateOrderStatus(
            envelope.payload.orderId,
            ORDER_STATUS.INVENTORY_FAILED,
          );

          if (result.alreadyUpdated) {
            console.log(
              '[OrderService] Duplicate InventoryReservationFailed event handled idempotently',
              {
                eventId: envelope.eventId,
                orderId: envelope.payload.orderId,
                correlationId: envelope.correlationId,
              },
            );
          } else {
            console.log('[OrderService] Order marked as inventory failed', {
              eventId: envelope.eventId,
              orderId: envelope.payload.orderId,
              correlationId: envelope.correlationId,
              reason: envelope.payload.reason,
              newStatus: ORDER_STATUS.INVENTORY_FAILED,
            });
          }
          break;
        }

        default: {
          // Unsupported event types on inventory.events are safely skipped
          console.log('[OrderService] Skipping unsupported inventory event type', {
            eventType: envelope.eventType,
            key: messageKey,
            partition,
            offset: message.offset,
          });
          return;
        }
      }
    } catch (error) {
      if (error instanceof OrderNotFoundError) {
        console.warn('[OrderService] Skipping inventory event for non-existent order', {
          orderId: error.orderId,
          eventType: envelope.eventType,
          eventId: envelope.eventId,
        });
        return;
      }
      throw error;
    }
  }

  isActive(): boolean {
    return this.isRunning;
  }
}
