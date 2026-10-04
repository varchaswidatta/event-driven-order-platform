import { OrderCreatedEvent } from '../domain/events.js';
import { Reservation } from '../domain/reservation.js';
import { RESERVATION_STATUS } from '../domain/reservation-status.js';
import { IInventoryRepository } from '../repositories/inventory.repository.js';
import { IStockServiceClient, StockReservationResponse } from '../clients/stock-service.client.js';
import { eventEnvelopeSchema, orderCreatedPayloadSchema } from '../validation/event.schema.js';
import {
  InvalidEventError,
  UnsupportedEventTypeError,
  UnsupportedEventVersionError,
} from '../errors/inventory.errors.js';

const SUPPORTED_EVENT_TYPE = 'OrderCreated';
const SUPPORTED_AGGREGATE_TYPE = 'Order';
const SUPPORTED_EVENT_VERSION = 1;

export interface ProcessEventResult {
  reservation: Reservation;
  alreadyExisted: boolean;
  stockReservation?: StockReservationResponse;
}

export interface IInventoryService {
  processOrderCreatedEvent(event: OrderCreatedEvent): Promise<ProcessEventResult>;
  validateAndParseEvent(rawPayload: unknown): OrderCreatedEvent;
}

export class InventoryService implements IInventoryService {
  constructor(
    private readonly repository: IInventoryRepository,
    private readonly stockClient?: IStockServiceClient,
  ) {}

  /**
   * Validates and parses a raw Kafka message value into a typed OrderCreated event.
   * Validates the envelope structure, event type, version, aggregate type, and payload.
   *
   * @throws InvalidEventError if envelope or payload validation fails
   * @throws UnsupportedEventTypeError if eventType is not OrderCreated
   * @throws UnsupportedEventVersionError if eventVersion is not supported
   */
  validateAndParseEvent(rawPayload: unknown): OrderCreatedEvent {
    // 1. Validate envelope structure
    const envelopeResult = eventEnvelopeSchema.safeParse(rawPayload);
    if (!envelopeResult.success) {
      const issues = envelopeResult.error.issues.map(
        (issue) => `${issue.path.join('.')}: ${issue.message}`,
      );
      throw new InvalidEventError('Invalid event envelope', issues);
    }

    const envelope = envelopeResult.data;

    // 2. Validate event type
    if (envelope.eventType !== SUPPORTED_EVENT_TYPE) {
      throw new UnsupportedEventTypeError(envelope.eventType);
    }

    // 3. Validate aggregate type
    if (envelope.aggregateType !== SUPPORTED_AGGREGATE_TYPE) {
      throw new InvalidEventError(
        `Expected aggregateType "${SUPPORTED_AGGREGATE_TYPE}" but received "${envelope.aggregateType}"`,
      );
    }

    // 4. Validate event version
    if (envelope.eventVersion !== SUPPORTED_EVENT_VERSION) {
      throw new UnsupportedEventVersionError(envelope.eventType, envelope.eventVersion);
    }

    // 5. Validate OrderCreated payload
    const payloadResult = orderCreatedPayloadSchema.safeParse(envelope.payload);
    if (!payloadResult.success) {
      const issues = payloadResult.error.issues.map(
        (issue) => `${issue.path.join('.')}: ${issue.message}`,
      );
      throw new InvalidEventError('Invalid OrderCreated payload', issues);
    }

    return {
      eventId: envelope.eventId,
      eventType: envelope.eventType,
      eventVersion: envelope.eventVersion,
      occurredAt: envelope.occurredAt,
      aggregateType: envelope.aggregateType,
      aggregateId: envelope.aggregateId,
      correlationId: envelope.correlationId,
      payload: payloadResult.data,
    };
  }

  /**
   * Processes a validated OrderCreated event by creating an inventory reservation,
   * calling the Stock Service to reserve stock, and then atomically updating
   * the reservation status and writing the outbox event.
   *
   * On stock reservation success → status=RESERVED, InventoryReserved event
   * On stock reservation business failure → status=FAILED, InventoryReservationFailed event
   * On stock service infrastructure error → exception propagated (Kafka redelivery)
   */
  async processOrderCreatedEvent(event: OrderCreatedEvent): Promise<ProcessEventResult> {
    const result = await this.repository.createReservation({
      orderId: event.payload.orderId,
      items: event.payload.items.map((item) => ({
        productId: item.productId,
        quantity: item.quantity,
      })),
    });

    // If reservation already existed (idempotent replay), skip stock call and outbox write
    if (result.alreadyExisted) {
      return {
        reservation: result.reservation,
        alreadyExisted: true,
      };
    }

    let stockReservation: StockReservationResponse | undefined;
    if (this.stockClient) {
      // The stock client call may throw infrastructure errors (UNAVAILABLE, TIMEOUT).
      // These are NOT caught here — they propagate up to the Kafka consumer, preventing
      // offset commit and triggering at-least-once redelivery.
      stockReservation = await this.stockClient.reserveStock(
        event.payload.orderId,
        event.payload.items.map((item) => ({
          productId: item.productId,
          quantity: item.quantity,
        })),
      );

      // Determine outcome based on stock reservation result
      if (stockReservation.success) {
        // SUCCESS: atomically update status + write InventoryReserved outbox event
        const updatedReservation = await this.repository.updateReservationStatus(
          event.payload.orderId,
          RESERVATION_STATUS.RESERVED,
          {
            aggregateType: 'InventoryReservation',
            aggregateId: event.payload.orderId,
            eventType: 'InventoryReserved',
            eventVersion: 1,
            correlationId: event.correlationId,
            payload: {
              orderId: event.payload.orderId,
              reservationId: result.reservation.id,
              items: event.payload.items.map((item) => ({
                productId: item.productId,
                quantity: item.quantity,
              })),
            },
          },
        );

        return {
          reservation: updatedReservation,
          alreadyExisted: false,
          stockReservation,
        };
      } else {
        // BUSINESS FAILURE: atomically update status + write InventoryReservationFailed outbox event
        const updatedReservation = await this.repository.updateReservationStatus(
          event.payload.orderId,
          RESERVATION_STATUS.FAILED,
          {
            aggregateType: 'InventoryReservation',
            aggregateId: event.payload.orderId,
            eventType: 'InventoryReservationFailed',
            eventVersion: 1,
            correlationId: event.correlationId,
            payload: {
              orderId: event.payload.orderId,
              reason: stockReservation.failureReason ?? 'Insufficient stock',
              items: event.payload.items.map((item) => ({
                productId: item.productId,
                quantity: item.quantity,
              })),
            },
          },
        );

        return {
          reservation: updatedReservation,
          alreadyExisted: false,
          stockReservation,
        };
      }
    }

    return {
      reservation: result.reservation,
      alreadyExisted: false,
      stockReservation,
    };
  }
}
