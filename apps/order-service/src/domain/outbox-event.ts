import crypto from 'node:crypto';

/**
 * Standard reusable application event envelope for all domain events.
 */
export interface EventEnvelope<T = unknown> {
  eventId: string;
  eventType: string;
  eventVersion: number;
  occurredAt: string;
  aggregateType: string;
  aggregateId: string;
  correlationId: string;
  payload: T;
}

/**
 * Line item representation in OrderCreated event payload (focused purely on inventory needs).
 */
export interface OrderCreatedItemPayload {
  productId: string;
  quantity: number;
}

/**
 * Business payload for OrderCreated events consumed by the downstream Inventory Service.
 */
export interface OrderCreatedPayload {
  orderId: string;
  customerId: string;
  items: OrderCreatedItemPayload[];
}

export type OrderCreatedEvent = EventEnvelope<OrderCreatedPayload>;

/**
 * Persistent database record representation of an outbox event.
 */
export interface OutboxEventRecord {
  id: string;
  aggregateType: string;
  aggregateId: string;
  eventType: string;
  eventVersion: number;
  correlationId: string;
  payload: OrderCreatedPayload;
  createdAt: Date;
  publishedAt: Date | null;
  retryCount: number;
}

export interface CreateOrderCreatedEventParams {
  eventId?: string;
  orderId: string;
  customerId: string;
  items: Array<{ productId: string; quantity: number }>;
  correlationId?: string;
  occurredAt?: Date;
}

/**
 * Factory to construct a typed, standardized OrderCreated event envelope.
 */
export function createOrderCreatedEvent(params: CreateOrderCreatedEventParams): OrderCreatedEvent {
  return {
    eventId: params.eventId ?? crypto.randomUUID(),
    eventType: 'OrderCreated',
    eventVersion: 1,
    occurredAt: (params.occurredAt ?? new Date()).toISOString(),
    aggregateType: 'Order',
    aggregateId: params.orderId,
    correlationId: params.correlationId ?? crypto.randomUUID(),
    payload: {
      orderId: params.orderId,
      customerId: params.customerId,
      items: params.items.map((item) => ({
        productId: item.productId,
        quantity: item.quantity,
      })),
    },
  };
}
