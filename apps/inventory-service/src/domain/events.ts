/**
 * Standard reusable application event envelope for all domain events.
 * Matches the contract established in Phase 3 by Order Service.
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
 * Line item representation in OrderCreated event payload.
 */
export interface OrderCreatedItemPayload {
  productId: string;
  quantity: number;
}

/**
 * Business payload for OrderCreated events published by Order Service.
 */
export interface OrderCreatedPayload {
  orderId: string;
  customerId: string;
  items: OrderCreatedItemPayload[];
}

export type OrderCreatedEvent = EventEnvelope<OrderCreatedPayload>;
