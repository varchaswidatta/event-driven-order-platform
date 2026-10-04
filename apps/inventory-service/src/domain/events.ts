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

// ─── Phase 7: Inventory Result Events ──────────────────────────────────────

/**
 * Line item included in inventory result event payloads.
 */
export interface InventoryEventItemPayload {
  productId: string;
  quantity: number;
}

/**
 * Business payload emitted when stock was successfully reserved for an order.
 */
export interface InventoryReservedPayload {
  orderId: string;
  reservationId: string;
  items: InventoryEventItemPayload[];
}

/**
 * Business payload emitted when stock reservation failed for an order.
 */
export interface InventoryReservationFailedPayload {
  orderId: string;
  reason: string;
  items: InventoryEventItemPayload[];
}

export type InventoryReservedEvent = EventEnvelope<InventoryReservedPayload>;
export type InventoryReservationFailedEvent = EventEnvelope<InventoryReservationFailedPayload>;
