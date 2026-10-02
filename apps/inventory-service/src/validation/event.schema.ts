import { z } from 'zod';

/**
 * Validates the outer EventEnvelope structure received from Kafka.
 */
export const eventEnvelopeSchema = z.object({
  eventId: z.string().uuid('eventId must be a valid UUID'),
  eventType: z.string().min(1, 'eventType must not be empty'),
  eventVersion: z.number().int().positive('eventVersion must be a positive integer'),
  occurredAt: z.string().min(1, 'occurredAt must not be empty'),
  aggregateType: z.string().min(1, 'aggregateType must not be empty'),
  aggregateId: z.string().uuid('aggregateId must be a valid UUID'),
  correlationId: z.string().uuid('correlationId must be a valid UUID'),
  payload: z.unknown(),
});

/**
 * Validates the OrderCreated event payload fields.
 */
export const orderCreatedPayloadSchema = z.object({
  orderId: z.string().uuid('orderId must be a valid UUID'),
  customerId: z.string().uuid('customerId must be a valid UUID'),
  items: z
    .array(
      z.object({
        productId: z.string().uuid('productId must be a valid UUID'),
        quantity: z
          .number()
          .int('quantity must be an integer')
          .positive('quantity must be greater than 0'),
      }),
    )
    .min(1, 'items must not be empty'),
});

export type ValidatedEventEnvelope = z.infer<typeof eventEnvelopeSchema>;
export type ValidatedOrderCreatedPayload = z.infer<typeof orderCreatedPayloadSchema>;
