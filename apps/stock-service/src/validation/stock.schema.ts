import { z } from 'zod';

export const stockItemSchema = z.object({
  productId: z.string().uuid('productId must be a valid UUID'),
  quantity: z
    .number()
    .int('quantity must be an integer')
    .positive('quantity must be greater than 0'),
});

export const reserveStockRequestSchema = z.object({
  orderId: z.string().uuid('orderId must be a valid UUID'),
  items: z.array(stockItemSchema).min(1, 'items must not be empty'),
});

export type ValidatedStockItem = z.infer<typeof stockItemSchema>;
export type ValidatedReserveStockRequest = z.infer<typeof reserveStockRequestSchema>;
