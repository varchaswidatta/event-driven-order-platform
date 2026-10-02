import { z } from 'zod';

export const createOrderItemSchema = z.object({
  productId: z.string().uuid('Product ID must be a valid UUID'),
  quantity: z
    .number()
    .int('Quantity must be an integer')
    .positive('Quantity must be greater than 0'),
  unitPrice: z
    .string()
    .regex(
      /^\d+(\.\d{1,2})?$/,
      'Unit price must be a valid non-negative decimal string with up to 2 decimal places',
    ),
});

export const createOrderSchema = z.object({
  customerId: z.string().uuid('Customer ID must be a valid UUID'),
  currency: z
    .string()
    .length(3, 'Currency must be exactly 3 characters')
    .regex(/^[A-Z]{3}$/, 'Currency must be a 3-character uppercase ISO code')
    .default('USD'),
  items: z
    .array(createOrderItemSchema)
    .min(1, 'Order items must not be empty. At least one item is required'),
});

export type ValidatedCreateOrderInput = z.infer<typeof createOrderSchema>;
export type ValidatedCreateOrderItemInput = z.infer<typeof createOrderItemSchema>;
