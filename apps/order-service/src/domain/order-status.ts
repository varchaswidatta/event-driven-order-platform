export const ORDER_STATUS = {
  PENDING: 'PENDING',
  INVENTORY_PROCESSING: 'INVENTORY_PROCESSING',
  CONFIRMED: 'CONFIRMED',
  INVENTORY_FAILED: 'INVENTORY_FAILED',
} as const;

export type OrderStatus = (typeof ORDER_STATUS)[keyof typeof ORDER_STATUS];

export const VALID_ORDER_STATUSES: readonly OrderStatus[] = Object.values(ORDER_STATUS);
