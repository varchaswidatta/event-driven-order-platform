export const RESERVATION_STATUS = {
  PENDING: 'PENDING',
} as const;

export type ReservationStatus = (typeof RESERVATION_STATUS)[keyof typeof RESERVATION_STATUS];

export const VALID_RESERVATION_STATUSES: readonly ReservationStatus[] =
  Object.values(RESERVATION_STATUS);
