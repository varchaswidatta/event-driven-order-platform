import { ReservationStatus } from './reservation-status.js';

export interface ReservationItem {
  id: string;
  reservationId: string;
  productId: string;
  quantity: number;
  createdAt: Date;
}

export interface Reservation {
  id: string;
  orderId: string;
  status: ReservationStatus;
  items: ReservationItem[];
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateReservationInput {
  orderId: string;
  items: Array<{ productId: string; quantity: number }>;
}
