import { OrderStatus } from './order-status.js';

export interface OrderItem {
  id: string;
  orderId: string;
  productId: string;
  quantity: number;
  unitPrice: string; // Represented as fixed-point decimal string matching PostgreSQL NUMERIC(12,2)
  createdAt: Date;
}

export interface Order {
  id: string;
  customerId: string;
  status: OrderStatus;
  totalAmount: string; // Represented as fixed-point decimal string matching PostgreSQL NUMERIC(12,2)
  currency: string;
  items: OrderItem[];
  createdAt: Date;
  updatedAt: Date;
}

export interface CreateOrderItemInput {
  productId: string;
  quantity: number;
  unitPrice: string;
}

export interface CreateOrderInput {
  id?: string;
  customerId: string;
  currency?: string;
  correlationId?: string;
  items: CreateOrderItemInput[];
}
