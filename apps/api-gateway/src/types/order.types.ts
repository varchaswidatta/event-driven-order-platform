export type OrderStatus = 'PENDING' | 'INVENTORY_PROCESSING' | 'CONFIRMED' | 'INVENTORY_FAILED';

export interface OrderItemDto {
  id?: string;
  orderId?: string;
  productId: string;
  quantity: number;
  unitPrice: string;
  createdAt?: string | Date;
}

export interface OrderDto {
  id: string;
  customerId: string;
  status: OrderStatus;
  totalAmount: string;
  currency: string;
  items: OrderItemDto[];
  createdAt: string | Date;
  updatedAt: string | Date;
}

export interface CreateOrderItemInput {
  productId: string;
  quantity: number;
  unitPrice: string;
}

export interface CreateOrderInput {
  customerId: string;
  items: CreateOrderItemInput[];
}
