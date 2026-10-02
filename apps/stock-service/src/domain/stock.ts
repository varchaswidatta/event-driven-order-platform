export interface Product {
  id: string;
  sku: string;
  name: string;
  price: string;
  createdAt: Date;
  updatedAt: Date;
}

export interface Stock {
  id: string;
  productId: string;
  availableQuantity: number;
  reservedQuantity: number;
  updatedAt: Date;
}

export interface StockReservationItem {
  id: string;
  reservationId: string;
  productId: string;
  quantity: number;
  createdAt: Date;
}

export interface StockReservation {
  id: string;
  orderId: string;
  status: 'RESERVED' | 'RELEASED';
  items?: StockReservationItem[];
  createdAt: Date;
  updatedAt: Date;
}

export interface StockItemInput {
  productId: string;
  quantity: number;
}

export interface ReserveStockInput {
  orderId: string;
  items: StockItemInput[];
}

export interface ReserveStockResult {
  success: boolean;
  reservationId?: string;
  failureReason?: string;
  alreadyExisted?: boolean;
}
