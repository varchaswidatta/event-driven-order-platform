-- Migration: 001_create_orders.sql
-- Description: Create orders and order_items tables with constraints and indexes

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL,
  status VARCHAR(50) NOT NULL CHECK (
    status IN ('PENDING', 'INVENTORY_PROCESSING', 'CONFIRMED', 'INVENTORY_FAILED')
  ),
  total_amount NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
  currency VARCHAR(3) NOT NULL CHECK (length(currency) = 3),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id UUID NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  unit_price NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast retrieval of order items by order_id
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

-- Index for customer query optimization
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
