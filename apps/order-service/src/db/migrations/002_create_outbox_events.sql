-- Migration: 002_create_outbox_events.sql
-- Description: Create outbox_events table and indexes for transactional outbox pattern

CREATE TABLE IF NOT EXISTS outbox_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type VARCHAR(64) NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type VARCHAR(128) NOT NULL,
  event_version INTEGER NOT NULL DEFAULT 1,
  payload JSONB NOT NULL,
  correlation_id UUID NOT NULL DEFAULT gen_random_uuid(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ NULL,
  retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0)
);

-- Partial index for high-throughput FIFO retrieval of unpublished events
CREATE INDEX IF NOT EXISTS idx_outbox_events_unpublished
  ON outbox_events (created_at ASC)
  WHERE published_at IS NULL;
