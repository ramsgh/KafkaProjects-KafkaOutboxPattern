CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    customer_name TEXT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS consumed_events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
