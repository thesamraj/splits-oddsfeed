-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create odds_events table
CREATE TABLE IF NOT EXISTS odds_events (
    event_id TEXT NOT NULL,
    book TEXT NOT NULL,
    payload JSONB NOT NULL,
    seen_at TIMESTAMPTZ DEFAULT NOW()
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('odds_events', 'seen_at', if_not_exists => TRUE);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_odds_events_book_time ON odds_events (book, seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_odds_events_event_id ON odds_events (event_id);
CREATE INDEX IF NOT EXISTS idx_odds_events_payload_gin ON odds_events USING GIN (payload);

-- Create retention policy to automatically drop old data (optional)
-- Uncomment to automatically drop data older than 30 days
-- SELECT add_retention_policy('odds_events', INTERVAL '30 days');