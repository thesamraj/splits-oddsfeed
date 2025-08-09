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

-- Create events table for normalized event data
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    league TEXT NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    home TEXT NOT NULL,
    away TEXT NOT NULL,
    sport TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create odds table for normalized odds data
CREATE TABLE IF NOT EXISTS odds (
    id BIGSERIAL,
    event_id TEXT NOT NULL REFERENCES events(id),
    book TEXT NOT NULL,
    market TEXT NOT NULL,
    line NUMERIC,
    price_home NUMERIC,
    price_away NUMERIC,
    price_over NUMERIC,
    price_under NUMERIC,
    total NUMERIC,
    ts TIMESTAMPTZ DEFAULT NOW(),
    outcome_name TEXT,
    outcome_price NUMERIC,
    outcome_point NUMERIC
);

-- Convert odds to hypertable for time-series optimization
SELECT create_hypertable('odds', 'ts', if_not_exists => TRUE);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_odds_event_id ON odds (event_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_odds_book ON odds (book, ts DESC);
CREATE INDEX IF NOT EXISTS idx_odds_market ON odds (market, ts DESC);
CREATE INDEX IF NOT EXISTS idx_events_league ON events (league, start_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_start_time ON events (start_time);

-- Create retention policy to automatically drop old data (optional)
-- Uncomment to automatically drop data older than 30 days
-- SELECT add_retention_policy('odds_events', INTERVAL '30 days');
-- SELECT add_retention_policy('odds', INTERVAL '30 days');
