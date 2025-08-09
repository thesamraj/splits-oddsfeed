-- 002_odds_policies.sql
-- Harden performance and storage for odds hypertable

-- Ensure odds is a hypertable (safe if already done)
SELECT create_hypertable('odds','ts', if_not_exists => TRUE, migrate_data => TRUE);

-- Indexes used by /odds (filters + sort)
CREATE INDEX IF NOT EXISTS idx_odds_event_ts ON odds (event_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_odds_book_market_ts ON odds (book, market, ts DESC);

-- Enable compression and set strategy
ALTER TABLE odds SET (
  timescaledb.compress,
  timescaledb.compress_orderby = 'ts DESC',
  timescaledb.compress_segmentby = 'event_id, book, market'
);

-- Compress rows older than 7 days (keep last 7d uncompressed for hot reads)
SELECT add_compression_policy('odds', INTERVAL '7 days', if_not_exists => TRUE);

-- Retain 90 days of data total (auto-drop older chunks)
SELECT add_retention_policy('odds', INTERVAL '90 days', if_not_exists => TRUE);
