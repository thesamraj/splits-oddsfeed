-- 003_odds_dedupe.sql
-- Add deduplication constraints and data quality indices

-- Add index for data quality and deduplication
-- Note: TimescaleDB hypertables have complex unique constraint requirements
-- Instead we rely on application-level deduplication and monitoring
CREATE INDEX IF NOT EXISTS idx_odds_dedupe
ON odds (event_id, book, market, outcome_name, ts);

-- Add index to improve query performance on timestamp ranges
CREATE INDEX IF NOT EXISTS idx_odds_ts_range ON odds (ts);

-- Add index for data quality queries
CREATE INDEX IF NOT EXISTS idx_odds_sanity_check ON odds (outcome_price) WHERE outcome_price IS NOT NULL;
