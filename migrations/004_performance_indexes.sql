-- 004_performance_indexes.sql
-- Add/confirm performance indexes for hot paths

-- Odds table indexes (idempotent)
CREATE INDEX IF NOT EXISTS idx_odds_book_ts ON odds (book, ts DESC);
CREATE INDEX IF NOT EXISTS idx_odds_event_id_only ON odds (event_id);
CREATE INDEX IF NOT EXISTS idx_odds_event_market ON odds (event_id, market);

-- Conditionally create odds_ticks indexes if table exists
DO $$
BEGIN
  IF to_regclass('public.odds_ticks') IS NOT NULL THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_odds_ticks_book_ts ON odds_ticks (book, ts DESC)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_odds_ticks_event_id ON odds_ticks (event_id)';
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_odds_ticks_event_market ON odds_ticks (event_id, market)';
  END IF;
END$$;
