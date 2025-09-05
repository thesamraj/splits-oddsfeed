-- Database optimization queries

-- Add missing indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_odds_ts_book ON odds(ts DESC, book);
CREATE INDEX IF NOT EXISTS idx_odds_event_market ON odds(event_id, market);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_sport ON events(sport);

-- Add partitioning for odds table (by day)
-- This helps with query performance and data retention
CREATE TABLE IF NOT EXISTS odds_partitioned (
    LIKE odds INCLUDING ALL
) PARTITION BY RANGE (ts);

-- Create partitions for the next 7 days
DO $$
DECLARE
    start_date date := CURRENT_DATE;
    end_date date := CURRENT_DATE + 7;
    partition_date date;
BEGIN
    FOR partition_date IN
        SELECT generate_series(start_date, end_date, '1 day'::interval)::date
    LOOP
        BEGIN
            EXECUTE format('CREATE TABLE IF NOT EXISTS odds_%s PARTITION OF odds_partitioned
                           FOR VALUES FROM (%L) TO (%L)',
                           to_char(partition_date, 'YYYYMMDD'),
                           partition_date,
                           partition_date + 1);
        EXCEPTION WHEN duplicate_table THEN
            NULL;
        END;
    END LOOP;
END$$;

-- Vacuum and analyze tables for better performance
VACUUM ANALYZE odds;
VACUUM ANALYZE events;
VACUUM ANALYZE odds_ticks;

-- Update table statistics
ANALYZE;
