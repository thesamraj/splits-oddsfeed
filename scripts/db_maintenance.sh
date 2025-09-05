#!/bin/bash
# Database Maintenance Script
# Runs automated cleanup and optimization

echo "[$(date)] Starting database maintenance..."

# Delete old data (keep only 24 hours)
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "
DELETE FROM odds WHERE ts < NOW() - INTERVAL '24 hours';
" 2>/dev/null

# Get row count after cleanup
ROWS=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "SELECT COUNT(*) FROM odds;" | tr -d ' ')
echo "[$(date)] Rows after cleanup: $ROWS"

# Vacuum and analyze
docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -c "
VACUUM ANALYZE odds;
" 2>/dev/null

# Check database size
SIZE=$(docker exec splits-oddsfeed-store-1 psql -U odds -d oddsfeed -t -c "SELECT pg_size_pretty(pg_database_size('oddsfeed'));" | tr -d ' ')
echo "[$(date)] Database size: $SIZE"

echo "[$(date)] Maintenance complete"
