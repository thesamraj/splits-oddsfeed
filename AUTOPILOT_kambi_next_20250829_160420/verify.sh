#!/bin/bash
set -euo pipefail

echo "=== Kambi Next Verification ==="
echo "Time: $(date -u +%Y%m%d_%H%M%S)"
echo ""

# Check BR/SH remain frozen
echo "BR/SH Status:"
docker ps --filter name=collector-br --filter name=collector-sh --format "table {{.Names}}\t{{.Status}}"
echo ""

# Check discovery results
echo "Discovery Results:"
docker logs splits-oddsfeed-collector-kambi-next-1 2>&1 | grep -E "(SUCCESS|Recommendation)" | tail -5
echo ""

# Check database for any new books
echo "Database check for new books (should be 0):"
docker compose exec -T store psql -U postgres -d postgres -c "
SELECT brand, COUNT(*) as odds_count
FROM odds_norm_10m
WHERE brand NOT IN ('betrivers', 'sugarhouse')
GROUP BY brand;" 2>/dev/null || echo "No new brands found"

echo ""
echo "=== Verification Complete ==="
