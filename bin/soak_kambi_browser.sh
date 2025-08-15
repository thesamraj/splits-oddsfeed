#!/usr/bin/env bash
set -euo pipefail
BOOK="kambi"; WINDOW="15"; SAMPLES=18; SLEEP=10
best=999999; worst=0; counts=()
echo "=== Starting 3-minute soak test ==="
for i in $(seq 1 ${SAMPLES}); do
  count=$(curl -fsS "http://localhost:8080/odds?book=${BOOK}&minutes=${WINDOW}&limit=500" | jq -r '.count // 0' || echo 0)
  fresh=$(docker compose exec -T store psql -U odds -d oddsfeed -tAc \
    "SELECT COALESCE(EXTRACT(EPOCH FROM (now()-max(ts))),999999)::int FROM odds WHERE book='${BOOK}';" | tr -d '[:space:]' || echo 999999)
  (( fresh < best )) && best=$fresh
  (( fresh > worst )) && worst=$fresh
  counts+=("$count")
  echo "t=$i count=$count freshness_s=$fresh"
  sleep ${SLEEP}
done
echo "----- SOAK SUMMARY -----"
echo "samples=${SAMPLES} interval=${SLEEP}s window=${WINDOW}m"
echo "counts: ${counts[*]}"
echo "best_fresh_s=${best} p95_fresh_s~=${worst}"
# Success criteria: API count ≥ 3 during run and p95 freshness ≤ 30s
min_count=$(printf '%s\n' "${counts[@]}" | sort -n | head -1)
if [[ "$min_count" -ge 3 && "$worst" -le 30 ]]; then
  echo "✅ SOAK TEST PASSED: min_count=$min_count≥3, p95_fresh=$worst≤30s"
else
  echo "❌ SOAK TEST FAILED: min_count=$min_count, p95_fresh=${worst}s"
fi
