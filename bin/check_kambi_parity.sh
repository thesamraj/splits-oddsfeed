#!/usr/bin/env bash
set -euo pipefail
BOOK="${1:-kambi}"
MIN="${2:-15}"
TOL="${3:-5}"
DBE=$(docker compose exec -T store psql -U odds -d oddsfeed -tAc \
  "SELECT COALESCE(COUNT(DISTINCT event_id),0) FROM odds WHERE book='${BOOK}' AND ts > now() - interval '${MIN} minutes';" \
  | tr -d '[:space:]' || echo 0)
APIC=$(curl -fsS "http://localhost:8080/odds?book=${BOOK}&minutes=${MIN}&limit=999" \
  | jq -r '.count // 0' || echo 0)
echo "DB events(${MIN}m)=${DBE} API count=${APIC} tol=${TOL}"
if [[ "${DBE}" -gt "${APIC}" ]]; then diff=$(( DBE - APIC )); else diff=$(( APIC - DBE )); fi
test "${diff}" -le "${TOL}" && echo "PARITY OK" || { echo "PARITY FAIL"; exit 1; }
