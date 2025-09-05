#!/usr/bin/env bash
set -euo pipefail
mkdir -p backups
echo "[backup_loop] running; will dump daily."
while true; do
  TS=$(date -u +%Y%m%d_%H%M%S)
  OUT="backups/pgdump_${TS}.sql.gz"
  if docker compose exec -T store pg_dump -U odds -d oddsfeed | gzip > "$OUT"; then
    echo "[backup_loop] wrote $OUT"
  else
    echo "[backup_loop][WARN] pg_dump failed"
  fi
  sleep 86400
done
