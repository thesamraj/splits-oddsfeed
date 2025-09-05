#!/usr/bin/env bash
set -euo pipefail
TS=$(date +%Y%m%d_%H%M%S)
docker exec splits-oddsfeed-store-1 pg_dump -U odds oddsfeed | gzip > "backup_${TS}.sql.gz"
ls -lh "backup_${TS}.sql.gz"
