#!/usr/bin/env bash
set -euo pipefail
: "${DATABASE_URL:?DATABASE_URL not set}"
psql "$DATABASE_URL" -t -c "SELECT NOW();" >/dev/null
echo "DB OK ($(date))"
