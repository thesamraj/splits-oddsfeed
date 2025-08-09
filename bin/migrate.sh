#!/usr/bin/env bash
set -euo pipefail

PSQL="docker compose exec -T store psql -U odds -d oddsfeed -v ON_ERROR_STOP=1"

# Wait for DB ready (simple loop)
for i in {1..30}; do
  if $PSQL -c "SELECT 1" >/dev/null 2>&1; then break; fi
  echo "waiting for store..."
  sleep 1
done

# Create migrations table
$PSQL -c "CREATE TABLE IF NOT EXISTS schema_migrations (
  version text PRIMARY KEY,
  applied_at timestamptz DEFAULT now()
);"

# Apply new migrations
shopt -s nullglob
for f in $(ls -1 migrations/*.sql | sort); do
  base="$(basename "$f")"
  already="$($PSQL -Atc "SELECT 1 FROM schema_migrations WHERE version = '$base' LIMIT 1" || true)"
  if [[ "$already" == "1" ]]; then
    echo "SKIP $base"
    continue
  fi
  echo "APPLY $base"
  # Copy file to container and run it
  docker compose cp "$f" store:/tmp/"$base"
  $PSQL -f "/tmp/$base"
  $PSQL -c "INSERT INTO schema_migrations(version) VALUES ('$base') ON CONFLICT DO NOTHING;"
done
echo "migrations complete"
