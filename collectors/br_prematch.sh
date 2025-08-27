#!/usr/bin/env bash
set -euo pipefail
BRAND_HINT="betrivers"
TOKEN="${TOKEN:-rsi2uspa}"
BASE="${BASE:-https://eu.offering-api.kambicdn.com/offering/v2018}"
ENDPOINTS="${ENDPOINTS:-event/open.json,event/live/open.json}"
SLEEP="${SLEEP:-60}"
REDIS="${REDIS:-redis://broker:6379/0}"
HEALTHZ_PORT="${HEALTHZ_PORT:-9129}"

published=0; last_ts=0
python3 - <<PY &>/dev/null &
from http.server import BaseHTTPRequestHandler, HTTPServer
import json, time, os
class H(BaseHTTPRequestHandler):
  def do_GET(self):
    if self.path!='/healthz': self.send_response(404); self.end_headers(); return
    self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers()
    d={"status":"active","brand":"betrivers","mode":"prematch","messages_published":int(os.getenv("PUBLISHED","0")), "last_publish_ts":float(os.getenv("LAST_TS","0"))}
    self.wfile.write(json.dumps(d).encode())
HTTPServer(("0.0.0.0",int(os.getenv("HEALTHZ_PORT","9129"))),H).serve_forever()
PY

while true; do
  ok=0
  IFS=',' read -ra eps <<< "$ENDPOINTS"
  for ep in "${eps[@]}"; do
    url="${BASE}/${TOKEN}/${ep}"
    resp="$(curl -fsS --max-time 10 -H 'Accept: application/json' "$url" 2>/dev/null || true)"
    status=$?
    if [ $status -eq 0 ] && [ -n "$resp" ]; then
      # publish envelope to redis
      docker exec -i splits-oddsfeed-broker-1 redis-cli PUBLISH odds.raw.kambi "{\"brand_hint\":\"$BRAND_HINT\",\"transport\":\"http\",\"mode\":\"prematch\",\"url\":\"$url\",\"payload\":$(echo "$resp" | jq -c .)}" >/dev/null 2>&1 || true
      published=$((published+1)); last_ts="$(date +%s)"
      export PUBLISHED="$published"; export LAST_TS="$last_ts"
      ok=1; break
    fi
  done
  if [ $ok -eq 0 ]; then sleep 90; else sleep "$SLEEP"; fi
done
