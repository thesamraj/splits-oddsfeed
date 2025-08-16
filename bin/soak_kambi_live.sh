#!/usr/bin/env bash
set -euo pipefail
MINUTES="${1:-15}"
SAMPLES=18  # 3 min @ 10s
SLEEP=10
echo "Starting live soak: ${SAMPLES} samples, ${SLEEP}s interval, window=${MINUTES}m"
for i in $(seq 1 "${SAMPLES}"); do
  # simple proxy via 0.5s bucket as approximation:
  P50A=$(curl -fsS http://localhost:9200/metrics | awk -F'[ {}"]+' '/kambi_e2e_latency_seconds_bucket.*le="0.5"/{s+=$NF} END{print (s>0)?0.5:1.0}')
  P95A=$(curl -fsS http://localhost:9200/metrics | awk -F'[ {}"]+' '/kambi_e2e_latency_seconds_bucket.*le="5"/{s+=$NF} END{print (s>0)?5:8}')
  FRESH=$(curl -fsS http://localhost:9200/metrics | awk '/^kambi_last_insert_ts_seconds/{ts=$2; printf "%.0f", ts}' | xargs -I {} bash -c 'echo $(($(date +%s) - {}))')
  API_CNT=$(curl -fsS "http://localhost:8080/odds?book=kambi&minutes=${MINUTES}" | jq -r '.count // 0')
  echo "$(date -Is)  approx_p50=${P50A}s  approx_p95=${P95A}s  freshness=${FRESH}s  api_count=${API_CNT}"
  sleep "${SLEEP}"
done
