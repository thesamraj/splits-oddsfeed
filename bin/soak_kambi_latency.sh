#!/usr/bin/env bash
set -euo pipefail
END=$((SECONDS+600))
SAMPLES=0
echo "soak: sampling normalizer metrics on :9200 for ~10m…"
while [ $SECONDS -lt $END ]; do
  curl -fsS localhost:9200/metrics | awk '/^kambi_e2e_latency_seconds_bucket/ {print}' >> /tmp/kambi_hist.$$
  sleep 30
  SAMPLES=$((SAMPLES+1))
done
echo "samples: $SAMPLES" >&2

# Compute p50/p95 from Prom buckets (simple L-infinity interpolation)
python3 - <<'PY'
import re, sys, collections
lines=open("/tmp/kambi_hist.$$").read().splitlines()
bucket=re.compile(r'^kambi_e2e_latency_seconds_bucket\{le="([^"]+)"\}\s+([0-9\.]+)')
counts=collections.OrderedDict()
for L in lines:
    m=bucket.match(L)
    if not m: continue
    le=float(m.group(1).replace("+Inf","inf"))
    v=float(m.group(2))
    counts[le]=v
# Use the last cumulative snapshot per bucket
keys=sorted(counts.keys(), key=lambda x: float('inf') if x==float('inf') else x)
vals=[counts[k] for k in keys]
if not vals:
    print("p50=n/a  p95=n/a  (no histogram samples)")
    sys.exit(0)
total=vals[-1]
def quantile(q):
    target=total*q
    for k,v in zip(keys,vals):
        if v>=target:
            return k
    return keys[-1]
p50=quantile(0.5)
p95=quantile(0.95)
print(f"p50~{p50}s  p95~{p95}s  total_samples={int(total)}")
PY
