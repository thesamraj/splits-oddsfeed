# Engineering Audit — splits-oddsfeed

## Executive Summary
- High: Bovada enhanced collector references an undefined metric (`PARSER_SCHEMA_CHANGES`) causing runtime errors when schema drifts; fixed by adding the counter.
- High: `scripts/verify_book.sh` queried non-existent DB columns (`created_at`, `market_type`); corrected to `ts` and `market` to prevent false failures.
- Med: `docker-compose.prod.yml` exposes ports `9090/9091` on host; prefer internal-only and map `19090/19091` in local overrides to reduce attack surface.
- Med: WebSocket collectors (e.g., DK) have reconnection logic but lack explicit backpressure/memory caps and Prometheus metrics; risk of memory growth under high rate.
- Med: Metrics proxy lacks `/targets` and `/readiness`; adding them improves ops visibility and liveness semantics.
- Med: DB indexes largely present; added a conditional migration to ensure hot-path btree indexes for `odds` and (if present) `odds_ticks`.
- Low: Slight metrics inconsistency (`odds_15m` not present on Bovada) and mixed use of simple vs explainable realness gates across collectors.
- Low: Docs are strong overall; added exact curl snippets, rollout gating guidance, and CI-secret failure playbook.

## Top Risks
- Undefined metric in Bovada: Impact high × Likelihood medium — Owner: Collectors — ETA: Now.
- Prod ports exposed (9090/9091): Impact medium × Likelihood medium — Owner: Infra — ETA: Next.
- WS memory/backpressure gaps: Impact medium × Likelihood medium — Owner: Collectors — ETA: Next.
- Verify script DB mismatches: Impact medium × Likelihood low — Owner: Ops — ETA: Now.
- Metrics proxy endpoints missing: Impact low × Likelihood medium — Owner: Ops — ETA: Next.
- Index drift on `odds_ticks`: Impact medium × Likelihood low — Owner: Data — ETA: Later.

## Quick Wins
- Add `PARSER_SCHEMA_CHANGES` counter in `collectors/bovada_real/bovada_real_enhanced.py` (done). Ensures schema drift is observable without exceptions.
- Fix `verify_book.sh` to use `ts` and `market` columns (done). Prevents false negatives in prod verification.
- Add `migrations/004_performance_indexes.sql` with conditional creation of hot-path indexes (done). Improves typical query plans.
- Remove `ports: 9090/9091` from `docker-compose.prod.yml` and keep host maps only in local overrides. Safer surface area.
- Add `/targets` and `/readiness` to `services/metrics_proxy/metrics_proxy.py`. Better SRE ergonomics.
- Cap DK/FD WS in-memory buffers (e.g., max len on windows/subscriptions), expose memory gauges, and count dropped messages for backpressure.
- Standardize `odds_15m` on Bovada; parity with Kambi metrics for dashboards.
- Align RUNBOOK SLO to `realness_score >= 0.9` (currently suggests 0.8 in SLOs).

## Gaps by Area

Collectors
- Bovada (enhanced):
  - Snapshot + ID-based dedup present (`compute_snapshot_hash`, `build_market_key` with price banding). Good.
  - ExplainableRealnessGate wired with warm-up/quarantine and `/realness/report`. Good.
  - Metrics on 9090 and health via Flask on 9091. Good.
  - Bug: `PARSER_SCHEMA_CHANGES` counter referenced but not defined — fixed.
- Kambi Unified:
  - Brand switch via `KAMBI_BRAND`. Good.
  - Retry/backoff and token-bucket rate-limiter; proxy via `PROXY_URL`. Good.
  - Uses a simple realness gate; consider migrating to ExplainableRealness for parity.
- WebSocket (DK):
  - Reconnection, heartbeat, and error handling present. Good foundation.
  - Missing: explicit backpressure (drop/queue caps), memory gauges, and Prometheus counters; consider sliding buffers with max size and drop metrics.

Normalizer
- Base normalizer has per-message try/except and idempotent upsert (ON CONFLICT). Good.
- Metrics present; health on 9091. Good.
- Quarantine flow is implemented in ExplainableRealness (collector side), not in normalizer — acceptable given current split.

Infra / Ops
- `render.yaml`: workers have no `healthCheckPath`; web services do. Secrets `sync: false`. Good.
- `docker-compose.prod.yml`: no sandbox/fake services. Exposes `9090/9091` on host; recommend internal-only with local override mapping to `19090/19091` only.
- Scripts: `verify_book.sh` robust fallbacks and 90s cap; fixed DB column names. `deny_generators.sh` portable regex and dry-run. `verify_no_secrets.sh` scoped patterns. Good.
- CI/CD: `secret_scan.yml`, `nightly_verify.yml`, `neon_retention.yml`, `progressive_rollout.yml` present and sensible. Good.

Data & DB
- Schema: `odds`, `events`, `odds_events` present; hypertables used. Indexes on `(book, ts DESC)` and `(event_id, ts DESC)` exist. Good.
- Added migration to ensure `(event_id, market)` for `odds` and conditional indexes for `odds_ticks`.
- Retention: 45/90 days enforced via weekly workflow; vacuum/autoanalyze defaults should be fine on Neon, but document timescale maintenance if usage grows.

Security
- `.env.example` uses placeholders; `.gitignore` protects `.env`. No hardcoded secrets found in scan paths. Actions use secrets.
- Added doc section for handling CI secret-scan failures and Render rotation specifics.

Observability
- Prometheus: `realness_score`, `ticks_total`, `collector_up`, `dedup_dropped_total{reason}`, upstream SLIs recorded in ExplainableRealness. Good.
- Kambi exports `odds_15m`; Bovada does not — recommend parity.
- Metrics proxy aggregates but lacks `/targets` and `/readiness` endpoints.

CI/CD
- Secret scans, denylist, nightly verify, and weekly retention workflows configured and referenced from docs.

Docs
- RUNBOOK, VERIFY, SECURITY present and strong. Added exact curl snippets, gating guidance, and CI failure playbook.

## Rollout Readiness (13 books)
- Blocks:
  - WS collectors: add backpressure/memory metrics before 24/7 ops.
  - Remove prod exposure of 9090/9091; rely on internal scrape or public proxy only.
  - Standardize realness gate usage and `odds_15m` coverage for cross-book dashboards.
- Green:
  - Dedup, explainable realness and quarantine path for Bovada.
  - Kambi unified with rate-limit/backoff/proxy.
  - CI/CD, secret scanning, retention workflows.
