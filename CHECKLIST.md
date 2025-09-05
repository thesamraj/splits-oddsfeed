# Production Readiness Checklist

| Check | Status |
|---|---|
| No sandbox/fake services in prod compose/render | ✅ |
| Internal metrics/health ports standardized (9090/9091); host maps to 19090/19091 only in local overrides | ⚠️ docker-compose.prod.yml exposes 9090/9091; recommend removing host mappings |
| Render workers have no healthCheckPath; web services do | ✅ |
| RealnessGate enabled with warm-up and quarantine; `/realness/report` present | ✅ (Bovada), basic gate for Kambi |
| CI secret scan + denylist pass workflows present | ✅ |
| Nightly verify + weekly retention configured | ✅ |

Notes:
- Consider adding `/targets` and `/readiness` to `services/metrics_proxy/metrics_proxy.py` for better observability.
- Add `odds_15m` metric to Bovada for consistency across collectors.
