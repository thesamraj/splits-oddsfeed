#!/usr/bin/env bash
set -euo pipefail

# Ensure Python + Playwright available
python3 - <<'PY'
import sys, subprocess
try:
    import playwright  # type: ignore
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", "playwright>=1.45,<1.48"])
    import playwright
# No local browser download needed for connect_over_cdp, but Playwright needs its deps
try:
    subprocess.check_call([sys.executable, "-m", "playwright", "install-deps"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
except Exception:
    pass
PY

export BRD_WSS="${BRD_WSS:-}"
export TEST_URL="${TEST_URL:-}"
export BROWSER_TIMEOUT_MS="${BROWSER_TIMEOUT_MS:-45000}"

OUT_JSON="$(python3 services/kambi_browser/brightdata_browser.py)"
echo "$OUT_JSON" | jq -C . || echo "$OUT_JSON"

OK="$(echo "$OUT_JSON" | jq -r '.ok' 2>/dev/null || echo false)"
STATUS="$(echo "$OUT_JSON" | jq -r '.status // "null"')"
REASON="$(echo "$OUT_JSON" | jq -r '.reason // ""')"

echo
if [ "$OK" = "true" ]; then
  echo "✅ PASS — Browser API fetched: status=${STATUS}"
  exit 0
fi

echo "❌ FAIL — reason=${REASON} status=${STATUS}"
echo "Troubleshooting:"
echo "  • If reason=auth_failed or 407: In Bright Data → Browser API → zone 'kambi_browser', copy the NEW WSS (password rotates)"
echo "  • If timeout/502: ensure Premium domains is ON, try state targeting (e.g. add '-country-us-state-nj' to username), or try UA mobile"
echo "  • To switch to mobile UA: add '-ua-mobile' to the username part of the WSS before the ':'"
echo "  • If connect errors persist: check IP allowlist in Bright Data or toggle a fresh session (add '-session-<rand>' to username)"
exit 1