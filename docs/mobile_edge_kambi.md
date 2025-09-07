## Kambi Mobile-Edge Requirements (Frozen until mobile IP is ready)

**Why frozen:** Direct/datacenter/residential pools (Bright Data, SmartProxy, SOAX) were blocked or timed out; TLS/SNI and IP reputation checks at the CDN edge prevent access.

**What's required to unblock:**
1. **Dedicated mobile proxy** on a US carrier in **NJ/PA/IL**, **sticky IPv4**, long-lived session (>=30–60 min).
2. **State targeting** (NJ first preference). Ability to force new IP on demand (API "reset").
3. **TLS/SNI passthrough** (no TLS terminating middleboxes), supports HTTP/2 and normal mobile ClientHello.
4. **Low latency** (<1s to Kambi edge) — critical for real-time odds.

**Vendors to try (order):** ProxyLTE (state-specific), The Social Proxy (unlimited 5G), LimeProxies (mobile GB plan). Avoid known-blocked mass pools.

**Integration plan once IP is ready:**
- Use simple `requests` (no proxy headers leaking), or Playwright **headful mobile profile** if needed, routed through the mobile IP.
- Start with BetRivers (Kambi) endpoints; expect 200/JSON under 1s when correct.
- Add sticky-session rotation logic + health checks (auto IP reset on failures).

**Status:** Kambi collectors disabled in compose. Enable only after a working mobile IP is provisioned.