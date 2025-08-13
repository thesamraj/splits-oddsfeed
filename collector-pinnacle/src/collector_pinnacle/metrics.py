"""Prometheus metrics for Pinnacle collector."""

from prometheus_client import Counter, Histogram, Gauge


# Core fetch metrics
collector_fetch_total = Counter(
    "collector_fetch_total", "Total API fetch attempts", ["source", "status"]
)

collector_fetch_duration_seconds = Histogram(
    "collector_fetch_duration_seconds", "API fetch duration", ["source"]
)

collector_last_success_timestamp_seconds = Gauge(
    "collector_last_success_timestamp_seconds",
    "Timestamp of last successful fetch",
    ["source"],
)

# Budget and rate limiting
collector_budget_daily_total = Gauge(
    "collector_budget_daily_total", "Daily request budget usage", ["source"]
)

collector_guardrail_skips_total = Counter(
    "collector_guardrail_skips_total",
    "Number of skipped requests due to guardrails",
    ["source", "reason"],
)
