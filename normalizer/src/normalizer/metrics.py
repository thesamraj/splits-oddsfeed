from prometheus_client import Histogram, Counter, Gauge

_INITIALIZED = True  # import-once guard per-process

kambi_e2e_latency_seconds = Histogram(
    "kambi_e2e_latency_seconds",
    "E2E latency from source ts to DB commit (seconds)",
    buckets=[0.1, 0.25, 0.5, 1, 2, 3, 5, 8, 13, 21],
)
kambi_rows_written_total = Counter(
    "kambi_rows_written_total", "Rows successfully written to DB"
)
kambi_e2e_skipped_total = Counter(
    "kambi_e2e_skipped_total", "Skipped Kambi envelopes with reason", ["reason"]
)

# Additional timing metrics
kambi_publish_to_normalize_ms = Histogram(
    "kambi_publish_to_normalize_ms",
    "Time from publish to normalize start (ms)",
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)
kambi_normalize_to_db_ms = Histogram(
    "kambi_normalize_to_db_ms",
    "Time from normalize to DB commit (ms)",
    buckets=[10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)
kambi_e2e_latency_ms = Histogram(
    "kambi_e2e_latency_ms",
    "End-to-end latency from source to DB (ms)",
    buckets=[100, 250, 500, 1000, 2000, 5000, 10000, 15000, 30000],
)
kambi_norm_backlog = Gauge("kambi_norm_backlog", "Normalizer queue depth")
