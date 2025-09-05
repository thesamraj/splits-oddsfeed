# Prometheus Alert Rules

## Critical Alerts

```yaml
groups:
  - name: oddsfeed_critical
    interval: 30s
    rules:
      # Collector down for 5 minutes
      - alert: CollectorDown
        expr: collector_up == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Collector {{ $labels.book }} is down"
          description: "Collector {{ $labels.book }} has been down for 5 minutes"

      # No ticks for 10 minutes
      - alert: NoTicks
        expr: increase(ticks_total[10m]) == 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "No ticks from {{ $labels.book }}"
          description: "{{ $labels.book }} has not produced ticks in 10 minutes"

      # Realness score too low
      - alert: LowRealnessScore
        expr: realness_score < 0.9
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Low realness score for {{ $labels.book }}"
          description: "{{ $labels.book }} realness score is {{ $value }} (threshold: 0.9)"

      # Too many 429 errors
      - alert: RateLimitExceeded
        expr: increase(http_429_total[10m]) > 100
        labels:
          severity: warning
        annotations:
          summary: "Rate limiting on {{ $labels.book }}"
          description: "{{ $labels.book }} received {{ $value }} 429 errors in 10 minutes"
```

## Example Grafana Queries

```promql
# Collector status dashboard
collector_up

# Ticks per minute by book
rate(ticks_total[1m])

# Odds upserts per minute
rate(odds_upserts_total[1m])

# Average realness score by book
realness_score

# Error rate by type
rate(errors_total[5m])

# 429 errors in last hour
increase(http_429_total[1h])

# Last successful collection (seconds ago)
time() - last_success_ts
```
