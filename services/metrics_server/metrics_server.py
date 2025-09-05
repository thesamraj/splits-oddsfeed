#!/usr/bin/env python3
"""
Prometheus Metrics Server
Centralized metrics endpoint for all services
"""

import os
import json
import redis
import psycopg2
from flask import Flask, Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY
from datetime import datetime, timezone
import logging
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metrics_server")

# Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder
PORT = int(os.getenv("METRICS_PORT", 9090))

app = Flask(__name__)

# Define metrics
books_active = Gauge("oddsfeed_books_active", "Number of active sportsbooks")
events_total = Gauge("oddsfeed_events_total", "Total events in database")
odds_records_total = Gauge(
    "oddsfeed_odds_records_total", "Total odds records in database"
)
odds_records_rate = Gauge("oddsfeed_odds_records_rate", "Odds records per minute")
database_size_bytes = Gauge("oddsfeed_database_size_bytes", "Database size in bytes")
redis_messages_rate = Gauge(
    "oddsfeed_redis_messages_rate", "Redis messages per minute", ["channel"]
)
realness_scores = Histogram(
    "oddsfeed_realness_score", "Distribution of realness scores", ["book"]
)
collector_up = Gauge(
    "oddsfeed_collector_up", "Collector status (1=up, 0=down)", ["book"]
)
normalizer_errors = Counter(
    "oddsfeed_normalizer_errors_total", "Total normalizer errors", ["book"]
)
api_requests = Counter(
    "oddsfeed_api_requests_total", "Total API requests", ["endpoint", "status"]
)
storage_latency = Histogram(
    "oddsfeed_storage_latency_seconds", "Database write latency"
)


class MetricsCollector:
    """Background metrics collector"""

    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.running = True
        self.last_odds_count = 0
        self.last_check_time = time.time()

    def get_db_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )

    def collect_database_metrics(self):
        """Collect metrics from database"""
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()

            # Active books (last 5 minutes)
            cur.execute(
                """
                SELECT COUNT(DISTINCT book)
                FROM odds
                WHERE ts > NOW() - INTERVAL '5 minutes'
            """
            )
            active_books = cur.fetchone()[0]
            books_active.set(active_books)

            # Total events
            cur.execute("SELECT COUNT(*) FROM events")
            total_events = cur.fetchone()[0]
            events_total.set(total_events)

            # Total odds records
            cur.execute("SELECT COUNT(*) FROM odds")
            total_odds = cur.fetchone()[0]
            odds_records_total.set(total_odds)

            # Calculate rate
            current_time = time.time()
            time_diff = current_time - self.last_check_time
            if time_diff > 0 and self.last_odds_count > 0:
                records_diff = total_odds - self.last_odds_count
                rate_per_minute = (records_diff / time_diff) * 60
                odds_records_rate.set(max(0, rate_per_minute))

            self.last_odds_count = total_odds
            self.last_check_time = current_time

            # Database size
            cur.execute(f"SELECT pg_database_size('{DB_NAME}')")
            db_size = cur.fetchone()[0]
            database_size_bytes.set(db_size)

            # Collector status (check last update per book)
            cur.execute(
                """
                SELECT book, MAX(ts) as last_update
                FROM odds
                WHERE ts > NOW() - INTERVAL '10 minutes'
                GROUP BY book
            """
            )

            results = cur.fetchall()
            known_books = [
                "bovada",
                "draftkings",
                "fanduel",
                "betmgm",
                "caesars",
                "pointsbet",
                "barstool",
                "betrivers",
                "unibet",
                "betano",
                "stake",
                "betonline",
                "pinnacle",
            ]

            # Set all to 0 first
            for book in known_books:
                collector_up.labels(book=book).set(0)

            # Set active ones to 1
            for book, last_update in results:
                if last_update:
                    age_seconds = (
                        datetime.now(last_update.tzinfo) - last_update
                    ).total_seconds()
                    if age_seconds < 300:  # Active if updated in last 5 minutes
                        collector_up.labels(book=book).set(1)

            cur.close()
            conn.close()

        except Exception as e:
            logger.error(f"Error collecting database metrics: {e}")

    def collect_redis_metrics(self):
        """Collect metrics from Redis"""
        try:
            # Check message rates on channels
            channels = [
                "odds.raw.bovada",
                "odds.raw.draftkings",
                "odds.raw.fanduel",
                "odds.normalized",
            ]

            for channel in channels:
                try:
                    # Use Redis INFO to get channel stats if available
                    pubsub = self.redis_client.pubsub()
                    pubsub.subscribe(channel)

                    # This is a simple check - in production you'd track actual message rates
                    redis_messages_rate.labels(channel=channel).set(0)

                    pubsub.unsubscribe()
                    pubsub.close()
                except:
                    pass

            # Check for realness scores in Redis
            realness_keys = self.redis_client.keys("realness:*")
            for key in realness_keys[:100]:  # Limit to prevent overload
                try:
                    data = self.redis_client.get(key)
                    if data:
                        score_data = json.loads(data)
                        book = score_data.get("book", "unknown")
                        score = score_data.get("score", 0)
                        realness_scores.labels(book=book).observe(score)
                except:
                    pass

        except Exception as e:
            logger.error(f"Error collecting Redis metrics: {e}")

    def run(self):
        """Background collection loop"""
        while self.running:
            try:
                self.collect_database_metrics()
                self.collect_redis_metrics()
            except Exception as e:
                logger.error(f"Error in metrics collection: {e}")

            time.sleep(30)  # Collect every 30 seconds

    def stop(self):
        """Stop the collector"""
        self.running = False


# Global collector instance
collector = MetricsCollector()


@app.route("/metrics")
def metrics():
    """Prometheus metrics endpoint"""
    try:
        api_requests.labels(endpoint="/metrics", status="success").inc()
        return Response(generate_latest(REGISTRY), mimetype="text/plain")
    except Exception as e:
        api_requests.labels(endpoint="/metrics", status="error").inc()
        logger.error(f"Error generating metrics: {e}")
        return Response("Error generating metrics", status=500)


@app.route("/health")
def health():
    """Health check endpoint"""
    try:
        # Quick DB check
        conn = collector.get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()

        # Quick Redis check
        collector.redis_client.ping()

        api_requests.labels(endpoint="/health", status="success").inc()
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        api_requests.labels(endpoint="/health", status="error").inc()
        return {"status": "unhealthy", "error": str(e)}, 500


@app.route("/")
def index():
    """Index page with links"""
    html = """
    <html>
    <head><title>OddsFeed Metrics Server</title></head>
    <body>
        <h1>OddsFeed Metrics Server</h1>
        <ul>
            <li><a href="/metrics">Prometheus Metrics</a></li>
            <li><a href="/health">Health Check</a></li>
        </ul>
        <h2>Grafana Dashboard</h2>
        <p>Import the following JSON into Grafana for a pre-built dashboard:</p>
        <details>
        <summary>Click to expand dashboard JSON</summary>
        <pre>{
  "dashboard": {
    "title": "OddsFeed Production Metrics",
    "panels": [
      {
        "title": "Active Sportsbooks",
        "targets": [{"expr": "oddsfeed_books_active"}]
      },
      {
        "title": "Odds Records Rate",
        "targets": [{"expr": "rate(oddsfeed_odds_records_total[5m])"}]
      },
      {
        "title": "Collector Status",
        "targets": [{"expr": "oddsfeed_collector_up"}]
      },
      {
        "title": "Realness Scores",
        "targets": [{"expr": "histogram_quantile(0.5, oddsfeed_realness_score)"}]
      },
      {
        "title": "Database Size",
        "targets": [{"expr": "oddsfeed_database_size_bytes / 1024 / 1024 / 1024"}]
      }
    ]
  }
}</pre>
        </details>
    </body>
    </html>
    """
    return html


if __name__ == "__main__":
    # Start background collector
    collector_thread = threading.Thread(target=collector.run)
    collector_thread.daemon = True
    collector_thread.start()

    logger.info(f"Starting metrics server on port {PORT}")

    try:
        app.run(host="0.0.0.0", port=PORT, debug=False)
    finally:
        collector.stop()
