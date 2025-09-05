#!/usr/bin/env python3
"""
Monitoring API - Real-time system status and analytics
Provides REST endpoints for monitoring dashboard
"""

import os
import json
import redis
import psycopg2
from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monitor_api")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder

app = Flask(__name__)
CORS(app)

# Initialize connections
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_db_connection():
    """Get database connection"""
    return psycopg2.connect(
        host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
    )


@app.route("/health")
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.utcnow().isoformat()})


@app.route("/api/books/status")
def books_status():
    """Get status of all books"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
        SELECT
            book,
            COUNT(DISTINCT event_id) as events,
            COUNT(*) as records,
            MAX(ts) as last_update
        FROM odds
        WHERE ts > NOW() - INTERVAL '5 minutes'
        GROUP BY book
        ORDER BY book;
        """

        cur.execute(query)
        results = cur.fetchall()

        books = []
        for row in results:
            book, events, records, last_update = row
            books.append(
                {
                    "book": book,
                    "events": events,
                    "records": records,
                    "last_update": last_update.isoformat() if last_update else None,
                    "status": (
                        "active"
                        if last_update
                        and (datetime.now(last_update.tzinfo) - last_update).seconds
                        < 60
                        else "stale"
                    ),
                }
            )

        cur.close()
        conn.close()

        return jsonify(books)

    except Exception as e:
        logger.error(f"Error getting book status: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/system/metrics")
def system_metrics():
    """Get system metrics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Database metrics
        cur.execute("SELECT pg_size_pretty(pg_database_size(%s))", (DB_NAME,))
        db_size = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM odds")
        total_records = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT event_id) FROM events")
        total_events = cur.fetchone()[0]

        # Records per minute
        cur.execute(
            """
            SELECT COUNT(*)
            FROM odds
            WHERE ts > NOW() - INTERVAL '1 minute'
        """
        )
        records_per_minute = cur.fetchone()[0]

        # Active collectors (from Redis)
        active_collectors = 0
        for key in redis_client.scan_iter("odds.raw.*"):
            active_collectors += 1

        metrics = {
            "database": {
                "size": db_size,
                "total_records": total_records,
                "total_events": total_events,
            },
            "performance": {
                "records_per_minute": records_per_minute,
                "records_per_second": round(records_per_minute / 60, 2),
            },
            "collectors": {"active": active_collectors},
        }

        cur.close()
        conn.close()

        return jsonify(metrics)

    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/analytics/latest")
def latest_analytics():
    """Get latest analytics results"""
    try:
        analytics = redis_client.get("analytics:latest")
        if analytics:
            return jsonify(json.loads(analytics))
        else:
            return jsonify({"message": "No analytics data available yet"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/odds/compare/<event_id>")
def compare_odds(event_id):
    """Compare odds for an event across all books"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
        SELECT DISTINCT ON (book, market, outcome_name)
            book,
            market,
            outcome_name,
            outcome_price,
            outcome_point,
            ts
        FROM odds
        WHERE event_id = %s
            AND ts > NOW() - INTERVAL '10 minutes'
        ORDER BY book, market, outcome_name, ts DESC;
        """

        cur.execute(query, (event_id,))
        results = cur.fetchall()

        comparison = defaultdict(lambda: defaultdict(dict))

        for row in results:
            book, market, outcome, price, point, ts = row
            comparison[market][outcome][book] = {
                "price": price,
                "point": float(point) if point else None,
                "timestamp": ts.isoformat() if ts else None,
            }

        cur.close()
        conn.close()

        return jsonify(dict(comparison))

    except Exception as e:
        logger.error(f"Error comparing odds: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/events/live")
def live_events():
    """Get all live events"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        query = """
        SELECT DISTINCT
            e.id,
            e.home,
            e.away,
            e.sport,
            e.start_time,
            COUNT(DISTINCT o.book) as books_offering,
            MIN(o.ts) as first_seen,
            MAX(o.ts) as last_update
        FROM events e
        JOIN odds o ON e.id = o.event_id
        WHERE o.ts > NOW() - INTERVAL '10 minutes'
        GROUP BY e.id, e.home, e.away, e.sport, e.start_time
        ORDER BY books_offering DESC, e.home, e.away
        LIMIT 100;
        """

        cur.execute(query)
        results = cur.fetchall()

        events = []
        for row in results:
            event_id, home, away, sport, start_time, books, first_seen, last_update = (
                row
            )
            events.append(
                {
                    "event_id": event_id,
                    "home": home,
                    "away": away,
                    "sport": sport,
                    "start_time": start_time.isoformat() if start_time else None,
                    "books_offering": books,
                    "first_seen": first_seen.isoformat() if first_seen else None,
                    "last_update": last_update.isoformat() if last_update else None,
                }
            )

        cur.close()
        conn.close()

        return jsonify(events)

    except Exception as e:
        logger.error(f"Error getting live events: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
