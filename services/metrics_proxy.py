#!/usr/bin/env python3
"""
Metrics proxy - aggregates metrics from internal services with DB fallback
"""
import os
import time
import threading
import requests
import psycopg2
from flask import Flask, Response, jsonify

app = Flask(__name__)
metrics_cache = {}
metrics_lock = threading.Lock()
targets_status = {}

# Database connection pool (if DATABASE_URL is set)
db_pool = None
if os.getenv("DATABASE_URL"):
    try:
        db_pool = psycopg2.pool.SimpleConnectionPool(
            1, 5, os.getenv("DATABASE_URL"), connect_timeout=5
        )
    except Exception as e:
        print(f"Failed to initialize DB pool: {e}", flush=True)


def parse_targets():
    """Parse METRICS_TARGETS env var (space or comma separated URLs or host:port)"""
    raw = os.getenv("METRICS_TARGETS", "")
    targets = []
    for part in raw.replace(",", " ").split():
        if part:
            # If it's a full URL, use as-is; else construct URL
            if part.startswith("http://") or part.startswith("https://"):
                targets.append(part)
            else:
                # Assume host:port format
                targets.append(f"http://{part}/metrics")
    return targets


def get_db_metrics(book):
    """Get metrics from database for a specific book"""
    if not db_pool:
        return None

    metrics = {}
    conn = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()

        # Get odds count in last 15 minutes
        cur.execute(
            """
            SELECT COUNT(*)
            FROM odds
            WHERE book = %s
            AND created_at > NOW() - INTERVAL '15 minutes'
        """,
            (book,),
        )
        odds_15m = cur.fetchone()[0]
        metrics[f'odds_15m{{book="{book}"}}'] = float(odds_15m)

        # Get tick count in last 15 minutes
        cur.execute(
            """
            SELECT COUNT(*)
            FROM odds_ticks
            WHERE book = %s
            AND created_at > NOW() - INTERVAL '15 minutes'
        """,
            (book,),
        )
        ticks_15m = cur.fetchone()[0]
        metrics[f'ticks_15m{{book="{book}"}}'] = float(ticks_15m)

        # Check if book is up (has recent data)
        cur.execute(
            """
            SELECT CASE
                WHEN MAX(created_at) > NOW() - INTERVAL '5 minutes' THEN 1
                ELSE 0
            END
            FROM odds
            WHERE book = %s
        """,
            (book,),
        )
        book_up = cur.fetchone()[0]
        metrics[f'book_up{{book="{book}"}}'] = float(book_up)

        cur.close()
        return metrics
    except Exception as e:
        print(f"Error getting DB metrics for {book}: {e}", flush=True)
        return None
    finally:
        if conn:
            db_pool.putconn(conn)


def scrape_metrics():
    targets = parse_targets()
    aggregated = []
    failed_books = set()

    for target in targets:
        try:
            resp = requests.get(target, timeout=3, allow_redirects=False)
            if resp.status_code == 200:
                # Filter out duplicate TYPE/HELP lines to avoid conflicts
                lines = resp.text.split("\n")
                filtered = []
                seen_defs = set()
                for line in lines:
                    if line.startswith("# HELP") or line.startswith("# TYPE"):
                        if line not in seen_defs:
                            seen_defs.add(line)
                            filtered.append(line)
                    else:
                        filtered.append(line)
                aggregated.append("\n".join(filtered))
                targets_status[target] = {"status": "ok", "last_error": None}
            else:
                targets_status[target] = {
                    "status": "error",
                    "last_error": f"HTTP {resp.status_code}",
                }
                # Extract book name from target URL
                if "bovada" in target.lower():
                    failed_books.add("bovada")
                elif "betrivers" in target.lower():
                    failed_books.add("betrivers")
        except requests.exceptions.Timeout:
            targets_status[target] = {"status": "error", "last_error": "timeout"}
            print(f"Timeout scraping {target}", flush=True)
            # Extract book name
            if "bovada" in target.lower():
                failed_books.add("bovada")
            elif "betrivers" in target.lower():
                failed_books.add("betrivers")
        except Exception as e:
            targets_status[target] = {"status": "error", "last_error": str(e)[:100]}
            print(f"Failed to scrape {target}: {e}", flush=True)

    # Add DB fallback metrics for failed books
    if failed_books and db_pool:
        db_metrics_lines = []
        db_metrics_lines.append("# HELP odds_15m Odds in last 15 minutes (DB fallback)")
        db_metrics_lines.append("# TYPE odds_15m gauge")
        db_metrics_lines.append(
            "# HELP ticks_15m Ticks in last 15 minutes (DB fallback)"
        )
        db_metrics_lines.append("# TYPE ticks_15m gauge")
        db_metrics_lines.append("# HELP book_up Book status (DB fallback)")
        db_metrics_lines.append("# TYPE book_up gauge")

        for book in failed_books:
            metrics = get_db_metrics(book)
            if metrics:
                for key, value in metrics.items():
                    db_metrics_lines.append(f"{key} {value}")
                print(f"Added DB fallback metrics for {book}", flush=True)

        if len(db_metrics_lines) > 6:  # More than just headers
            aggregated.append("\n".join(db_metrics_lines))

    with metrics_lock:
        metrics_cache["data"] = "\n".join(aggregated)
        metrics_cache["timestamp"] = time.time()
        print(
            f"Scraped {len(targets)} targets, {len(metrics_cache['data'])} bytes",
            flush=True,
        )


@app.route("/metrics")
def metrics():
    # Check if we need to scrape (without holding lock)
    with metrics_lock:
        need_scrape = (
            "data" not in metrics_cache
            or time.time() - metrics_cache.get("timestamp", 0) > 30
        )

    if need_scrape:
        scrape_metrics()  # This updates metrics_cache with its own lock

    # Return the cached data
    with metrics_lock:
        return Response(metrics_cache.get("data", ""), mimetype="text/plain")


@app.route("/healthz")
def healthz():
    return "OK", 200


@app.route("/readiness")
def readiness():
    """Readiness probe - checks if we have recent metrics"""
    with metrics_lock:
        if "data" not in metrics_cache:
            return jsonify({"status": "not_ready", "reason": "no_data"}), 503

        age = time.time() - metrics_cache.get("timestamp", 0)
        if age > 120:
            return (
                jsonify(
                    {"status": "not_ready", "reason": "stale_data", "age_seconds": age}
                ),
                503,
            )

    return jsonify({"status": "ready", "age_seconds": age}), 200


@app.route("/targets")
def targets():
    """Show current targets and their status"""
    db_status = "connected" if db_pool else "not_configured"
    return jsonify(
        {
            "targets": parse_targets(),
            "status": targets_status,
            "last_scrape": metrics_cache.get("timestamp"),
            "db_fallback": db_status,
        }
    )


@app.route("/realness/<book>/report")
def realness_report(book):
    """Proxy realness report from internal collector"""
    # Input validation - alphanumeric and hyphens only
    import re

    if not re.match(r"^[a-z0-9-]+$", book.lower()):
        return jsonify({"error": "Invalid book name format"}), 400

    # Full allowlist map for all 13 books
    service_map = {
        "bovada": "bovada-collector",
        "betrivers": "betrivers-collector",
        "barstool": "barstool-collector",
        "caesars": "caesars-collector",
        "sugarhouse": "sugarhouse-collector",
        "unibet": "unibet-collector",
        "fanduel": "fanduel-collector",
        "draftkings": "draftkings-collector",
        "betmgm": "betmgm-collector",
        "pinnacle": "pinnacle-collector",
        "bet365": "bet365-collector",
        "stake": "stake-collector",
        "pointsbet": "pointsbet-collector",
    }

    service = service_map.get(book.lower())
    if not service:
        return jsonify({"error": "Unknown book"}), 404

    # Proxy request with safe headers and timeout
    try:
        url = f"http://{service}:9091/realness/report"
        headers = {"User-Agent": "metrics-proxy/1.0", "Accept": "application/json"}
        resp = requests.get(url, timeout=5, headers=headers)

        if resp.status_code == 200:
            return jsonify(resp.json())
        else:
            return (
                jsonify(
                    {
                        "error": f"Service returned {resp.status_code}",
                        "book": book,
                        "service": service,
                        "details": resp.text[:200] if resp.text else None,
                    }
                ),
                502,
            )
    except requests.exceptions.Timeout:
        return (
            jsonify({"error": "Service timeout", "book": book, "service": service}),
            504,
        )
    except requests.exceptions.ConnectionError:
        return (
            jsonify({"error": "Service unavailable", "book": book, "service": service}),
            503,
        )
    except Exception as e:
        return (
            jsonify(
                {"error": "Internal proxy error", "book": book, "details": str(e)[:100]}
            ),
            500,
        )


@app.route("/healthz/<book>")
def book_healthz(book):
    """Proxy health check from internal collector"""
    # Input validation
    import re

    if not re.match(r"^[a-z0-9-]+$", book.lower()):
        return jsonify({"error": "Invalid book name format"}), 400

    # Full allowlist map for all 13 books
    service_map = {
        "bovada": "bovada-collector",
        "betrivers": "betrivers-collector",
        "barstool": "barstool-collector",
        "caesars": "caesars-collector",
        "sugarhouse": "sugarhouse-collector",
        "unibet": "unibet-collector",
        "fanduel": "fanduel-collector",
        "draftkings": "draftkings-collector",
        "betmgm": "betmgm-collector",
        "pinnacle": "pinnacle-collector",
        "bet365": "bet365-collector",
        "stake": "stake-collector",
        "pointsbet": "pointsbet-collector",
    }

    service = service_map.get(book.lower())
    if not service:
        return jsonify({"error": "Unknown book"}), 404

    try:
        url = f"http://{service}:9091/healthz"
        headers = {"User-Agent": "metrics-proxy/1.0"}
        resp = requests.get(url, timeout=3, headers=headers)

        # Return plain text for health checks
        return resp.text, resp.status_code
    except requests.exceptions.Timeout:
        return (
            jsonify({"error": "Service timeout", "book": book, "service": service}),
            504,
        )
    except requests.exceptions.ConnectionError:
        return (
            jsonify({"error": "Service unavailable", "book": book, "service": service}),
            503,
        )
    except Exception as e:
        return (
            jsonify(
                {"error": "Internal proxy error", "book": book, "details": str(e)[:100]}
            ),
            500,
        )


# TODO: Add rate limiting with flask-limiter when available


if __name__ == "__main__":
    print(f"Starting metrics proxy with targets: {parse_targets()}")
    if db_pool:
        print("Database fallback enabled")
    else:
        print("Database fallback not configured")

    scrape_metrics()  # Initial scrape
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
