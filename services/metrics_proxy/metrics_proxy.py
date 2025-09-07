#!/usr/bin/env python3
"""
Metrics proxy - aggregates metrics from public service URLs with DB fallback
"""
import os
import time
import threading
import requests
import psycopg2
import re
from flask import Flask, Response, jsonify

app = Flask(__name__)
metrics_cache = {}
metrics_lock = threading.Lock()
targets_status = {}

# Database connection pool (if DATABASE_URL is set)
db_pool = None
if os.getenv("DATABASE_URL"):
    try:
        from psycopg2 import pool
        db_pool = pool.SimpleConnectionPool(
            1, 5, os.getenv("DATABASE_URL"), connect_timeout=5
        )
    except Exception as e:
        print(f"Failed to initialize DB pool: {e}", flush=True)

# Book to service base URL mapping from environment
BOOK_BASE_URLS = {
    "bovada": os.getenv("SERVICE_URL_BOVADA", "http://bovada:8000"),
    "normalizer": os.getenv("SERVICE_URL_NORMALIZER", "http://normalizer:8000"),
    "betrivers": os.getenv("SERVICE_URL_BETRIVERS", "http://betrivers:8000"),
    "barstool": os.getenv("SERVICE_URL_BARSTOOL", ""),
    "caesars": os.getenv("SERVICE_URL_CAESARS", ""),
    "sugarhouse": os.getenv("SERVICE_URL_SUGARHOUSE", ""),
    "unibet": os.getenv("SERVICE_URL_UNIBET", ""),
    "fanduel": os.getenv("SERVICE_URL_FANDUEL", ""),
    "draftkings": os.getenv("SERVICE_URL_DRAFTKINGS", ""),
    "betmgm": os.getenv("SERVICE_URL_BETMGM", ""),
    "pinnacle": os.getenv("SERVICE_URL_PINNACLE", ""),
    "bet365": os.getenv("SERVICE_URL_BET365", ""),
    "stake": os.getenv("SERVICE_URL_STAKE", ""),
    "pointsbet": os.getenv("SERVICE_URL_POINTSBET", ""),
}


def parse_targets():
    """Parse METRICS_TARGETS env var (comma-separated FULL URLs)"""
    raw = os.getenv("METRICS_TARGETS", "")
    targets = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            # Ensure it's a proper URL
            if not (part.startswith("http://") or part.startswith("https://")):
                part = f"http://{part}"  # Default to http for local
            targets.append(part)
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
    """Scrape metrics from public service URLs"""
    targets = parse_targets()
    aggregated = []
    failed_books = set()
    
    print(f"Starting scrape of {len(targets)} targets: {targets}", flush=True)

    for base_url in targets:
        try:
            # Append /metrics to base URL
            metrics_url = f"{base_url.rstrip('/')}/metrics"
            print(f"Scraping {metrics_url}...", flush=True)
            resp = requests.get(metrics_url, timeout=5, allow_redirects=False)
            
            if resp.status_code == 200:
                print(f"Got {len(resp.text)} bytes from {metrics_url}", flush=True)
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
                targets_status[base_url] = {"status": "ok", "last_error": None}
            else:
                targets_status[base_url] = {
                    "status": "error",
                    "last_error": f"HTTP {resp.status_code}",
                }
                # Extract book name from URL
                if "bovada" in base_url.lower():
                    failed_books.add("bovada")
                elif "normalizer" in base_url.lower():
                    failed_books.add("normalizer")
                elif "betrivers" in base_url.lower():
                    failed_books.add("betrivers")
        except requests.exceptions.Timeout:
            targets_status[base_url] = {"status": "error", "last_error": "timeout"}
            print(f"Timeout scraping {base_url}/metrics", flush=True)
            # Extract book name
            if "bovada" in base_url.lower():
                failed_books.add("bovada")
            elif "normalizer" in base_url.lower():
                failed_books.add("normalizer")
        except Exception as e:
            targets_status[base_url] = {"status": "error", "last_error": str(e)[:100]}
            print(f"Failed to scrape {metrics_url}: {e}", flush=True)

    # Add DB fallback metrics for failed books
    if failed_books and db_pool:
        print(f"Attempting DB fallback for: {failed_books}", flush=True)
        for book in failed_books:
            db_metrics = get_db_metrics(book)
            if db_metrics:
                fallback_lines = [f"# DB fallback metrics for {book}"]
                for metric, value in db_metrics.items():
                    fallback_lines.append(f"{metric} {value}")
                aggregated.append("\n".join(fallback_lines))
                print(f"Added DB fallback metrics for {book}", flush=True)

    # Join all metrics
    result = "\n".join(aggregated)

    # Update cache
    with metrics_lock:
        metrics_cache["data"] = result
        metrics_cache["timestamp"] = time.time()
        metrics_cache["targets"] = targets

    return result


def update_metrics_loop():
    """Background thread to update metrics periodically"""
    while True:
        try:
            scrape_metrics()
        except Exception as e:
            print(f"Error in metrics loop: {e}", flush=True)
        time.sleep(10)  # Update every 10 seconds


# Start background thread
threading.Thread(target=update_metrics_loop, daemon=True).start()


@app.route("/metrics")
def metrics():
    """Serve aggregated metrics"""
    with metrics_lock:
        if "data" not in metrics_cache:
            # Initial scrape if no data yet
            data = scrape_metrics()
        else:
            data = metrics_cache.get("data", "")

    return Response(data, mimetype="text/plain; version=0.0.4")


@app.route("/healthz")
def healthz():
    """Health check endpoint"""
    with metrics_lock:
        has_data = "data" in metrics_cache
        if has_data:
            age = time.time() - metrics_cache.get("timestamp", 0)
        else:
            age = -1

    return jsonify(
        {
            "status": "healthy" if has_data and age < 120 else "unhealthy",
            "has_data": has_data,
            "data_age_seconds": age if has_data else None,
            "targets": parse_targets(),
        }
    )


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
    """Proxy realness report from service"""
    # Input validation - alphanumeric and hyphens only
    if not re.match(r"^[a-z0-9-]+$", book.lower()):
        return jsonify({"error": "Invalid book name format"}), 400

    # For local, use hardcoded URL for bovada
    if book.lower() == "bovada":
        base_url = os.getenv("BOVADA_BASE", "http://bovada:8000")
    else:
        # Get base URL for the book from environment
        base_url = BOOK_BASE_URLS.get(book.lower(), "").strip()
        if not base_url:
            return jsonify({"error": "Unknown book or service URL not configured"}), 404

    # Proxy request with safe headers and timeout
    try:
        url = f"{base_url.rstrip('/')}/realness/report"
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
                        "url": url,
                        "details": resp.text[:200] if resp.text else None,
                    }
                ),
                502,
            )
    except requests.exceptions.Timeout:
        return (
            jsonify({"error": "Service timeout", "book": book, "url": base_url}),
            504,
        )
    except requests.exceptions.ConnectionError:
        return (
            jsonify({"error": "Service unavailable", "book": book, "url": base_url}),
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
    """Proxy health check from public service"""
    # Input validation
    if not re.match(r"^[a-z0-9-]+$", book.lower()):
        return jsonify({"error": "Invalid book name format"}), 400

    # Get base URL for the book
    base_url = BOOK_BASE_URLS.get(book.lower(), "").strip()
    if not base_url:
        return jsonify({"error": "Unknown book or service URL not configured"}), 404

    try:
        url = f"{base_url.rstrip('/')}/healthz"
        headers = {"User-Agent": "metrics-proxy/1.0"}
        resp = requests.get(url, timeout=3, headers=headers)

        # Return the response as-is
        if resp.headers.get('content-type', '').startswith('application/json'):
            return jsonify(resp.json()), resp.status_code
        else:
            return resp.text, resp.status_code
    except requests.exceptions.Timeout:
        return (
            jsonify({"error": "Service timeout", "book": book, "url": base_url}),
            504,
        )
    except requests.exceptions.ConnectionError:
        return (
            jsonify({"error": "Service unavailable", "book": book, "url": base_url}),
            503,
        )
    except Exception as e:
        return (
            jsonify(
                {"error": "Internal proxy error", "book": book, "details": str(e)[:100]}
            ),
            500,
        )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    print(f"Starting metrics proxy on port {port}", flush=True)
    print(f"METRICS_TARGETS: {os.getenv('METRICS_TARGETS', 'not set')}", flush=True)
    print(f"DATABASE_URL: {'configured' if os.getenv('DATABASE_URL') else 'not set'}", flush=True)
    
    # Wait a bit for other services to start
    print("Waiting 15s for services to start...", flush=True)
    time.sleep(15)
    
    # Initial scrape before starting server
    print("Performing initial metrics scrape...", flush=True)
    scrape_metrics()
    
    app.run(host="0.0.0.0", port=port, debug=False)