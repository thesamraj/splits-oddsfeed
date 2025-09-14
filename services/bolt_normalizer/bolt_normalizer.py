#!/usr/bin/env python3
"""
Bolt Normalizer Service
Ingests BoltOdds staging stream into Neon staging table
Completely isolated from production collectors
"""
import os
import json
import time
import threading
import signal
import sys
from datetime import datetime
from typing import Dict, Optional, Tuple

import psycopg2
import psycopg2.extras
import redis
from flask import Flask, jsonify, Response
from prometheus_client import REGISTRY, generate_latest, CONTENT_TYPE_LATEST, Counter, Gauge

# Configuration
CHANNEL = os.getenv("BOLT_CHANNEL", "odds.raw.bolt.staging")
DB = os.getenv("DATABASE_URL")
RURL = os.getenv("REDIS_URL")
PORT = int(os.getenv("PORT", "8000"))

# Flask app
app = Flask(__name__)

# Metrics
INGESTED = Counter("bolt_ingested_total", "Bolt frames ingested", ["action"])
ERR = Counter("bolt_errors_total", "Errors", ["stage"])
UP = Gauge("collector_up", "Collector up", ["book"])
UP.labels(book="bolt").set(0.0)

# State tracking
LAST = {"ts": 0, "action": None, "count": 0}
running = True

# Database schema
DDL = """
CREATE TABLE IF NOT EXISTS bolt_raw (
  id bigserial primary key,
  ts timestamptz not null default now(),
  action text,
  sport text,
  book text,
  event_id text,
  home_team text,
  away_team text,
  payload jsonb,
  created_at timestamptz not null default now()
);

-- Index for querying
CREATE INDEX IF NOT EXISTS idx_bolt_raw_ts ON bolt_raw(ts DESC);
CREATE INDEX IF NOT EXISTS idx_bolt_raw_event ON bolt_raw(event_id);
CREATE INDEX IF NOT EXISTS idx_bolt_raw_action ON bolt_raw(action);
"""

def db():
    """Get database connection"""
    return psycopg2.connect(DB, sslmode="require")

def ensure_schema():
    """Create table if not exists"""
    try:
        with db() as conn, conn.cursor() as cur:
            cur.execute(DDL)
            conn.commit()
            print("✓ Database schema ready")
    except Exception as e:
        print(f"✗ Schema creation failed: {e}")
        sys.exit(1)

def parse_fields(envelope: Dict) -> Tuple[Optional[str], ...]:
    """
    Extract fields from BoltOdds message
    Handles nested structure: envelope -> data -> data
    """
    # The staging envelope contains: {timestamp, type, data: {actual message}, collector}
    msg = envelope.get("data", {})
    
    # Extract action
    action = msg.get("action") or envelope.get("type", "unknown")
    
    # Get the actual game data (nested)
    data = msg.get("data", {})
    
    # Extract fields
    sport = data.get("sport")
    book = data.get("sportsbook")
    event_id = data.get("universal_game_id")
    home_team = data.get("home_team")
    away_team = data.get("away_team")
    
    return action, sport, book, event_id, home_team, away_team

def worker():
    """Main worker thread - Redis subscriber"""
    global running
    
    print(f"Worker starting - Redis URL: {RURL[:30] if RURL else 'NOT SET'}...", flush=True)
    print(f"Worker subscribing to channel: {CHANNEL}", flush=True)
    
    try:
        ensure_schema()
        print("Schema ready", flush=True)
    except Exception as e:
        print(f"Schema creation error: {e}", flush=True)
    
    # Connect to Redis
    try:
        if 'rediss://' in RURL or 'upstash' in RURL:
            r = redis.from_url(RURL, ssl_cert_reqs=None)
        else:
            r = redis.from_url(RURL)
        print("Worker connected to Redis", flush=True)
        p = r.pubsub(ignore_subscribe_messages=True)
        p.subscribe(CHANNEL)
        print(f"✓ Subscribed to {CHANNEL}", flush=True)
        UP.labels(book="bolt").set(1.0)
    except Exception as e:
        print(f"✗ Redis connection failed: {e}")
        ERR.labels(stage="redis").inc()
        return
    
    # Process messages
    conn = None
    batch = []
    batch_size = 50
    last_flush = time.time()
    
    print("Worker entering message loop", flush=True)
    msg_count = 0
    while running:
        try:
            # Get message with timeout
            m = p.get_message(timeout=0.1)
            
            if not m:
                # Flush batch if we have data and timeout reached
                if batch and (time.time() - last_flush > 1.0):
                    flush_batch(conn, batch)
                    batch = []
                    last_flush = time.time()
                continue
            
            # Parse message
            msg_count += 1
            if msg_count == 1:
                print(f"First message received!", flush=True)
            try:
                obj = json.loads(m["data"])
            except Exception:
                ERR.labels(stage="json").inc()
                continue
            
            # Extract fields
            action, sport, book, event_id, home_team, away_team = parse_fields(obj)
            
            # Skip non-data frames
            if action in ["ping", "socket_connected", "subscribe"]:
                continue
            
            # Add to batch
            batch.append((action, sport, book, event_id, home_team, away_team, json.dumps(obj)))
            
            # Flush if batch is full
            if len(batch) >= batch_size:
                if not conn:
                    conn = db()
                flush_batch(conn, batch)
                batch = []
                last_flush = time.time()
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Worker error: {e}")
            ERR.labels(stage="worker").inc()
            time.sleep(1)
            
    # Cleanup
    if conn:
        conn.close()
    UP.labels(book="bolt").set(0.0)

def flush_batch(conn, batch):
    """Flush batch to database"""
    if not batch:
        return
        
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                """INSERT INTO bolt_raw(action, sport, book, event_id, home_team, away_team, payload) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                batch
            )
        conn.commit()
        
        # Update metrics
        for row in batch:
            action = row[0]
            INGESTED.labels(action=str(action)).inc()
            LAST["action"] = action
            
        LAST["ts"] = time.time()
        LAST["count"] += len(batch)
        
    except Exception as e:
        print(f"Batch insert error: {e}")
        conn.rollback()
        ERR.labels(stage="db").inc()

@app.route("/healthz")
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "channel": CHANNEL,
        "last_action": LAST["action"],
        "last_ts": LAST["ts"],
        "total_ingested": LAST["count"]
    })

@app.route("/metrics")
def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(REGISTRY), mimetype=CONTENT_TYPE_LATEST)

@app.route("/stats")
def stats():
    """Database statistics"""
    try:
        with db() as conn, conn.cursor() as cur:
            # Last 5 minutes
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(DISTINCT event_id) as events,
                    COUNT(DISTINCT sport) as sports,
                    COUNT(DISTINCT book) as books
                FROM bolt_raw 
                WHERE ts > NOW() - INTERVAL '5 minutes'
            """)
            recent = cur.fetchone()
            
            # Action distribution
            cur.execute("""
                SELECT action, COUNT(*) as count
                FROM bolt_raw
                WHERE ts > NOW() - INTERVAL '5 minutes'
                GROUP BY action
                ORDER BY count DESC
                LIMIT 10
            """)
            actions = cur.fetchall()
            
            return jsonify({
                "last_5_min": {
                    "total_rows": recent[0],
                    "unique_events": recent[1],
                    "sports": recent[2],
                    "books": recent[3]
                },
                "actions": {row[0]: row[1] for row in actions}
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def signal_handler(sig, frame):
    """Handle shutdown signal"""
    global running
    print("\nShutting down...")
    running = False
    sys.exit(0)

if __name__ == "__main__":
    # Setup signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start worker thread
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    
    # Start Flask
    print(f"Starting Bolt Normalizer on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)