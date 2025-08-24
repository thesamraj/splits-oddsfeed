#!/usr/bin/env python3
"""
Health check server for SugarHouse polite collector
"""

import json
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from typing import Optional


class HealthState:
    """Shared state for health monitoring"""

    def __init__(self):
        self.status = "initializing"
        self.last_status_code: Optional[int] = None
        self.last_200_ts: Optional[float] = None
        self.last_429_ts: Optional[float] = None
        self.backoff_seconds: int = 0
        self.next_request_ts: Optional[float] = None

    def update_success(self):
        """Update state on successful request"""
        self.status = "active"
        self.last_status_code = 200
        self.last_200_ts = time.time()
        self.backoff_seconds = 0

    def update_rate_limited(self, backoff_sec: int):
        """Update state on 429 rate limit"""
        self.status = "rate_limited"
        self.last_status_code = 429
        self.last_429_ts = time.time()
        self.backoff_seconds = backoff_sec
        self.next_request_ts = time.time() + backoff_sec

    def update_idle(self):
        """Update state to idle"""
        if self.status != "rate_limited":
            self.status = "idle"

    def to_dict(self) -> dict:
        """Convert state to JSON dict"""
        return {
            "status": self.status,
            "last_status": self.last_status_code,
            "last_200_ts": self.last_200_ts,
            "last_429_ts": self.last_429_ts,
            "backoff_seconds": self.backoff_seconds,
        }


# Global health state instance
health_state = HealthState()


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for /healthz endpoint"""

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            response = health_state.to_dict()
            self.wfile.write(json.dumps(response).encode())
        else:
            self.send_error(404)

    def log_message(self, format, *args):
        # Suppress default HTTP server logging
        pass


def start_health_server(port: int = 9122):
    """Start the health check server in background thread"""

    def run_server():
        server = HTTPServer(("0.0.0.0", port), HealthHandler)
        print(f"[healthz] starting on port {port}")
        server.serve_forever()

    thread = Thread(target=run_server, daemon=True)
    thread.start()
    return thread
