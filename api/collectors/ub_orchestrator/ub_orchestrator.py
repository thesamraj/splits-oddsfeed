#!/usr/bin/env python3
"""
Unibet Orchestrator
Manages HTTP and CDP collectors with automatic fallback and recovery
"""
import json
import time
import os
import subprocess
import signal
import sys
from datetime import datetime
import redis
import requests
from typing import Dict, Any

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
HEALTHZ_PORT = int(os.getenv("HEALTHZ_PORT", "9136"))
HTTP_HEALTH_URL = "http://localhost:9134/healthz"
CDP_HEALTH_URL = "http://localhost:9135/healthz"
FALLBACK_SIGNAL_KEY = "ub:collector:fallback_required"
RECOVERY_CHECK_INTERVAL = 300  # 5 minutes


class UBOrchestrator:
    def __init__(self):
        self.redis_client = redis.from_url(REDIS_URL)
        self.http_process = None
        self.cdp_process = None
        self.current_mode = "http"
        self.fallback_time = None
        self.recovery_attempts = 0

    def start_http_collector(self):
        """Start HTTP collector process"""
        if self.http_process and self.http_process.poll() is None:
            return  # Already running

        print("[ORCHESTRATOR] Starting HTTP collector")
        self.http_process = subprocess.Popen(
            ["python3", "/app/collectors/ub_http/ub_http_collector.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(5)  # Give it time to start

    def start_cdp_collector(self):
        """Start CDP collector process"""
        if self.cdp_process and self.cdp_process.poll() is None:
            return  # Already running

        print("[ORCHESTRATOR] Starting CDP collector")
        self.cdp_process = subprocess.Popen(
            ["python3", "/app/collectors/ub_cdp/ub_cdp_collector.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(10)  # CDP takes longer to start

    def check_http_health(self) -> Dict[str, Any]:
        """Check HTTP collector health"""
        try:
            resp = requests.get(HTTP_HEALTH_URL, timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        return {"status": "error", "consecutive_failures": 999}

    def check_cdp_health(self) -> Dict[str, Any]:
        """Check CDP collector health"""
        try:
            resp = requests.get(CDP_HEALTH_URL, timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        return {"status": "error"}

    def attempt_recovery(self):
        """Try to recover HTTP collector after fallback"""
        if self.current_mode != "fallback":
            return

        # Only attempt recovery every 5 minutes
        if (
            self.fallback_time
            and (datetime.utcnow() - self.fallback_time).seconds
            < RECOVERY_CHECK_INTERVAL
        ):
            return

        print("[ORCHESTRATOR] Attempting HTTP recovery")
        self.recovery_attempts += 1

        # Clear fallback signal to let HTTP try again
        self.redis_client.delete(FALLBACK_SIGNAL_KEY)

        # Restart HTTP collector
        if self.http_process:
            self.http_process.terminate()
            time.sleep(2)

        self.start_http_collector()
        time.sleep(30)  # Give it time to try

        # Check if it's working
        http_health = self.check_http_health()
        if http_health.get("status") == "active":
            print("[ORCHESTRATOR] HTTP recovery successful")
            self.current_mode = "http"
            self.fallback_time = None
            self.recovery_attempts = 0
        else:
            print(
                f"[ORCHESTRATOR] HTTP recovery failed (attempt {self.recovery_attempts})"
            )
            # Re-signal fallback
            self.redis_client.setex(
                FALLBACK_SIGNAL_KEY, 3600, json.dumps({"recovery_failed": True})
            )

    def auto_fix_common_issues(self):
        """Auto-fix common blockers"""
        # Check for rate limiting
        http_health = self.check_http_health()
        if http_health.get("consecutive_failures", 0) > 10:
            print("[ORCHESTRATOR] Detected rate limiting, rotating user agent")
            # Signal collectors to rotate user agents
            self.redis_client.publish(
                "ub:control", json.dumps({"command": "rotate_user_agent"})
            )

        # Check for stale processes
        if self.http_process and self.http_process.poll() is not None:
            print("[ORCHESTRATOR] HTTP collector crashed, restarting")
            self.start_http_collector()

        if self.cdp_process and self.cdp_process.poll() is not None:
            print("[ORCHESTRATOR] CDP collector crashed, restarting")
            self.start_cdp_collector()

    def run(self):
        """Main orchestration loop"""
        print("[ORCHESTRATOR] Starting Unibet Orchestrator")

        # Start with HTTP collector
        self.start_http_collector()

        # Start CDP collector in standby
        self.start_cdp_collector()

        # Health check endpoint
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading

        class HealthHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/healthz":
                    self.send_response(200)
                    self.end_headers()
                    status = {
                        "mode": orchestrator.current_mode,
                        "http_health": orchestrator.check_http_health(),
                        "cdp_health": orchestrator.check_cdp_health(),
                        "recovery_attempts": orchestrator.recovery_attempts,
                    }
                    self.wfile.write(json.dumps(status).encode())

            def log_message(self, format, *args):
                pass

        orchestrator = self
        health_server = HTTPServer(("0.0.0.0", HEALTHZ_PORT), HealthHandler)
        health_thread = threading.Thread(target=health_server.serve_forever)
        health_thread.daemon = True
        health_thread.start()

        print(f"[ORCHESTRATOR] Health endpoint at port {HEALTHZ_PORT}")

        while True:
            try:
                # Check if fallback is needed
                fallback_signal = self.redis_client.get(FALLBACK_SIGNAL_KEY)
                if fallback_signal and self.current_mode != "fallback":
                    print("[ORCHESTRATOR] Switching to CDP fallback mode")
                    self.current_mode = "fallback"
                    self.fallback_time = datetime.utcnow()

                # Auto-fix common issues
                self.auto_fix_common_issues()

                # Attempt recovery if in fallback
                if self.current_mode == "fallback":
                    self.attempt_recovery()

                # Monitor and log status
                http_health = self.check_http_health()
                cdp_health = self.check_cdp_health()

                print(
                    f"[ORCHESTRATOR] Mode: {self.current_mode} | HTTP: {http_health.get('status')} | CDP: {cdp_health.get('status')}"
                )

                time.sleep(60)

            except KeyboardInterrupt:
                print("[ORCHESTRATOR] Shutting down")
                if self.http_process:
                    self.http_process.terminate()
                if self.cdp_process:
                    self.cdp_process.terminate()
                break
            except Exception as e:
                print(f"[ORCHESTRATOR] Error: {e}")
                time.sleep(60)


def signal_handler(sig, frame):
    print("[ORCHESTRATOR] Received shutdown signal")
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    orchestrator = UBOrchestrator()
    orchestrator.run()
