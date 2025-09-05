#!/usr/bin/env python3
"""
Automated Alert Service - Monitors system health and sends alerts
"""

import os
import redis
import psycopg2
import json
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("alert_service")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 60))

# Alert thresholds
THRESHOLDS = {
    "min_books_active": 10,
    "min_messages_per_minute": 100,
    "max_db_cpu_percent": 80,
    "max_error_rate": 0.1,
    "data_staleness_seconds": 300,
}


class AlertService:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.alerts_sent = {}
        self.last_check = {}

    def connect_db(self):
        """Connect to PostgreSQL"""
        return psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )

    def check_redis_health(self):
        """Check Redis broker health"""
        try:
            self.redis_client.ping()
            return True, "Redis healthy"
        except Exception as e:
            return False, f"Redis down: {e}"

    def check_active_books(self):
        """Check number of active books"""
        try:
            channels = self.redis_client.pubsub_channels("odds.raw.*")
            active_count = len(channels)

            if active_count < THRESHOLDS["min_books_active"]:
                return (
                    False,
                    f"Only {active_count} books active (threshold: {THRESHOLDS['min_books_active']})",
                )

            return True, f"{active_count} books active"

        except Exception as e:
            return False, f"Error checking books: {e}"

    def check_data_flow(self):
        """Check data flow rate"""
        try:
            conn = self.connect_db()
            cur = conn.cursor()

            # Check records in last minute
            cur.execute(
                """
                SELECT COUNT(*)
                FROM odds
                WHERE ts > NOW() - INTERVAL '1 minute'
            """
            )

            count = cur.fetchone()[0]
            cur.close()
            conn.close()

            if count < THRESHOLDS["min_messages_per_minute"]:
                return (
                    False,
                    f"Low data flow: {count} msgs/min (threshold: {THRESHOLDS['min_messages_per_minute']})",
                )

            return True, f"Data flow: {count} msgs/min"

        except Exception as e:
            return False, f"Error checking data flow: {e}"

    def check_data_staleness(self):
        """Check for stale data per book"""
        try:
            conn = self.connect_db()
            cur = conn.cursor()

            # Check latest data per book
            cur.execute(
                """
                SELECT book, MAX(ts) as latest
                FROM odds
                WHERE ts > NOW() - INTERVAL '1 hour'
                GROUP BY book
            """
            )

            stale_books = []
            now = datetime.utcnow()

            for book, latest in cur.fetchall():
                age_seconds = (now - latest.replace(tzinfo=None)).total_seconds()
                if age_seconds > THRESHOLDS["data_staleness_seconds"]:
                    stale_books.append(f"{book} ({int(age_seconds)}s old)")

            cur.close()
            conn.close()

            if stale_books:
                return False, f"Stale data: {', '.join(stale_books)}"

            return True, "All books have fresh data"

        except Exception as e:
            return False, f"Error checking staleness: {e}"

    def send_alert(self, alert_type, message):
        """Send alert (log for now, can add email/Slack later)"""
        # Prevent alert spam
        alert_key = f"{alert_type}:{message[:50]}"
        if alert_key in self.alerts_sent:
            last_sent = self.alerts_sent[alert_key]
            if (datetime.utcnow() - last_sent).total_seconds() < 300:  # 5 min cooldown
                return

        # Log alert
        logger.error(f"🚨 ALERT [{alert_type}]: {message}")

        # Store alert in Redis for dashboard
        alert_data = {
            "type": alert_type,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
        }

        self.redis_client.lpush("alerts:history", json.dumps(alert_data))
        self.redis_client.ltrim("alerts:history", 0, 99)  # Keep last 100 alerts

        # Track sent alerts
        self.alerts_sent[alert_key] = datetime.utcnow()

    def run_checks(self):
        """Run all health checks"""
        checks = [
            ("redis_health", self.check_redis_health),
            ("active_books", self.check_active_books),
            ("data_flow", self.check_data_flow),
            ("data_staleness", self.check_data_staleness),
        ]

        all_healthy = True

        for check_name, check_func in checks:
            try:
                healthy, message = check_func()

                if not healthy:
                    self.send_alert(check_name, message)
                    all_healthy = False
                else:
                    logger.info(f"✅ {check_name}: {message}")

            except Exception as e:
                logger.error(f"Check {check_name} failed: {e}")
                all_healthy = False

        # Store overall status
        status = {
            "healthy": all_healthy,
            "last_check": datetime.utcnow().isoformat(),
            "checks_run": len(checks),
        }

        self.redis_client.set("alerts:status", json.dumps(status))

        return all_healthy

    def run(self):
        """Main monitoring loop"""
        logger.info("Alert Service started - monitoring system health")

        while True:
            try:
                healthy = self.run_checks()

                if healthy:
                    logger.info("System healthy - all checks passed")
                else:
                    logger.warning("System issues detected - alerts sent")

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")

            time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    alert_service = AlertService()
    alert_service.run()
