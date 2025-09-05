#!/usr/bin/env python3
"""
Comprehensive Monitoring Dashboard
Shows real-time status of all 13 sportsbooks
"""

import os
import redis
import psycopg2
import docker
import time
from datetime import datetime, timedelta
from tabulate import tabulate

# Config - example only; use environment variables in production
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder - set via environment
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder - set via environment

ALL_BOOKS = [
    "bovada",
    "draftkings",
    "fanduel",
    "betmgm",
    "betrivers",
    "barstool",
    "sugarhouse",
    "unibet",
    "caesars",
    "pinnacle",
    "pointsbet",
    "mybookie",
    "stake",
]


class Monitor:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.docker_client = docker.from_env()

    def get_db_stats(self):
        """Get database statistics"""
        try:
            conn = psycopg2.connect(
                host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
            )
            cur = conn.cursor()

            # Overall stats
            cur.execute(
                "SELECT pg_size_pretty(pg_database_size(%s)), COUNT(*) FROM odds",
                (DB_NAME,),
            )
            db_size, total_records = cur.fetchone()

            # Per-book stats
            cur.execute(
                """
                SELECT
                    book,
                    COUNT(*) as records,
                    COUNT(DISTINCT event_id) as events,
                    MIN(ts) as oldest,
                    MAX(ts) as newest
                FROM odds
                WHERE ts > NOW() - INTERVAL '5 minutes'
                GROUP BY book
            """
            )

            book_stats = {}
            for row in cur.fetchall():
                book, records, events, oldest, newest = row
                book_stats[book] = {
                    "records_5m": records,
                    "events": events,
                    "rate_per_min": records / 5,
                    "last_seen": newest,
                }

            cur.close()
            conn.close()

            return db_size, total_records, book_stats

        except Exception:
            return "Error", 0, {}

    def get_redis_stats(self):
        """Get Redis channel statistics"""
        stats = {}

        for book in ALL_BOOKS:
            channel = f"odds.raw.{book}"
            # Check if channel has subscribers
            num_subs = self.redis_client.pubsub_numsub(channel)[0][1]
            stats[book] = {"subscribers": num_subs}

        return stats

    def get_container_stats(self):
        """Get Docker container statistics"""
        containers = {}

        for container in self.docker_client.containers.list():
            name = container.name
            for book in ALL_BOOKS:
                if book in name.lower():
                    status = container.status
                    stats = container.stats(stream=False)

                    # Calculate CPU and memory usage
                    cpu_percent = 0
                    memory_mb = 0

                    try:
                        # CPU calculation
                        cpu_delta = (
                            stats["cpu_stats"]["cpu_usage"]["total_usage"]
                            - stats["precpu_stats"]["cpu_usage"]["total_usage"]
                        )
                        system_delta = (
                            stats["cpu_stats"]["system_cpu_usage"]
                            - stats["precpu_stats"]["system_cpu_usage"]
                        )
                        if system_delta > 0:
                            cpu_percent = (cpu_delta / system_delta) * 100

                        # Memory calculation
                        memory_mb = stats["memory_stats"]["usage"] / (1024 * 1024)
                    except:
                        pass

                    if book not in containers:
                        containers[book] = []

                    containers[book].append(
                        {
                            "name": name,
                            "status": status,
                            "cpu": cpu_percent,
                            "memory_mb": memory_mb,
                        }
                    )

        return containers

    def display_dashboard(self):
        """Display comprehensive monitoring dashboard"""
        os.system("clear")

        print("=" * 80)
        print("ODDSFEED MONITORING DASHBOARD - 13 SPORTSBOOKS".center(80))
        print("=" * 80)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()

        # Get all stats
        db_size, total_records, book_db_stats = self.get_db_stats()
        redis_stats = self.get_redis_stats()
        container_stats = self.get_container_stats()

        # Database overview
        print(f"DATABASE: {db_size} | {total_records:,} total records")
        print()

        # Book status table
        table_data = []

        for book in ALL_BOOKS:
            # Determine book name for display
            display_name = book.upper()
            if book == "pinnacle":
                display_name = "PIN"

            # Database stats
            db_stat = book_db_stats.get(book, {}) or book_db_stats.get(book[:3], {})
            records_5m = db_stat.get("records_5m", 0)
            rate = db_stat.get("rate_per_min", 0)
            events = db_stat.get("events", 0)

            # Redis stats
            redis_stat = redis_stats.get(book, {})
            subscribers = redis_stat.get("subscribers", 0)

            # Container stats
            containers = container_stats.get(book, [])
            container_status = "DOWN"
            if containers:
                running = [c for c in containers if c["status"] == "running"]
                if running:
                    container_status = f"UP ({len(running)})"

            # Data freshness
            last_seen = db_stat.get("last_seen")
            if last_seen:
                age = datetime.now(last_seen.tzinfo) - last_seen
                if age < timedelta(minutes=1):
                    freshness = "LIVE"
                elif age < timedelta(minutes=5):
                    freshness = "RECENT"
                else:
                    freshness = "STALE"
            else:
                freshness = "NO DATA"

            # Overall status
            if freshness == "LIVE" and container_status.startswith("UP"):
                status = "✅ OPERATIONAL"
            elif freshness == "RECENT":
                status = "⚠️  DEGRADED"
            else:
                status = "❌ DOWN"

            table_data.append(
                [
                    display_name,
                    status,
                    container_status,
                    f"{records_5m:,}",
                    f"{rate:.0f}/min",
                    events,
                    freshness,
                ]
            )

        headers = [
            "BOOK",
            "STATUS",
            "CONTAINERS",
            "RECORDS(5m)",
            "RATE",
            "EVENTS",
            "DATA",
        ]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        # Summary statistics
        print()
        operational = sum(1 for row in table_data if "OPERATIONAL" in row[1])
        degraded = sum(1 for row in table_data if "DEGRADED" in row[1])
        down = sum(1 for row in table_data if "DOWN" in row[1])

        print(
            f"SUMMARY: {operational}/13 Operational | {degraded} Degraded | {down} Down"
        )
        print(f"COVERAGE: {(operational/13)*100:.1f}% | TARGET: 100%")

        # Service status
        print()
        print("SERVICES:")
        services = [
            "normalizer",
            "db-cleaner",
            "deduplicator",
            "rate-limiter",
            "direct-storage",
        ]
        service_status = []

        for service in services:
            try:
                containers = self.docker_client.containers.list(
                    filters={"name": service}
                )
                if containers and containers[0].status == "running":
                    service_status.append(f"✅ {service}")
                else:
                    service_status.append(f"❌ {service}")
            except:
                service_status.append(f"❌ {service}")

        print(" | ".join(service_status))

    def run(self):
        """Main monitoring loop"""
        while True:
            try:
                self.display_dashboard()
                time.sleep(5)
            except KeyboardInterrupt:
                print("\nMonitoring stopped")
                break
            except Exception as e:
                print(f"Error: {e}")
                time.sleep(5)


if __name__ == "__main__":
    monitor = Monitor()
    monitor.run()
