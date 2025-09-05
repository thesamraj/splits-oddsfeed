#!/usr/bin/env python3
"""
Database Cleaner Service - Maintains 2-hour retention window
Runs every 30 minutes to keep database size manageable
"""

import os
import psycopg2
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("db_cleaner")

# Config
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", 1))  # Reduced to 1 hour
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS", 600))  # 10 minutes


def clean_database():
    """Delete old data and vacuum"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
        )
        conn.autocommit = True
        cur = conn.cursor()

        # Get current stats
        cur.execute("SELECT COUNT(*) FROM odds")
        before_count = cur.fetchone()[0]

        cur.execute("SELECT pg_size_pretty(pg_database_size(%s))", (DB_NAME,))
        before_size = cur.fetchone()[0]

        logger.info(f"Before cleanup: {before_count} records, {before_size}")

        # Delete old data
        cur.execute(
            f"DELETE FROM odds WHERE ts < NOW() - INTERVAL '{RETENTION_HOURS} hours'"
        )
        deleted = cur.rowcount

        # Skip VACUUM for now due to disk space constraints
        logger.info(f"Deleted {deleted} records")
        # cur.execute("VACUUM ANALYZE odds")

        # Get new stats
        cur.execute("SELECT COUNT(*) FROM odds")
        after_count = cur.fetchone()[0]

        cur.execute("SELECT pg_size_pretty(pg_database_size(%s))", (DB_NAME,))
        after_size = cur.fetchone()[0]

        logger.info(f"After cleanup: {after_count} records, {after_size}")
        logger.info(f"Freed {before_count - after_count} records")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Cleanup error: {e}")


def run():
    """Main loop"""
    logger.info(
        f"Database Cleaner started - {RETENTION_HOURS} hour retention, cleaning every {INTERVAL_SECONDS/60} minutes"
    )

    while True:
        clean_database()
        logger.info(f"Next cleanup in {INTERVAL_SECONDS/60} minutes")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
