#!/usr/bin/env python3
"""
Fixed DraftKings Normalizer
- Properly extracts outcome_name and outcome_price
- Writes to both odds and ticks tables
"""

import redis
import json
import time
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# Redis connection
r = redis.from_url("redis://broker:6379/0")
p = r.pubsub()
p.psubscribe("odds.raw.draftkings")

# PostgreSQL connection
conn = psycopg2.connect(host="store", database="oddsfeed", user="odds", password="odds")
cur = conn.cursor()

print("DraftKings normalizer (FIXED) started", flush=True)


# Function to ensure event exists
def ensure_event(event_id, home="TBD", away="TBD", sport="unknown"):
    try:
        cur.execute(
            """
            INSERT INTO events (id, brand, sport, league, home, away, start_time)
            VALUES (%s, 'draftkings', %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                home = EXCLUDED.home,
                away = EXCLUDED.away,
                sport = EXCLUDED.sport
        """,
            (event_id, sport, sport, home, away),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error ensuring event {event_id}: {e}", flush=True)


def insert_odds_and_ticks(odds_data):
    """Insert into both odds and ticks tables"""
    if not odds_data:
        return

    try:
        # Insert into odds table
        execute_batch(
            cur,
            """INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
               VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
               ON CONFLICT DO NOTHING""",
            odds_data,
        )

        # Also insert into ticks table for price history
        ticks_data = [
            (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
            for book, event_id, market, outcome_name, outcome_price, outcome_point, ts in odds_data
        ]

        execute_batch(
            cur,
            """INSERT INTO ticks (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
               VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
               ON CONFLICT DO NOTHING""",
            ticks_data,
        )

        conn.commit()
        print(
            f'Inserted {len(odds_data)} DraftKings odds + ticks at {datetime.now().strftime("%H:%M:%S")}',
            flush=True,
        )
    except Exception as e:
        print(f"Error inserting odds/ticks: {e}", flush=True)
        conn.rollback()


for msg in p.listen():
    if msg["type"] == "pmessage":
        try:
            data = json.loads(msg["data"])

            # Process DraftKings events
            odds_to_insert = []

            for event in data.get("events", []):
                event_id = event["id"]
                home = event.get("home", "TBD")
                away = event.get("away", "TBD")
                sport = event.get("sport", "unknown")

                # Ensure event exists
                ensure_event(event_id, home, away, sport)

                # Process odds for this event
                for odd in event.get("odds", []):
                    market = odd.get("market", "h2h")
                    timestamp = data.get("timestamp", time.time())

                    # Handle different market types with proper outcome names
                    if market == "h2h":
                        # Moneyline
                        if "home_price" in odd and odd["home_price"] is not None:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "moneyline",  # market
                                    home,  # outcome_name (team name)
                                    odd["home_price"],  # outcome_price
                                    None,  # outcome_point
                                    timestamp,  # ts
                                )
                            )
                        if "away_price" in odd and odd["away_price"] is not None:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "moneyline",  # market
                                    away,  # outcome_name (team name)
                                    odd["away_price"],  # outcome_price
                                    None,  # outcome_point
                                    timestamp,  # ts
                                )
                            )

                    elif market == "spreads":
                        # Spread betting
                        line = odd.get("line", 0)
                        if "home_price" in odd and odd["home_price"] is not None:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "spread",  # market
                                    f"{home} {line:+.1f}",  # outcome_name with line
                                    odd["home_price"],  # outcome_price
                                    line,  # outcome_point (spread value)
                                    timestamp,  # ts
                                )
                            )
                        if "away_price" in odd and odd["away_price"] is not None:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "spread",  # market
                                    f"{away} {-line:+.1f}",  # outcome_name with line
                                    odd["away_price"],  # outcome_price
                                    -line,  # outcome_point (spread value)
                                    timestamp,  # ts
                                )
                            )

                    elif market == "totals":
                        # Over/Under
                        total = odd.get("total", 0)
                        if "over_price" in odd and odd["over_price"] is not None:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "total",  # market
                                    f"Over {total}",  # outcome_name
                                    odd["over_price"],  # outcome_price
                                    total,  # outcome_point (total value)
                                    timestamp,  # ts
                                )
                            )
                        if "under_price" in odd and odd["under_price"] is not None:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "total",  # market
                                    f"Under {total}",  # outcome_name
                                    odd["under_price"],  # outcome_price
                                    total,  # outcome_point (total value)
                                    timestamp,  # ts
                                )
                            )

            # Batch insert odds and ticks
            insert_odds_and_ticks(odds_to_insert)

        except Exception as e:
            print(f"Error processing message: {e}", flush=True)
            conn.rollback()
