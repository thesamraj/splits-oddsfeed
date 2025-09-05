#!/usr/bin/env python3
"""
Fixed FanDuel Normalizer with new schema
- Properly extracts outcome_name and outcome_price
- Writes to both odds and ticks tables with new schema
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
p.psubscribe("odds.raw.fanduel")

# PostgreSQL connection
conn = psycopg2.connect(host="store", database="oddsfeed", user="odds", password="odds")
cur = conn.cursor()

print("FanDuel normalizer (NEW SCHEMA) started", flush=True)


# Function to ensure event exists
def ensure_event(event_id, home="TBD", away="TBD", sport="unknown"):
    try:
        cur.execute(
            """
            INSERT INTO events (id, brand, sport, league, home, away, start_time)
            VALUES (%s, 'fanduel', %s, %s, %s, %s, NOW())
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
    """Insert into both odds and ticks tables with new schema"""
    if not odds_data:
        return

    try:
        # Insert into odds table with new schema
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
            f'Inserted {len(odds_data)} FanDuel odds + ticks at {datetime.now().strftime("%H:%M:%S")}',
            flush=True,
        )
    except Exception as e:
        print(f"Error inserting odds/ticks: {e}", flush=True)
        conn.rollback()


for msg in p.listen():
    if msg["type"] == "pmessage":
        try:
            data = json.loads(msg["data"])

            # Process FanDuel events
            odds_to_insert = []

            for event in data.get("events", []):
                event_id = event.get("id", f"fd_{int(time.time() * 1000)}")
                home = event.get("home", "TBD")
                away = event.get("away", "TBD")
                sport = event.get("sport", "unknown")

                # Ensure event exists
                ensure_event(event_id, home, away, sport)

                # Process odds for this event
                for odd in event.get("odds", []):
                    market = odd.get("market", "h2h")
                    timestamp = data.get("timestamp", time.time())

                    # Determine outcome name and price
                    label = str(odd.get("label", "")).lower()
                    value = odd.get("value")

                    if not value:
                        continue

                    outcome_name = None
                    outcome_point = None

                    # Handle different market types
                    if market == "h2h" or market == "moneyline":
                        if "home" in label or home.lower() in label:
                            outcome_name = home
                        elif "away" in label or away.lower() in label:
                            outcome_name = away
                        else:
                            # Try to infer from position
                            outcome_name = odd.get(
                                "team", odd.get("participant", "Unknown")
                            )

                        if outcome_name:
                            odds_to_insert.append(
                                (
                                    "fanduel",  # book
                                    event_id,  # event_id
                                    "moneyline",  # market
                                    outcome_name,  # outcome_name
                                    value,  # outcome_price
                                    None,  # outcome_point
                                    timestamp,  # ts
                                )
                            )

                    elif market == "spreads" or market == "spread":
                        line = odd.get("line", 0)
                        team = odd.get("team", "")

                        if (
                            "home" in label
                            or home.lower() in label
                            or team.lower() == home.lower()
                        ):
                            outcome_name = f"{home} {line:+.1f}"
                            outcome_point = line
                        elif (
                            "away" in label
                            or away.lower() in label
                            or team.lower() == away.lower()
                        ):
                            outcome_name = f"{away} {line:+.1f}"
                            outcome_point = line
                        else:
                            outcome_name = f"{team} {line:+.1f}"
                            outcome_point = line

                        if outcome_name:
                            odds_to_insert.append(
                                (
                                    "fanduel",  # book
                                    event_id,  # event_id
                                    "spread",  # market
                                    outcome_name,  # outcome_name
                                    value,  # outcome_price
                                    outcome_point,  # outcome_point
                                    timestamp,  # ts
                                )
                            )

                    elif market == "totals" or market == "total":
                        total = odd.get("total", odd.get("line", 0))

                        if "over" in label:
                            outcome_name = f"Over {total}"
                        elif "under" in label:
                            outcome_name = f"Under {total}"
                        else:
                            outcome_name = f"{odd.get('type', 'Total')} {total}"

                        if outcome_name:
                            odds_to_insert.append(
                                (
                                    "fanduel",  # book
                                    event_id,  # event_id
                                    "total",  # market
                                    outcome_name,  # outcome_name
                                    value,  # outcome_price
                                    total,  # outcome_point
                                    timestamp,  # ts
                                )
                            )

            # Process any standalone raw_odds
            for odd in data.get("raw_odds", []):
                if odd.get("value"):
                    event_id = odd.get("event_id", f"fd_{int(time.time() * 1000)}")
                    outcome_name = odd.get("label", odd.get("team", "Unknown"))

                    odds_to_insert.append(
                        (
                            "fanduel",  # book
                            event_id,  # event_id
                            "moneyline",  # market (default)
                            outcome_name,  # outcome_name
                            odd["value"],  # outcome_price
                            None,  # outcome_point
                            data.get("timestamp", time.time()),  # ts
                        )
                    )

            # Batch insert odds and ticks
            insert_odds_and_ticks(odds_to_insert)

        except Exception as e:
            print(f"Error processing message: {e}", flush=True)
            conn.rollback()
