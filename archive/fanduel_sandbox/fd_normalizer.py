#!/usr/bin/env python3
import redis
import json
import time
import psycopg2
from psycopg2.extras import execute_batch

# Redis connection
r = redis.from_url("redis://broker:6379/0")
p = r.pubsub()
p.psubscribe("odds.raw.fanduel")

# PostgreSQL connection
conn = psycopg2.connect(host="store", database="oddsfeed", user="odds", password="odds")
cur = conn.cursor()

print("FanDuel normalizer started", flush=True)


# Function to ensure event exists
def ensure_event(event_id):
    try:
        cur.execute(
            """
            INSERT INTO events (id, brand, sport, league, home, away, start_time)
            VALUES (%s, 'fanduel', 'unknown', 'unknown', 'TBD', 'TBD', NOW())
            ON CONFLICT (id) DO NOTHING
        """,
            (event_id,),
        )
        conn.commit()
    except Exception:
        conn.rollback()


for msg in p.listen():
    if msg["type"] == "pmessage":
        try:
            data = json.loads(msg["data"])

            # Process raw odds
            odds_to_insert = []

            # Process events with proper IDs
            event_ids = set()
            for event in data.get("events", []):
                event_id = event.get("id", f"fd_{int(time.time() * 1000)}")
                event_ids.add(event_id)
                for odd in event.get("odds", []):
                    if odd.get("value"):
                        odds_to_insert.append(
                            (
                                "fanduel",  # book
                                event_id,  # event_id
                                "moneyline",  # market
                                "home",  # outcome_name
                                odd["value"],  # outcome_price
                                None,  # outcome_point
                                data["timestamp"],  # ts
                            )
                        )

            # Also process standalone odds
            for odd in data.get("raw_odds", []):
                if odd.get("value"):
                    event_id = odd.get("event_id", f"fd_{int(time.time() * 1000)}")
                    event_ids.add(event_id)
                    odds_to_insert.append(
                        (
                            "fanduel",  # book
                            event_id,  # event_id
                            "moneyline",  # market
                            "away",  # outcome_name
                            odd["value"],  # outcome_price
                            None,  # outcome_point
                            data["timestamp"],  # ts
                        )
                    )

            # Ensure all events exist
            for event_id in event_ids:
                ensure_event(event_id)

            if odds_to_insert:
                execute_batch(
                    cur,
                    """INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
                       VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
                       ON CONFLICT DO NOTHING""",
                    odds_to_insert,
                )
                conn.commit()
                print(f"Inserted {len(odds_to_insert)} FanDuel odds", flush=True)

        except Exception as e:
            print(f"Error: {e}", flush=True)
            conn.rollback()
