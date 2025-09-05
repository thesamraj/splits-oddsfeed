#!/usr/bin/env python3
import os
import redis
import json
import time
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime

# Redis connection
r = redis.from_url(os.getenv("REDIS_URL", "redis://broker:6379/0"))
p = r.pubsub()
p.psubscribe("odds.raw.pointsbet")

# PostgreSQL connection - example only; not executed in prod
# Use DATABASE_URL environment variable in production
if os.getenv("DATABASE_URL"):
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
else:
    # Placeholder for local development - replace with actual credentials
    conn = psycopg2.connect(
        host="<host>", database="<database>", user="<user>", password="<password>"
    )
cur = conn.cursor()

print("PointsBet normalizer started", flush=True)


# Function to ensure event exists
def ensure_event(event_id, home, away, sport, competition, starts_at):
    try:
        # Parse the starts_at timestamp if provided
        start_time = None
        if starts_at:
            try:
                start_time = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
            except:
                start_time = datetime.now()
        else:
            start_time = datetime.now()

        cur.execute(
            """
            INSERT INTO events (id, brand, sport, league, home, away, start_time)
            VALUES (%s, 'pointsbet', %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                home = EXCLUDED.home,
                away = EXCLUDED.away,
                sport = EXCLUDED.sport,
                league = EXCLUDED.league,
                start_time = EXCLUDED.start_time
        """,
            (event_id, sport.lower(), competition, home, away, start_time),
        )
        conn.commit()
    except Exception as e:
        print(f"Error ensuring event {event_id}: {e}", flush=True)
        conn.rollback()


for msg in p.listen():
    if msg["type"] == "pmessage":
        try:
            data = json.loads(msg["data"])

            # Process raw odds
            odds_to_insert = []

            # Process events
            for event in data.get("events", []):
                event_id = event.get("id")
                home = event.get("home", "TBD")
                away = event.get("away", "TBD")
                sport = event.get("sport", "unknown")
                competition = event.get("competition", "unknown")
                starts_at = event.get("starts_at")

                # Ensure event exists
                ensure_event(event_id, home, away, sport, competition, starts_at)

                # Process odds for each market
                for odd in event.get("odds", []):
                    market = odd.get("market")
                    timestamp = data.get("timestamp", time.time())

                    if market == "h2h":
                        # Head to head (moneyline)
                        if odd.get("home_price") is not None:
                            odds_to_insert.append(
                                (
                                    "pointsbet",  # book
                                    event_id,  # event_id
                                    "moneyline",  # market
                                    "home",  # outcome_name
                                    odd["home_price"],  # outcome_price
                                    None,  # outcome_point
                                    timestamp,  # ts
                                )
                            )
                        if odd.get("away_price") is not None:
                            odds_to_insert.append(
                                (
                                    "pointsbet",  # book
                                    event_id,  # event_id
                                    "moneyline",  # market
                                    "away",  # outcome_name
                                    odd["away_price"],  # outcome_price
                                    None,  # outcome_point
                                    timestamp,  # ts
                                )
                            )

                    elif market == "spreads":
                        # Spread betting
                        line = odd.get("line", 0)
                        if odd.get("home_price") is not None:
                            odds_to_insert.append(
                                (
                                    "pointsbet",  # book
                                    event_id,  # event_id
                                    "spread",  # market
                                    "home",  # outcome_name
                                    odd["home_price"],  # outcome_price
                                    line,  # outcome_point (spread line)
                                    timestamp,  # ts
                                )
                            )
                        if odd.get("away_price") is not None:
                            odds_to_insert.append(
                                (
                                    "pointsbet",  # book
                                    event_id,  # event_id
                                    "spread",  # market
                                    "away",  # outcome_name
                                    odd["away_price"],  # outcome_price
                                    -line,  # outcome_point (opposite spread for away)
                                    timestamp,  # ts
                                )
                            )

                    elif market == "totals":
                        # Over/Under
                        total = odd.get("total", 0)
                        if odd.get("over_price") is not None:
                            odds_to_insert.append(
                                (
                                    "pointsbet",  # book
                                    event_id,  # event_id
                                    "total",  # market
                                    "over",  # outcome_name
                                    odd["over_price"],  # outcome_price
                                    total,  # outcome_point (total line)
                                    timestamp,  # ts
                                )
                            )
                        if odd.get("under_price") is not None:
                            odds_to_insert.append(
                                (
                                    "pointsbet",  # book
                                    event_id,  # event_id
                                    "total",  # market
                                    "under",  # outcome_name
                                    odd["under_price"],  # outcome_price
                                    total,  # outcome_point (total line)
                                    timestamp,  # ts
                                )
                            )

            if odds_to_insert:
                execute_batch(
                    cur,
                    """INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, outcome_point, ts)
                       VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
                       ON CONFLICT DO NOTHING""",
                    odds_to_insert,
                )
                conn.commit()
                print(
                    f'Inserted {len(odds_to_insert)} PointsBet odds at {datetime.now().strftime("%H:%M:%S")}',
                    flush=True,
                )

        except Exception as e:
            print(f"Error: {e}", flush=True)
            conn.rollback()
