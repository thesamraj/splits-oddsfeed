#!/usr/bin/env python3
import redis
import json
import psycopg2
from psycopg2.extras import execute_batch

# Redis connection
r = redis.from_url("redis://broker:6379/0")
p = r.pubsub()
p.psubscribe("odds.raw.draftkings")

# PostgreSQL connection
conn = psycopg2.connect(host="store", database="oddsfeed", user="odds", password="odds")
cur = conn.cursor()

print("DraftKings normalizer started", flush=True)


# Function to ensure event exists
def ensure_event(event_id, home="TBD", away="TBD"):
    try:
        cur.execute(
            """
            INSERT INTO events (id, brand, sport, league, home, away, start_time)
            VALUES (%s, 'draftkings', 'unknown', 'unknown', %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE SET
                home = EXCLUDED.home,
                away = EXCLUDED.away
        """,
            (event_id, home, away),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error ensuring event {event_id}: {e}", flush=True)


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

                # Ensure event exists
                ensure_event(event_id, home, away)

                # Process odds for this event
                for odd in event.get("odds", []):
                    market = odd.get("market", "h2h")
                    timestamp = data["timestamp"]

                    # Handle different market types
                    if market == "h2h":
                        # Moneyline
                        if "home_price" in odd:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "h2h",  # market
                                    odd["home_price"],  # price_home
                                    None,  # price_away
                                    None,  # price_over
                                    None,  # price_under
                                    None,  # line
                                    None,  # total
                                    timestamp,  # ts
                                )
                            )
                        if "away_price" in odd:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "h2h",  # market
                                    None,  # price_home
                                    odd["away_price"],  # price_away
                                    None,  # price_over
                                    None,  # price_under
                                    None,  # line
                                    None,  # total
                                    timestamp,  # ts
                                )
                            )
                    elif market == "spreads":
                        # Spread betting
                        line = odd.get("line", 0) * 1000  # Convert to Kambi-style
                        if "home_price" in odd:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "spreads",  # market
                                    odd["home_price"],  # price_home
                                    None,  # price_away
                                    None,  # price_over
                                    None,  # price_under
                                    line,  # line
                                    None,  # total
                                    timestamp,  # ts
                                )
                            )
                        if "away_price" in odd:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "spreads",  # market
                                    None,  # price_home
                                    odd["away_price"],  # price_away
                                    None,  # price_over
                                    None,  # price_under
                                    line,  # line
                                    None,  # total
                                    timestamp,  # ts
                                )
                            )
                    elif market == "totals":
                        # Over/Under
                        total = odd.get("total", 0) * 1000  # Convert to Kambi-style
                        if "over_price" in odd:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "totals",  # market
                                    None,  # price_home
                                    None,  # price_away
                                    odd["over_price"],  # price_over
                                    None,  # price_under
                                    None,  # line
                                    total,  # total
                                    timestamp,  # ts
                                )
                            )
                        if "under_price" in odd:
                            odds_to_insert.append(
                                (
                                    "draftkings",  # book
                                    event_id,  # event_id
                                    "totals",  # market
                                    None,  # price_home
                                    None,  # price_away
                                    None,  # price_over
                                    odd["under_price"],  # price_under
                                    None,  # line
                                    total,  # total
                                    timestamp,  # ts
                                )
                            )

            # Batch insert odds
            if odds_to_insert:
                execute_batch(
                    cur,
                    """INSERT INTO odds (book, event_id, market, price_home, price_away, price_over, price_under, line, total, ts)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))
                       ON CONFLICT DO NOTHING""",
                    odds_to_insert,
                )
                conn.commit()
                print(f"Inserted {len(odds_to_insert)} DraftKings odds", flush=True)

        except Exception as e:
            print(f"Error: {e}", flush=True)
            conn.rollback()
