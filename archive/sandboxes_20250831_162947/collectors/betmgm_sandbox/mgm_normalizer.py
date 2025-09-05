#!/usr/bin/env python3
import json
import redis
import psycopg2
from datetime import datetime

# Redis connection
r = redis.from_url("redis://broker:6379/0")

# PostgreSQL connection
conn = psycopg2.connect(
    host="store", port=5432, dbname="oddsfeed", user="odds", password="odds"
)
conn.autocommit = True


def ensure_event(cursor, event_id, home, away):
    """Ensure event exists in database"""
    try:
        cursor.execute(
            """
            INSERT INTO events (id, league, start_time, home, away, sport, brand, created_at)
            VALUES (%s, 'TBD', NOW(), %s, %s, 'TBD', 'betmgm', NOW())
            ON CONFLICT (id) DO NOTHING
        """,
            (event_id, home, away),
        )
    except Exception as e:
        print(f"Error ensuring event {event_id}: {e}", flush=True)


def process_message(channel, data):
    """Process incoming BetMGM odds data"""
    try:
        message = json.loads(data)
        events = message.get("events", [])

        cursor = conn.cursor()

        for event in events:
            event_id = event["id"]
            home = event.get("home", "TBD")
            away = event.get("away", "TBD")

            # Ensure event exists
            ensure_event(cursor, event_id, home, away)

            # Process odds
            for odds in event.get("odds", []):
                market = odds.get("market", "h2h")

                if market == "h2h":
                    # Head to head market
                    home_price = odds.get("home_price")
                    away_price = odds.get("away_price")

                    if home_price and away_price:
                        cursor.execute(
                            """
                            INSERT INTO odds (event_id, book, market, price_home, price_away, ts)
                            VALUES (%s, 'betmgm', 'h2h', %s, %s, NOW())
                        """,
                            (event_id, home_price, away_price),
                        )

                elif market == "spreads":
                    # Spread market
                    home_price = odds.get("home_price")
                    away_price = odds.get("away_price")
                    line = odds.get("line")

                    if home_price and away_price and line is not None:
                        cursor.execute(
                            """
                            INSERT INTO odds (event_id, book, market, price_home, price_away, line, ts)
                            VALUES (%s, 'betmgm', 'spreads', %s, %s, %s, NOW())
                        """,
                            (event_id, home_price, away_price, line),
                        )

                elif market == "totals":
                    # Totals market
                    over_price = odds.get("over_price")
                    under_price = odds.get("under_price")
                    total = odds.get("total")

                    if over_price and under_price and total is not None:
                        cursor.execute(
                            """
                            INSERT INTO odds (event_id, book, market, price_over, price_under, total, ts)
                            VALUES (%s, 'betmgm', 'totals', %s, %s, %s, NOW())
                        """,
                            (event_id, over_price, under_price, total),
                        )

        cursor.close()
        print(f"Processed {len(events)} BetMGM events", flush=True)

    except Exception as e:
        print(f"Error processing message: {e}", flush=True)


def main():
    """Main loop to listen for BetMGM odds data"""
    print(f"BetMGM normalizer started at {datetime.now()}", flush=True)

    # Subscribe to BetMGM raw odds channel
    pubsub = r.pubsub()
    pubsub.subscribe("odds.raw.betmgm")

    for message in pubsub.listen():
        if message["type"] == "message":
            process_message(message["channel"], message["data"])


if __name__ == "__main__":
    main()
