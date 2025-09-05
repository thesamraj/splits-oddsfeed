#!/usr/bin/env python3
import os
import json
import time
import redis
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

REDIS_URL = os.getenv("REDIS_URL", "redis://broker:6379/0")
r = redis.from_url(REDIS_URL)


# Mock data generator for testing
def generate_mock_data(book):
    events = []
    for i in range(random.randint(3, 8)):
        event = {
            "id": f"{book}_{i}_{int(time.time())}",
            "home": f"Team {i*2}",
            "away": f"Team {i*2+1}",
            "sport": "NFL",
            "league": "NFL",
            "markets": [
                {
                    "type": "h2h",
                    "outcomes": [
                        {
                            "name": f"Team {i*2}",
                            "price": round(1.8 + random.random() * 0.4, 2),
                        },
                        {
                            "name": f"Team {i*2+1}",
                            "price": round(1.8 + random.random() * 0.4, 2),
                        },
                    ],
                },
                {
                    "type": "spread",
                    "outcomes": [
                        {"name": f"Team {i*2} -3.5", "price": 1.91},
                        {"name": f"Team {i*2+1} +3.5", "price": 1.91},
                    ],
                },
                {
                    "type": "total",
                    "outcomes": [
                        {"name": "Over 45.5", "price": 1.90},
                        {"name": "Under 45.5", "price": 1.92},
                    ],
                },
            ],
        }
        events.append(event)
    return events


def collect_book(book):
    try:
        # Generate mock data for all books
        events = generate_mock_data(book)

        msg = {
            "book": book,
            "brand": book,
            "timestamp": datetime.utcnow().isoformat(),
            "events": events,
        }

        # Publish to normalizer
        channel = f"odds.raw.{book}"
        r.publish(channel, json.dumps(msg))

        # Also write directly to DB for reliability
        from psycopg2 import connect

        conn = connect(
            os.getenv("DATABASE_URL", "postgresql://odds:odds@store:5432/oddsfeed")
        )
        cur = conn.cursor()

        for event in events:
            # First insert event
            cur.execute(
                """
                INSERT INTO events (id, home, away, sport, league, start_time)
                VALUES (%s, %s, %s, %s, %s, NOW() + interval '1 day')
                ON CONFLICT (id) DO NOTHING
            """,
                (
                    event["id"],
                    event.get("home", ""),
                    event.get("away", ""),
                    event.get("sport", "NFL"),
                    event.get("league", "NFL"),
                ),
            )

            # Then insert odds
            for market in event.get("markets", []):
                for outcome in market.get("outcomes", []):
                    cur.execute(
                        """
                        INSERT INTO odds (book, event_id, market, outcome_name, outcome_price, ts)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT DO NOTHING
                    """,
                        (
                            book,
                            event["id"],
                            market["type"],
                            outcome["name"],
                            outcome["price"],
                        ),
                    )

        conn.commit()
        conn.close()
        print(f"{book}: Published {len(events)} events", flush=True)

    except Exception as e:
        print(f"{book}: Error - {e}", flush=True)


# All 13 books
BOOKS = [
    "barstool",
    "betmgm",
    "betrivers",
    "bovada",
    "caesars",
    "draftkings",
    "fanduel",
    "pinnacle",
    "pointsbet",
    "stake",
    "sugarhouse",
    "unibet",
    "bet365",
]

print(f"Unified collector started for {len(BOOKS)} books", flush=True)

# Install psycopg2
os.system("pip install psycopg2-binary >/dev/null 2>&1")

while True:
    with ThreadPoolExecutor(max_workers=13) as executor:
        executor.map(collect_book, BOOKS)
    time.sleep(20)
