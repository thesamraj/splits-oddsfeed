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


def process_normalized_message(data):
    """Process normalized odds and create ticks for changes"""
    try:
        message = json.loads(data)

        if message.get("source") != "betmgm":
            return

        cursor = conn.cursor()

        # Query current odds and compare
        cursor.execute(
            """
            SELECT event_id, market, price_home, price_away, price_over, price_under,
                   line_home, line_away, line_total
            FROM odds
            WHERE book = 'betmgm'
        """
        )

        current_odds = {}
        for row in cursor.fetchall():
            key = f"{row[0]}_{row[1]}"  # event_id_market
            current_odds[key] = {
                "price_home": row[2],
                "price_away": row[3],
                "price_over": row[4],
                "price_under": row[5],
                "line_home": row[6],
                "line_away": row[7],
                "line_total": row[8],
            }

        # Check for changes and create ticks
        tick_count = 0
        for event_id, odds_data in message.get("odds", {}).items():
            for market, market_data in odds_data.items():
                key = f"{event_id}_{market}"

                if key in current_odds:
                    old = current_odds[key]

                    # Check for price changes
                    changes = []
                    if market == "h2h":
                        if old["price_home"] != market_data.get("price_home"):
                            changes.append(
                                (
                                    "price_home",
                                    old["price_home"],
                                    market_data.get("price_home"),
                                )
                            )
                        if old["price_away"] != market_data.get("price_away"):
                            changes.append(
                                (
                                    "price_away",
                                    old["price_away"],
                                    market_data.get("price_away"),
                                )
                            )
                    elif market == "spreads":
                        if old["price_home"] != market_data.get("price_home"):
                            changes.append(
                                (
                                    "price_home",
                                    old["price_home"],
                                    market_data.get("price_home"),
                                )
                            )
                        if old["price_away"] != market_data.get("price_away"):
                            changes.append(
                                (
                                    "price_away",
                                    old["price_away"],
                                    market_data.get("price_away"),
                                )
                            )
                        if old["line_home"] != market_data.get("line_home"):
                            changes.append(
                                (
                                    "line_home",
                                    old["line_home"],
                                    market_data.get("line_home"),
                                )
                            )
                    elif market == "totals":
                        if old["price_over"] != market_data.get("price_over"):
                            changes.append(
                                (
                                    "price_over",
                                    old["price_over"],
                                    market_data.get("price_over"),
                                )
                            )
                        if old["price_under"] != market_data.get("price_under"):
                            changes.append(
                                (
                                    "price_under",
                                    old["price_under"],
                                    market_data.get("price_under"),
                                )
                            )
                        if old["line_total"] != market_data.get("line_total"):
                            changes.append(
                                (
                                    "line_total",
                                    old["line_total"],
                                    market_data.get("line_total"),
                                )
                            )

                    # Create ticks for changes
                    for field, old_val, new_val in changes:
                        if old_val is not None and new_val is not None:
                            cursor.execute(
                                """
                                INSERT INTO ticks (event_id, book, market, field, old_value, new_value, created_at)
                                VALUES (%s, 'betmgm', %s, %s, %s, %s, NOW())
                            """,
                                (event_id, market, field, old_val, new_val),
                            )
                            tick_count += 1

        if tick_count > 0:
            print(f"Created {tick_count} BetMGM ticks", flush=True)

        cursor.close()

    except Exception as e:
        print(f"Error processing ticks: {e}", flush=True)


def main():
    """Main loop to process normalized BetMGM odds for ticks"""
    print(f"BetMGM ticks processor started at {datetime.now()}", flush=True)

    # Subscribe to normalized odds channel
    pubsub = r.pubsub()
    pubsub.subscribe("odds.norm.betmgm")

    for message in pubsub.listen():
        if message["type"] == "message":
            process_normalized_message(message["data"])


if __name__ == "__main__":
    main()
