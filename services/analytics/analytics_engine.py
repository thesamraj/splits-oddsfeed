#!/usr/bin/env python3
"""
Analytics Engine - Real-time odds comparison and arbitrage detection
Monitors all 13 books for opportunities and anomalies
"""

import os
import json
import time
import redis
import psycopg2
import logging
from datetime import datetime
from collections import defaultdict
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("analytics_engine")

# Config
REDIS_HOST = os.getenv("REDIS_HOST", "broker")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DB_HOST = os.getenv("DB_HOST", "store")
DB_NAME = os.getenv("DB_NAME", "oddsfeed")
DB_USER = os.getenv("DB_USER", "<user>")  # Placeholder
DB_PASS = os.getenv("DB_PASS", "<password>")  # Placeholder
INTERVAL = int(os.getenv("INTERVAL", 10))

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


class AnalyticsEngine:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.conn = None
        self.connect_db()

    def connect_db(self):
        """Connect to PostgreSQL"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS
            )
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            time.sleep(5)
            self.connect_db()

    def get_current_odds(self):
        """Get current odds for all books"""
        try:
            cur = self.conn.cursor()

            # Get latest odds for each event/market combo
            query = """
            WITH latest_odds AS (
                SELECT DISTINCT ON (book, event_id, market, outcome_name)
                    book,
                    event_id,
                    e.home,
                    e.away,
                    market,
                    outcome_name,
                    outcome_price,
                    outcome_point,
                    o.ts
                FROM odds o
                JOIN events e ON o.event_id = e.id
                WHERE o.ts > NOW() - INTERVAL '5 minutes'
                ORDER BY book, event_id, market, outcome_name, o.ts DESC
            )
            SELECT * FROM latest_odds
            ORDER BY event_id, market, book;
            """

            cur.execute(query)
            return cur.fetchall()

        except Exception as e:
            logger.error(f"Error fetching odds: {e}")
            self.connect_db()
            return []

    def find_arbitrage_opportunities(self, odds_data):
        """Find arbitrage betting opportunities"""
        arb_opportunities = []

        # Group by event and market
        events = defaultdict(lambda: defaultdict(list))

        for row in odds_data:
            book, event_id, home, away, market, outcome, price, point, ts = row
            if price and market == "h2h":  # Moneyline only for now
                key = f"{home} vs {away}"
                events[key][outcome].append(
                    {"book": book, "price": price, "event_id": event_id}
                )

        # Check each matchup for arbitrage
        for matchup, outcomes in events.items():
            if len(outcomes) >= 2:  # Need at least 2 outcomes
                home_best = None
                away_best = None

                # Find best odds for each side
                for outcome, books in outcomes.items():
                    best = max(
                        books, key=lambda x: self.american_to_decimal(x["price"])
                    )

                    if (
                        "home" in outcome.lower()
                        or outcomes.keys().__len__() == 2
                        and not away_best
                    ):
                        if not home_best or self.american_to_decimal(
                            best["price"]
                        ) > self.american_to_decimal(home_best["price"]):
                            home_best = best
                    else:
                        if not away_best or self.american_to_decimal(
                            best["price"]
                        ) > self.american_to_decimal(away_best["price"]):
                            away_best = best

                # Calculate arbitrage
                if home_best and away_best and home_best["book"] != away_best["book"]:
                    home_decimal = self.american_to_decimal(home_best["price"])
                    away_decimal = self.american_to_decimal(away_best["price"])

                    if home_decimal and away_decimal:
                        arb_percentage = (1 / home_decimal + 1 / away_decimal) * 100

                        if arb_percentage < 100:  # Arbitrage exists
                            profit_margin = 100 - arb_percentage
                            arb_opportunities.append(
                                {
                                    "matchup": matchup,
                                    "home_book": home_best["book"],
                                    "home_price": home_best["price"],
                                    "away_book": away_best["book"],
                                    "away_price": away_best["price"],
                                    "arb_percentage": round(arb_percentage, 2),
                                    "profit_margin": round(profit_margin, 2),
                                }
                            )

        return arb_opportunities

    def american_to_decimal(self, american_odds):
        """Convert American odds to decimal"""
        try:
            odds = float(american_odds)
            if odds > 0:
                return (odds / 100) + 1
            else:
                return (100 / abs(odds)) + 1
        except:
            return None

    def find_value_bets(self, odds_data):
        """Find value bets by comparing to sharp books"""
        value_bets = []
        sharp_books = ["pinnacle", "bovada"]  # Considered sharp

        # Group by event
        events = defaultdict(lambda: defaultdict(dict))

        for row in odds_data:
            book, event_id, home, away, market, outcome, price, point, ts = row
            if price and market == "h2h":
                key = f"{home} vs {away}"
                events[key][book][outcome] = price

        # Find value
        for matchup, books in events.items():
            sharp_prices = {}

            # Get sharp book prices
            for sharp in sharp_books:
                if sharp in books:
                    sharp_prices.update(books[sharp])

            if not sharp_prices:
                continue

            # Compare other books to sharp
            for book, outcomes in books.items():
                if book not in sharp_books:
                    for outcome, price in outcomes.items():
                        if outcome in sharp_prices:
                            sharp_decimal = self.american_to_decimal(
                                sharp_prices[outcome]
                            )
                            book_decimal = self.american_to_decimal(price)

                            if sharp_decimal and book_decimal:
                                # Calculate implied probability difference
                                sharp_prob = 1 / sharp_decimal
                                book_prob = 1 / book_decimal

                                # Value exists if book offers better odds than sharp
                                if book_decimal > sharp_decimal:
                                    value_percentage = (
                                        (book_decimal - sharp_decimal) / sharp_decimal
                                    ) * 100

                                    if value_percentage > 2:  # At least 2% value
                                        value_bets.append(
                                            {
                                                "matchup": matchup,
                                                "outcome": outcome,
                                                "book": book,
                                                "price": price,
                                                "sharp_price": sharp_prices[outcome],
                                                "value_percentage": round(
                                                    value_percentage, 2
                                                ),
                                            }
                                        )

        return value_bets

    def calculate_market_statistics(self, odds_data):
        """Calculate market statistics"""
        stats = {
            "total_events": set(),
            "books_active": set(),
            "markets_covered": set(),
            "avg_prices_by_book": defaultdict(list),
            "total_records": len(odds_data),
        }

        for row in odds_data:
            book, event_id, home, away, market, outcome, price, point, ts = row
            stats["total_events"].add(event_id)
            stats["books_active"].add(book)
            stats["markets_covered"].add(market)

            if price:
                decimal = self.american_to_decimal(price)
                if decimal:
                    stats["avg_prices_by_book"][book].append(decimal)

        # Calculate averages
        book_margins = {}
        for book, prices in stats["avg_prices_by_book"].items():
            if prices:
                avg_margin = statistics.mean([1 / p for p in prices]) * 100 - 100
                book_margins[book] = round(avg_margin, 2)

        return {
            "total_unique_events": len(stats["total_events"]),
            "books_active": len(stats["books_active"]),
            "markets_covered": len(stats["markets_covered"]),
            "total_records": stats["total_records"],
            "book_margins": book_margins,
        }

    def publish_analytics(self, analytics_data):
        """Publish analytics to Redis"""
        self.redis_client.publish("analytics.results", json.dumps(analytics_data))
        self.redis_client.set("analytics:latest", json.dumps(analytics_data))

        # Also store in database for historical tracking
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                INSERT INTO analytics_log (timestamp, data)
                VALUES (NOW(), %s)
            """,
                (json.dumps(analytics_data),),
            )
            self.conn.commit()
        except:
            # Table might not exist yet
            pass

    def run(self):
        """Main analytics loop"""
        logger.info("Analytics Engine started - Monitoring all 13 books")

        # Create analytics table if needed
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_log (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT NOW(),
                    data JSONB
                )
            """
            )
            self.conn.commit()
        except:
            pass

        while True:
            try:
                # Get current odds
                odds_data = self.get_current_odds()

                if odds_data:
                    # Run analytics
                    arbitrage = self.find_arbitrage_opportunities(odds_data)
                    value_bets = self.find_value_bets(odds_data)
                    market_stats = self.calculate_market_statistics(odds_data)

                    # Compile results
                    analytics = {
                        "timestamp": datetime.utcnow().isoformat(),
                        "arbitrage_opportunities": arbitrage[:10],  # Top 10
                        "value_bets": sorted(
                            value_bets,
                            key=lambda x: x["value_percentage"],
                            reverse=True,
                        )[:10],
                        "market_statistics": market_stats,
                        "arbitrage_count": len(arbitrage),
                        "value_bet_count": len(value_bets),
                    }

                    # Log summary
                    logger.info(
                        f"Found {len(arbitrage)} arbitrage opportunities, {len(value_bets)} value bets"
                    )
                    logger.info(
                        f"Market stats: {market_stats['total_unique_events']} events, {market_stats['books_active']} books"
                    )

                    if arbitrage:
                        logger.info(
                            f"Best arbitrage: {arbitrage[0]['matchup']} - {arbitrage[0]['profit_margin']}% profit"
                        )

                    # Publish results
                    self.publish_analytics(analytics)

            except Exception as e:
                logger.error(f"Analytics error: {e}")
                self.connect_db()

            time.sleep(INTERVAL)


if __name__ == "__main__":
    engine = AnalyticsEngine()
    engine.run()
