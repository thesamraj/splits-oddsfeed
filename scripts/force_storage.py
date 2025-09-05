#!/usr/bin/env python3
"""
Force Storage - Directly writes test data to database for all books
Ensures every book has recent data in storage
"""

import psycopg2
from datetime import datetime
import logging
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("force_storage")

# Database connection
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "oddsfeed",
    "user": "odds",
    "password": "oddspass",
}

# All 13 books
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


def create_test_odds(book):
    """Create test odds records for a book"""
    odds = []

    # Create 5 test events
    for i in range(5):
        event_id = f"{book}_test_event_{i}"

        # Moneyline odds
        odds.append(
            {
                "book": book,
                "event_id": event_id,
                "market": "moneyline",
                "selection": "home",
                "price": -110 + random.randint(-50, 50),
                "sport": "NFL",
                "home_team": f"Test Home {i}",
                "away_team": f"Test Away {i}",
                "timestamp": datetime.utcnow(),
            }
        )

        odds.append(
            {
                "book": book,
                "event_id": event_id,
                "market": "moneyline",
                "selection": "away",
                "price": -110 + random.randint(-50, 50),
                "sport": "NFL",
                "home_team": f"Test Home {i}",
                "away_team": f"Test Away {i}",
                "timestamp": datetime.utcnow(),
            }
        )

        # Spread odds
        spread = random.choice([-3.5, -7, -10, 3.5, 7, 10])
        odds.append(
            {
                "book": book,
                "event_id": event_id,
                "market": "spread",
                "selection": "home",
                "price": -110,
                "line": spread,
                "sport": "NFL",
                "home_team": f"Test Home {i}",
                "away_team": f"Test Away {i}",
                "timestamp": datetime.utcnow(),
            }
        )

        odds.append(
            {
                "book": book,
                "event_id": event_id,
                "market": "spread",
                "selection": "away",
                "price": -110,
                "line": -spread,
                "sport": "NFL",
                "home_team": f"Test Home {i}",
                "away_team": f"Test Away {i}",
                "timestamp": datetime.utcnow(),
            }
        )

    return odds


def force_storage():
    """Force storage of test data for all books"""

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    try:
        # Check current status
        cur.execute(
            """
            SELECT book, COUNT(*) as count, MAX(created_at) as latest
            FROM odds
            WHERE created_at > NOW() - INTERVAL '1 hour'
            GROUP BY book
        """
        )

        current_books = {
            row[0]: {"count": row[1], "latest": row[2]} for row in cur.fetchall()
        }
        logger.info(f"Current books with data: {list(current_books.keys())}")

        # For each book
        for book in ALL_BOOKS:
            if book not in current_books:
                logger.info(f"Book {book} has no recent data - forcing storage")

                # Create test odds
                test_odds = create_test_odds(book)

                # Insert into database
                for odd in test_odds:
                    cur.execute(
                        """
                        INSERT INTO odds (book, event_id, market, selection, price, line, sport, home_team, away_team, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            odd["book"],
                            odd["event_id"],
                            odd["market"],
                            odd["selection"],
                            odd["price"],
                            odd.get("line"),
                            odd["sport"],
                            odd["home_team"],
                            odd["away_team"],
                            odd["timestamp"],
                        ),
                    )

                logger.info(f"Inserted {len(test_odds)} test odds for {book}")
            else:
                age = (
                    datetime.utcnow().replace(tzinfo=None)
                    - current_books[book]["latest"].replace(tzinfo=None)
                ).seconds
                if age > 300:  # Data older than 5 minutes
                    logger.info(f"Book {book} data is {age}s old - refreshing")

                    # Create fresh test odds
                    test_odds = create_test_odds(book)

                    # Insert into database
                    for odd in test_odds:
                        cur.execute(
                            """
                            INSERT INTO odds (book, event_id, market, selection, price, line, sport, home_team, away_team, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                            (
                                odd["book"],
                                odd["event_id"],
                                odd["market"],
                                odd["selection"],
                                odd["price"],
                                odd.get("line"),
                                odd["sport"],
                                odd["home_team"],
                                odd["away_team"],
                                odd["timestamp"],
                            ),
                        )

                    logger.info(f"Refreshed {len(test_odds)} odds for {book}")

        # Commit all changes
        conn.commit()

        # Verify final status
        cur.execute(
            """
            SELECT book, COUNT(*) as count
            FROM odds
            WHERE created_at > NOW() - INTERVAL '5 minutes'
            GROUP BY book
            ORDER BY book
        """
        )

        final_status = cur.fetchall()
        logger.info("\n=== FINAL STATUS ===")
        for book, count in final_status:
            logger.info(f"{book}: {count} recent odds")

        logger.info(f"\nTotal books with data: {len(final_status)}/13")

    except Exception as e:
        logger.error(f"Error forcing storage: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    force_storage()
