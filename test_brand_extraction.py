#!/usr/bin/env python3
"""
Test script to verify brand extraction works properly
"""
import redis
import json
import time

# Test envelope that simulates what a Kambi browser collector would publish
test_envelope = {
    "capture_id": "test-12345",
    "transport": "websocket",
    "url": "https://eu1.offering-api.kambicdn.com/offering/v2018/rsi2uspa/betoffer/outcome.json?lang=en_US&market=US-PA",
    "page_url": "https://pa.betrivers.com/?page=sportsbook#american_football/nfl",
    "source_ts_ms": int(time.time() * 1000),
    "received_ts_ms": int(time.time() * 1000),
    "event_id": "1023693113",
    "content_type": "application/json",
    "payload": json.dumps(
        {
            "betOffers": [
                {
                    "criterion": {"label": "Point Spread"},
                    "outcomes": [
                        {
                            "label": "Home Team -7",
                            "odds": 1940,  # Kambi scaled decimal
                            "line": -7000,
                        },
                        {"label": "Away Team +7", "odds": 1840, "line": 7000},
                    ],
                }
            ]
        }
    ),
}


def test_brand_extraction():
    # Connect to Redis
    r = redis.from_url("redis://localhost:6379/0")

    # Publish to kambi channel
    channel = "odds.raw.kambi"
    message = json.dumps(test_envelope)

    print(f"Publishing test message to {channel}")
    print(f"URL: {test_envelope['url']}")
    print("Expected brand: betrivers (from rsi2uspa token)")

    r.publish(channel, message)
    print("Message published successfully!")


if __name__ == "__main__":
    test_brand_extraction()
