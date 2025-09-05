#!/usr/bin/env python3
"""Tests for Bovada parser functions"""

import unittest
import sys
import os
import time
from datetime import datetime, timezone, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Mock the dependencies
class MockRedis:
    def from_url(self, url):
        return self


class MockGate:
    def record_upstream(self, **kwargs):
        pass


# Mock modules before import
sys.modules["redis"] = MockRedis()
sys.modules["prometheus_client"] = type(sys)("prometheus_client")
sys.modules["flask"] = type(sys)("flask")
sys.modules["collectors.base.explainable_realness"] = type(sys)("explainable_realness")

# Now we can import
from collectors.bovada_real.bovada_real_enhanced import BovadaRealCollector


class TestBovadaParser(unittest.TestCase):

    def setUp(self):
        """Set up test collector"""
        # Mock environment
        os.environ["REDIS_URL"] = "redis://localhost:6379/0"

        # Create collector instance
        self.collector = BovadaRealCollector()
        self.collector.gate = MockGate()

    def test_event_id_stability(self):
        """Test that event IDs are stable and consistent"""
        event1 = {
            "id": "12345",
            "competitors": [
                {"name": "Team A", "home": False},
                {"name": "Team B", "home": True},
            ],
            "startTime": 1800000000000,
        }

        # Same event, should get same ID
        event2 = dict(event1)

        id1 = self.collector.parse_event_id(event1)
        id2 = self.collector.parse_event_id(event2)

        self.assertEqual(id1, id2)
        self.assertEqual(id1, "bovada_12345")

        # Test fallback with no ID
        event3 = {
            "path": [{"id": "nfl"}],
            "competitors": [
                {"name": "Patriots", "home": False},
                {"name": "Jets", "home": True},
            ],
            "startTime": 1800000000000,
        }

        id3 = self.collector.parse_event_id(event3)
        self.assertIsNotNone(id3)
        self.assertTrue(id3.startswith("bovada_"))

    def test_team_normalization(self):
        """Test team name normalization and aliasing"""
        event = {
            "competitors": [
                {"name": "1. NY Jets", "home": False},
                {"name": "  TB Buccaneers  ", "home": True},
            ]
        }

        home, away = self.collector.parse_teams(event)

        # Should normalize NY Jets and strip ranking
        self.assertEqual(away, "New York Jets")
        # Should strip whitespace and apply Tampa Bay alias
        self.assertEqual(home, "Tampa Bay Buccaneers")

    def test_markets_mapping_h2h_spread_total(self):
        """Test market parsing for h2h, spread, and total"""
        event = {
            "displayGroups": [
                {
                    "markets": [
                        {
                            "id": "m1",
                            "description": "Moneyline",
                            "outcomes": [
                                {
                                    "id": "o1",
                                    "description": "Patriots",
                                    "price": {"american": "-150"},
                                },
                                {
                                    "id": "o2",
                                    "description": "Jets",
                                    "price": {"american": "+130"},
                                },
                            ],
                        },
                        {
                            "id": "m2",
                            "description": "Point Spread",
                            "outcomes": [
                                {
                                    "id": "o3",
                                    "description": "Patriots -3.5",
                                    "price": {"american": "-110", "handicap": -3.5},
                                }
                            ],
                        },
                        {
                            "id": "m3",
                            "description": "Total Points",
                            "outcomes": [
                                {
                                    "id": "o4",
                                    "description": "Over 45.5",
                                    "price": {"american": "-105", "handicap": 45.5},
                                },
                                {
                                    "id": "o5",
                                    "description": "Under 45.5",
                                    "price": {"american": "-115", "handicap": 45.5},
                                },
                            ],
                        },
                    ]
                }
            ]
        }

        markets = self.collector.parse_markets(event, "Patriots", "Jets")

        # Should have 5 markets total
        self.assertEqual(len(markets), 5)

        # Check h2h markets
        h2h_markets = [m for m in markets if m["market_type"] == "h2h"]
        self.assertEqual(len(h2h_markets), 2)

        # Check spread
        spread_markets = [m for m in markets if m["market_type"] == "spread"]
        self.assertEqual(len(spread_markets), 1)
        self.assertEqual(spread_markets[0]["line"], -3.5)

        # Check totals
        total_markets = [m for m in markets if m["market_type"] == "total"]
        self.assertEqual(len(total_markets), 2)
        self.assertEqual(total_markets[0]["line"], 45.5)

    def test_dedup_ringbuffer_ttl(self):
        """Test dedup ring buffer with TTL"""
        # Add an entry
        is_dup1 = self.collector.is_duplicate("ev1", "h2h", "home", None, 150)
        self.assertFalse(is_dup1)

        # Same entry should be duplicate
        is_dup2 = self.collector.is_duplicate("ev1", "h2h", "home", None, 150)
        self.assertTrue(is_dup2)

        # Different price (after rounding) should not be duplicate
        is_dup3 = self.collector.is_duplicate("ev1", "h2h", "home", None, 155)
        self.assertFalse(is_dup3)

        # Price jitter within rounding should be duplicate
        is_dup4 = self.collector.is_duplicate("ev1", "h2h", "home", None, 150.01)
        self.assertTrue(is_dup4)

        # Test TTL expiry (mock time)
        old_entry_key = "test_old:h2h:home:0:100.0"
        self.collector.dedup_buffer[old_entry_key] = time.time() - 3600  # 1 hour old

        # Clean and check it's removed
        self.collector.clean_dedup_buffer()
        self.assertNotIn(old_entry_key, self.collector.dedup_buffer)

    def test_time_spread_histogram(self):
        """Test that start times are properly parsed for time spread"""
        now = datetime.now(timezone.utc)

        # Event today at 1pm
        event1 = {
            "startTime": int(
                (
                    now.replace(hour=13, minute=0, second=0) + timedelta(days=0)
                ).timestamp()
                * 1000
            )
        }
        time1 = self.collector.parse_start_time(event1)
        self.assertIsNotNone(time1)

        # Event tomorrow at 7pm
        event2 = {
            "startTime": int(
                (
                    now.replace(hour=19, minute=0, second=0) + timedelta(days=1)
                ).timestamp()
                * 1000
            )
        }
        time2 = self.collector.parse_start_time(event2)
        self.assertIsNotNone(time2)

        # Event in 3 days at 4pm
        event3 = {
            "startTime": int(
                (
                    now.replace(hour=16, minute=0, second=0) + timedelta(days=3)
                ).timestamp()
                * 1000
            )
        }
        time3 = self.collector.parse_start_time(event3)
        self.assertIsNotNone(time3)

        # Parse times and check they span multiple days
        dt1 = datetime.fromisoformat(time1)
        dt2 = datetime.fromisoformat(time2)
        dt3 = datetime.fromisoformat(time3)

        time_range = (dt3 - dt1).total_seconds() / 3600
        self.assertGreater(time_range, 48)  # Should span > 48 hours

        # Check different hour buckets
        buckets = set()
        for dt in [dt1, dt2, dt3]:
            bucket = dt.strftime("%Y-%m-%d_%H")
            buckets.add(bucket)

        self.assertEqual(len(buckets), 3)  # 3 different hour buckets


if __name__ == "__main__":
    unittest.main()
