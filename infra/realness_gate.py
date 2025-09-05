#!/usr/bin/env python3
"""
Realness Gate - Production safety module to validate real bookmaker data
Prevents test/mock/generated data from entering production systems
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
from collections import defaultdict
import math
import re

logger = logging.getLogger("realness_gate")


class RealnessGate:
    """
    Production gate to validate real bookmaker data vs generated/test data
    """

    def __init__(self, strict_mode: bool = True):
        """
        Initialize realness gate

        Args:
            strict_mode: If True, blocks all suspicious data. If False, only blocks obvious fakes.
        """
        self.strict_mode = strict_mode
        self.metrics = defaultdict(int)
        self.last_reset = datetime.now(timezone.utc)

        # Known test patterns
        self.test_patterns = [
            r"test",
            r"demo",
            r"sample",
            r"example",
            r"fake",
            r"mock",
            r"generated",
            r"placeholder",
        ]

        # Known real team patterns (partial list for validation)
        self.real_teams = {
            "nfl": {
                "kansas city chiefs",
                "buffalo bills",
                "miami dolphins",
                "baltimore ravens",
                "cincinnati bengals",
                "cleveland browns",
                "philadelphia eagles",
                "dallas cowboys",
                "new york giants",
                "san francisco 49ers",
                "seattle seahawks",
                "los angeles rams",
            },
            "nba": {
                "los angeles lakers",
                "boston celtics",
                "golden state warriors",
                "miami heat",
                "denver nuggets",
                "milwaukee bucks",
            },
            "mlb": {
                "new york yankees",
                "los angeles dodgers",
                "houston astros",
                "atlanta braves",
                "tampa bay rays",
                "boston red sox",
            },
        }

    def compute_realness(self, data: Dict[str, Any]) -> float:
        """
        Compute realness score for incoming data

        Returns:
            float: Realness score from 0.0 (fake) to 1.0 (real)
        """
        score = 1.0

        # Extract events from data structure
        events = self._extract_events(data)

        if not events:
            logger.warning("No events found in data")
            return 0.0

        # Test 1: Minimum event count (real feeds have many events)
        if len(events) < 10:
            score *= 0.5
            self.metrics["low_event_count"] += 1

        # Test 2: League diversity (real feeds have multiple sports/leagues)
        leagues = set()
        sports = set()
        for event in events:
            if "league" in event:
                leagues.add(event["league"])
            if "sport" in event:
                sports.add(event["sport"])

        if len(leagues) < 2:
            score *= 0.7
            self.metrics["low_league_diversity"] += 1

        if len(sports) < 1:
            score *= 0.5
            self.metrics["no_sports"] += 1

        # Test 3: Team name entropy (generated data often reuses same teams)
        team_counts = defaultdict(int)
        for event in events:
            if "home_team" in event:
                team_counts[event["home_team"].lower()] += 1
            if "away_team" in event:
                team_counts[event["away_team"].lower()] += 1

        if team_counts:
            entropy = self._calculate_entropy(list(team_counts.values()))
            if entropy < 2.0:  # Low entropy indicates repetitive teams
                score *= 0.6
                self.metrics["low_team_entropy"] += 1

        # Test 4: Check for test patterns in team names
        for team in team_counts.keys():
            for pattern in self.test_patterns:
                if re.search(pattern, team, re.IGNORECASE):
                    score *= 0.1
                    self.metrics["test_pattern_detected"] += 1
                    logger.warning(f"Test pattern '{pattern}' found in team: {team}")
                    break

        # Test 5: Validate known real teams
        real_team_matches = 0
        for event in events[:20]:  # Check first 20 events
            home = event.get("home_team", "").lower()
            away = event.get("away_team", "").lower()
            sport = event.get("sport", "").lower()

            if sport in self.real_teams:
                if home in self.real_teams[sport]:
                    real_team_matches += 1
                if away in self.real_teams[sport]:
                    real_team_matches += 1

        if real_team_matches < 5 and len(events) > 10:
            score *= 0.7
            self.metrics["few_real_teams"] += 1

        # Test 6: Price validation (check for realistic odds)
        prices_valid = self._validate_prices(events)
        if not prices_valid:
            score *= 0.8
            self.metrics["invalid_prices"] += 1

        # Test 7: Timestamp validation
        timestamps_valid = self._validate_timestamps(events)
        if not timestamps_valid:
            score *= 0.9
            self.metrics["invalid_timestamps"] += 1

        # Test 8: Event ID patterns (check for sequential/predictable IDs)
        ids_suspicious = self._check_event_ids(events)
        if ids_suspicious:
            score *= 0.7
            self.metrics["suspicious_ids"] += 1

        return max(0.0, min(1.0, score))

    def _extract_events(self, data: Dict[str, Any]) -> List[Dict]:
        """Extract events from various data structures"""
        events = []

        # Handle direct events array
        if "events" in data:
            events = data["events"]
        # Handle odds format
        elif "odds" in data:
            events = data["odds"]
        # Handle single event
        elif "event_id" in data or "id" in data:
            events = [data]
        # Handle message wrapper
        elif "message" in data:
            return self._extract_events(data["message"])
        # Handle data wrapper
        elif "data" in data:
            return self._extract_events(data["data"])

        return events if isinstance(events, list) else []

    def _calculate_entropy(self, counts: List[int]) -> float:
        """Calculate Shannon entropy for distribution"""
        total = sum(counts)
        if total == 0:
            return 0

        entropy = 0
        for count in counts:
            if count > 0:
                p = count / total
                entropy -= p * math.log2(p)

        return entropy

    def _validate_prices(self, events: List[Dict]) -> bool:
        """Validate that prices look realistic"""
        prices_found = []

        for event in events:
            markets = event.get("markets", [])
            for market in markets:
                selections = market.get("selections", [])
                for selection in selections:
                    price = selection.get("price")
                    if price:
                        prices_found.append(price)

        if not prices_found:
            return True  # No prices to validate

        # Check for variety in prices (not all -110)
        unique_prices = set(prices_found)
        if len(unique_prices) < 3 and len(prices_found) > 10:
            return False

        # Check for realistic range
        for price in prices_found:
            if isinstance(price, (int, float)):
                if price > 0 and (price < 1.01 or price > 100):  # Decimal odds
                    return False
                elif price < 0 and (price > -100 or price < -10000):  # American odds
                    return False
                elif price > 100 and price > 10000:  # American positive odds
                    return False

        return True

    def _validate_timestamps(self, events: List[Dict]) -> bool:
        """Validate that timestamps look realistic"""
        now = datetime.now(timezone.utc)
        future_limit = now + timedelta(
            days=365
        )  # Events shouldn't be more than a year out
        past_limit = now - timedelta(days=7)  # Old events are suspicious

        for event in events:
            start_time = event.get("start_time")
            if start_time:
                try:
                    if isinstance(start_time, str):
                        dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                        if dt < past_limit or dt > future_limit:
                            return False
                except:
                    pass

        return True

    def _check_event_ids(self, events: List[Dict]) -> bool:
        """Check if event IDs look suspicious (sequential, predictable)"""
        ids = []
        for event in events:
            event_id = event.get("event_id") or event.get("id")
            if event_id:
                ids.append(str(event_id))

        if len(ids) < 2:
            return False

        # Check for sequential numeric IDs
        try:
            numeric_ids = [int(id) for id in ids if id.isdigit()]
            if len(numeric_ids) > 5:
                diffs = [
                    numeric_ids[i + 1] - numeric_ids[i]
                    for i in range(len(numeric_ids) - 1)
                ]
                if all(d == 1 for d in diffs):
                    return True  # Suspicious: perfectly sequential
        except:
            pass

        # Check for timestamp-based IDs (common in generators)
        timestamp_pattern = re.compile(r"^\d{10,13}")
        timestamp_matches = sum(1 for id in ids if timestamp_pattern.match(id))
        if timestamp_matches > len(ids) * 0.8:
            return True  # Suspicious: mostly timestamp IDs

        return False

    def validate(self, data: Dict[str, Any], source: str = "unknown") -> bool:
        """
        Validate if data is real or fake

        Args:
            data: Incoming data to validate
            source: Source identifier (book name)

        Returns:
            bool: True if data passes realness check, False otherwise
        """
        try:
            score = self.compute_realness(data)

            # Log metrics
            self.metrics["total_validations"] += 1
            self.metrics[f"source_{source}"] += 1

            if score < 0.5:
                self.metrics["blocked_fake"] += 1
                logger.warning(f"Blocked fake data from {source}: score={score:.2f}")
                return False
            elif score < 0.8 and self.strict_mode:
                self.metrics["blocked_suspicious"] += 1
                logger.warning(
                    f"Blocked suspicious data from {source} (strict mode): score={score:.2f}"
                )
                return False
            else:
                self.metrics["passed_real"] += 1
                logger.debug(f"Passed real data from {source}: score={score:.2f}")
                return True

        except Exception as e:
            logger.error(f"Error validating data from {source}: {e}")
            if self.strict_mode:
                self.metrics["blocked_error"] += 1
                return False
            else:
                self.metrics["passed_error"] += 1
                return True

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics"""
        uptime = (datetime.now(timezone.utc) - self.last_reset).total_seconds()

        return {
            "uptime_seconds": uptime,
            "metrics": dict(self.metrics),
            "mode": "strict" if self.strict_mode else "permissive",
            "last_reset": self.last_reset.isoformat(),
        }

    def allow_write(self, realness: float, min_thresh: float = 0.9) -> bool:
        """
        Check if write should be allowed based on realness score

        Args:
            realness: Computed realness score (0.0-1.0)
            min_thresh: Minimum threshold for allowing writes

        Returns:
            bool: True if write allowed, False otherwise
        """
        return realness >= min_thresh

    def reset_metrics(self):
        """Reset metrics counters"""
        self.metrics.clear()
        self.last_reset = datetime.now(timezone.utc)
        logger.info("Metrics reset")


def create_gate(strict: bool = True) -> RealnessGate:
    """Factory function to create realness gate"""
    return RealnessGate(strict_mode=strict)


if __name__ == "__main__":
    # Test the realness gate
    logging.basicConfig(level=logging.INFO)

    gate = create_gate(strict=True)

    # Test with fake data
    # Test data for development (not used in production)
    test_example = {
        "events": [
            {
                "event_id": "12345",
                "home_team": "Team A",
                "away_team": "Team B",
                "sport": "NFL",
                "markets": [
                    {
                        "type": "moneyline",
                        "selections": [
                            {"name": "Team A", "price": -110},
                            {"name": "Team B", "price": -110},
                        ],
                    }
                ],
            }
        ]
        * 5  # Repeated same event
    }

    # Test with more realistic data
    real_data = {
        "events": [
            {
                "event_id": "dk_2024_nfl_kc_buf",
                "home_team": "Kansas City Chiefs",
                "away_team": "Buffalo Bills",
                "sport": "NFL",
                "league": "NFL",
                "start_time": (
                    datetime.now(timezone.utc) + timedelta(days=2)
                ).isoformat(),
                "markets": [
                    {
                        "type": "moneyline",
                        "selections": [
                            {"name": "Kansas City Chiefs", "price": -145},
                            {"name": "Buffalo Bills", "price": 125},
                        ],
                    },
                    {
                        "type": "spread",
                        "selections": [
                            {"name": "Kansas City Chiefs", "price": -110, "line": -3.5},
                            {"name": "Buffalo Bills", "price": -110, "line": 3.5},
                        ],
                    },
                ],
            },
            {
                "event_id": "dk_2024_nba_lal_bos",
                "home_team": "Los Angeles Lakers",
                "away_team": "Boston Celtics",
                "sport": "NBA",
                "league": "NBA",
                "start_time": (
                    datetime.now(timezone.utc) + timedelta(days=1)
                ).isoformat(),
                "markets": [
                    {
                        "type": "moneyline",
                        "selections": [
                            {"name": "Los Angeles Lakers", "price": 110},
                            {"name": "Boston Celtics", "price": -130},
                        ],
                    }
                ],
            },
        ]
        + [
            {
                "event_id": f"dk_2024_mlb_{i}",
                "home_team": "New York Yankees",
                "away_team": "Boston Red Sox",
                "sport": "MLB",
                "league": "MLB",
                "markets": [],
            }
            for i in range(10)
        ]
    }

    print("Testing example data:")
    result = gate.validate(test_example, "test_example")
    print(f"Result: {'BLOCKED' if not result else 'PASSED'}")
    print(f"Score: {gate.compute_realness(test_example):.2f}")

    print("\nTesting real data:")
    result = gate.validate(real_data, "test_real")
    print(f"Result: {'BLOCKED' if not result else 'PASSED'}")
    print(f"Score: {gate.compute_realness(real_data):.2f}")

    print("\nMetrics:")
    print(json.dumps(gate.get_metrics(), indent=2))
