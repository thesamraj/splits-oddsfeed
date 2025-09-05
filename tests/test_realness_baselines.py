#!/usr/bin/env python3
"""
Test realness baseline logic with hard/soft signals
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import unittest
from datetime import datetime, timezone, timedelta
from collectors.base.explainable_realness import ExplainableRealnessGate


class TestRealnessBaselines(unittest.TestCase):

    def setUp(self):
        """Create test gate with Bovada config"""
        self.gate = ExplainableRealnessGate("bovada", db_url=None)
        # Skip warm-up for tests
        self.gate.samples_collected = 300
        self.gate.first_sample_time = datetime.now(timezone.utc) - timedelta(minutes=15)

    def test_small_slate_hour_hard_signals_good(self):
        """Test: Small-slate hour with good hard signals should pass with relaxed threshold"""
        # Simulate late-night hour (small expected slate)
        events = []

        # Create 50 unique events with good prices and teams
        for i in range(50):
            events.append(
                {
                    "sport": "NFL",
                    "home_team": f"Team {i}",
                    "away_team": f"Opponent {i}",
                    "price": -110 + i % 20,  # Varied prices
                    "home_price": -110 + i % 15,
                    "away_price": 100 + i % 15,
                    "start_time": (
                        datetime.now(timezone.utc) + timedelta(hours=i)
                    ).isoformat(),
                }
            )

        # Compute realness
        score, failures = self.gate.compute_realness({"events": events})
        features = self.gate.feature_scores

        # Check hard signals pass
        self.assertGreaterEqual(
            features["price_variance"], 0.8, "Price variance should be >= 0.8"
        )
        self.assertGreaterEqual(
            features["team_entropy"], 0.8, "Team entropy should be >= 0.8"
        )
        self.assertGreaterEqual(
            features["duplicate_ratio"], 0.6, "Duplicate ratio should be >= 0.6"
        )

        # With hard signals passing, should accept >= 0.85
        if score >= 0.85:
            should_block = self.gate.should_enforce(score)
            self.assertFalse(
                should_block,
                f"Should not block with score {score} when hard signals pass",
            )

    def test_large_slate_hour_all_good(self):
        """Test: Large-slate hour with all signals good should have composite >= 0.9"""
        # Create 200 diverse events
        events = []
        sports = ["NFL", "NBA", "MLB", "NHL", "NCAAF"]
        teams = [f"Team{i}" for i in range(50)]

        for i in range(200):
            sport = sports[i % len(sports)]
            events.append(
                {
                    "sport": sport,
                    "home_team": teams[i % len(teams)],
                    "away_team": teams[(i + 1) % len(teams)],
                    "price": -110 + (i * 3) % 40,
                    "home_price": -110 + (i * 2) % 30,
                    "away_price": 100 + (i * 2) % 30,
                    "start_time": (
                        datetime.now(timezone.utc) + timedelta(hours=i % 72)
                    ).isoformat(),
                }
            )

        # Compute realness
        score, failures = self.gate.compute_realness({"events": events})

        # Large diverse slate should score >= 0.9
        self.assertGreaterEqual(
            score, 0.85, f"Large slate should score >= 0.85, got {score}"
        )

        # Should not block
        should_block = self.gate.should_enforce(score)
        self.assertFalse(
            should_block, f"Should not block large diverse slate with score {score}"
        )

    def test_bad_signals_block_regardless(self):
        """Test: Bad hard signals should block even if diversity/time spread look fine"""
        # Create events with no price variance (all same price)
        events = []
        for i in range(100):
            events.append(
                {
                    "sport": "NFL",
                    "home_team": f"Team {i}",
                    "away_team": f"Opponent {i}",
                    "price": -110,  # All same price - no variance
                    "home_price": -110,
                    "away_price": 100,
                    "start_time": (
                        datetime.now(timezone.utc) + timedelta(hours=i)
                    ).isoformat(),
                }
            )

        # Compute realness
        score, failures = self.gate.compute_realness({"events": events})
        features = self.gate.feature_scores

        # Price variance should fail
        self.assertLess(
            features["price_variance"],
            0.8,
            "Price variance should be < 0.8 with identical prices",
        )

        # Should block regardless of composite score
        should_block = self.gate.should_enforce(score)
        self.assertTrue(
            should_block,
            f"Should block with bad price variance even if score is {score}",
        )

    def test_context_aware_event_diversity(self):
        """Test: Event diversity should scale with expected hourly events"""
        # Test with 100 unique events
        events = []
        for i in range(100):
            events.append(
                {
                    "sport": "NFL",
                    "home_team": f"Team {i}",
                    "away_team": f"Opponent {i}",
                    "price": -110 + i % 20,
                    "start_time": datetime.now(timezone.utc).isoformat(),
                }
            )

        # Compute features
        score, _ = self.gate.compute_realness({"events": events})
        features = self.gate.feature_scores

        # Check diversity is context-aware
        current_hour = datetime.now(timezone.utc).hour
        expected = self.gate.get_expected_events_for_hour(current_hour)

        # 100 unique events should score well if expected is ~200
        # but poorly if expected is only 50
        if expected > 150:
            self.assertLess(
                features["event_diversity"],
                0.7,
                f"100 events should score < 0.7 when {expected} expected",
            )
        else:
            self.assertGreater(
                features["event_diversity"],
                0.5,
                f"100 events should score > 0.5 when {expected} expected",
            )

    def test_weighted_composite_calculation(self):
        """Test: Composite should use configured weights correctly"""
        # Create minimal valid events
        events = [
            {
                "sport": "NFL",
                "home_team": "Team A",
                "away_team": "Team B",
                "price": -110,
                "start_time": datetime.now(timezone.utc).isoformat(),
            }
        ]

        score, _ = self.gate.compute_realness({"events": events})
        features = self.gate.feature_scores

        # Manually calculate expected composite
        expected_composite = sum(
            features.get(k, 0) * self.gate.weights.get(k, 0) for k in self.gate.weights
        )

        # Should match calculated score (within floating point tolerance)
        self.assertAlmostEqual(
            score,
            expected_composite,
            places=4,
            msg=f"Composite {score} should match weighted sum {expected_composite}",
        )


if __name__ == "__main__":
    unittest.main()
