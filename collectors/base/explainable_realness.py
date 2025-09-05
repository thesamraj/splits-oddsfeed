#!/usr/bin/env python3
"""
Explainable RealnessGate with detailed metrics and quarantine support
"""
import os
import json
import logging
import yaml
from datetime import datetime, timezone
from collections import defaultdict, deque
from typing import Dict, List, Tuple
from prometheus_client import Counter, Gauge, Histogram
import psycopg2
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Feature metrics
REALNESS_FEAT_EVENT_DIVERSITY = Gauge(
    "realness_feat_event_diversity", "Event diversity score", ["book"]
)
REALNESS_FEAT_TEAM_ENTROPY = Gauge(
    "realness_feat_team_entropy", "Team entropy score", ["book"]
)
REALNESS_FEAT_PRICE_VARIANCE = Gauge(
    "realness_feat_price_variance", "Price variance score", ["book"]
)
REALNESS_FEAT_TIME_SPREAD = Gauge(
    "realness_feat_time_spread_minutes", "Time spread in minutes", ["book"]
)
REALNESS_FEAT_DUPLICATE_RATIO = Gauge(
    "realness_feat_duplicate_ratio", "Duplicate event ratio", ["book"]
)

# Outcome metrics
REALNESS_BLOCKS_TOTAL = Counter(
    "realness_blocks_total", "Total blocks by realness gate", ["book", "reason"]
)
REALNESS_PASSES_TOTAL = Counter(
    "realness_passes_total", "Total passes through realness gate", ["book", "warmup"]
)
QUARANTINE_COUNT_TOTAL = Counter(
    "quarantine_count_total", "Total events quarantined", ["book"]
)

# Baseline metrics
REALNESS_EXPECTED_UNIQUE_EVENTS = Gauge(
    "realness_expected_unique_events",
    "Expected unique events for hour",
    ["book", "hour"],
)
REALNESS_EVENT_DIVERSITY_RAW = Gauge(
    "realness_event_diversity_raw", "Raw event diversity count", ["book"]
)
REALNESS_TIME_BUCKETS_RAW = Gauge(
    "realness_time_buckets_raw", "Raw time bucket count", ["book"]
)

# Source observability
UPSTREAM_HTTP_STATUS = Counter(
    "upstream_http_status", "Upstream HTTP status codes", ["book", "status"]
)
UPSTREAM_PAYLOAD_BYTES = Histogram(
    "upstream_payload_bytes", "Upstream payload size", ["book"]
)
REQUEST_DURATION = Histogram("request_duration_seconds", "Request duration", ["book"])


class ExplainableRealnessGate:
    """Explainable realness scoring with warm-up and quarantine"""

    def __init__(self, book: str, db_url: str = None):
        self.book = book
        self.db_url = db_url or os.getenv("DATABASE_URL")

        # Load weights configuration
        self.config = self.load_weights_config()

        # Configuration
        self.threshold = float(os.getenv("REALNESS_THRESHOLD", "0.9"))
        self.hard_minimum_threshold = self.config.get("thresholds", {}).get(
            "hard_minimum", 0.85
        )
        self.min_samples = int(os.getenv("REALNESS_MIN_SAMPLES", "200"))
        self.min_window_min = int(os.getenv("REALNESS_MIN_WINDOW_MIN", "10"))
        self.quarantine = os.getenv("REALNESS_QUARANTINE", "true").lower() == "true"

        # Feature weights (from config)
        self.weights = self.config.get(
            "weights",
            {
                "event_diversity": 0.10,
                "team_entropy": 0.25,
                "price_variance": 0.30,
                "time_spread": 0.10,
                "duplicate_ratio": 0.25,
            },
        )

        # Hard signal configs
        hard_signal_list = self.config.get("hard_signals", [])
        self.hard_signals = {}
        for signal_dict in hard_signal_list:
            for k, v in signal_dict.items():
                self.hard_signals[k] = v

        # Expected baselines
        self.expected_hourly_events = self.config.get("expected_hourly_events", {})

        # State tracking
        self.samples_collected = 0
        self.first_sample_time = None
        self.is_warming_up = True
        self.recent_events = deque(maxlen=1000)
        self.last_report = {}
        self.last_raw_sample = None
        self.upstream_info = {}

        # Feature scores
        self.feature_scores = {
            "event_diversity": 0.0,
            "team_entropy": 0.0,
            "price_variance": 0.0,
            "time_spread": 0.0,
            "duplicate_ratio": 0.0,
        }

        # Setup quarantine table if DB available
        if self.db_url and self.quarantine:
            self._ensure_quarantine_table()

        logger.info(f"ExplainableRealnessGate initialized for {self.book}")
        logger.info(
            f"Threshold: {self.threshold}, Hard min: {self.hard_minimum_threshold}"
        )
        logger.info(f"Weights: {self.weights}")
        logger.info(f"Hard signals: {self.hard_signals}")

    def load_weights_config(self) -> dict:
        """Load weights configuration from YAML"""
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "realness", "weights.yml"
        )

        # Default config if file doesn't exist
        default_config = {
            "weights": {
                "event_diversity": 0.10,
                "team_entropy": 0.25,
                "price_variance": 0.30,
                "time_spread": 0.10,
                "duplicate_ratio": 0.25,
            },
            "thresholds": {"hard_minimum": 0.85, "standard": 0.90},
            "hard_signals": [
                {"price_variance": 0.8},
                {"team_entropy": 0.8},
                {"duplicate_ratio": 0.6},
            ],
        }

        try:
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    full_config = yaml.safe_load(f)

                    # Get book-specific config or default
                    if self.book in full_config:
                        book_config = full_config[self.book]
                        # Handle inheritance
                        if "inherit" in book_config:
                            parent = full_config.get(book_config["inherit"], {})
                            book_config = {**parent, **book_config}
                            del book_config["inherit"]
                        # Merge with defaults
                        config = {
                            **full_config.get("default", default_config),
                            **book_config,
                        }
                    else:
                        config = full_config.get("default", default_config)

                    return config
        except Exception as e:
            logger.warning(f"Could not load weights config: {e}, using defaults")

        return default_config

    def _ensure_quarantine_table(self):
        """Create quarantine_odds table if it doesn't exist"""
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS quarantine_odds (
                    id SERIAL PRIMARY KEY,
                    book VARCHAR(50) NOT NULL,
                    event_id VARCHAR(255) NOT NULL,
                    sport VARCHAR(50),
                    league VARCHAR(50),
                    home_team VARCHAR(255),
                    away_team VARCHAR(255),
                    market_type VARCHAR(50),
                    outcome_name VARCHAR(255),
                    outcome_price DECIMAL(10,2),
                    home_price DECIMAL(10,2),
                    away_price DECIMAL(10,2),
                    spread DECIMAL(10,2),
                    total DECIMAL(10,2),
                    start_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    realness_score DECIMAL(4,3),
                    failure_reasons TEXT
                )
            """
            )
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Quarantine table ready")
        except Exception as e:
            logger.error(f"Failed to create quarantine table: {e}")

    def check_warmup(self) -> bool:
        """Check if still in warm-up period"""
        if not self.is_warming_up:
            return False

        # Check sample threshold
        if self.samples_collected < self.min_samples:
            return True

        # Check time window
        if self.first_sample_time:
            elapsed = (
                datetime.now(timezone.utc) - self.first_sample_time
            ).total_seconds() / 60
            if elapsed < self.min_window_min:
                return True

        # Warm-up complete
        self.is_warming_up = False
        logger.info(
            f"{self.book}: Warm-up complete - {self.samples_collected} samples, {elapsed:.1f} minutes"
        )
        return False

    def get_expected_events_for_hour(self, hour: int) -> int:
        """Get expected unique events for current hour"""
        # Use config baseline if available
        if self.expected_hourly_events:
            return self.expected_hourly_events.get(hour, 100)

        # Reasonable defaults
        if 0 <= hour < 6:  # Late night/early morning
            return 80
        elif 6 <= hour < 12:  # Morning
            return 100
        elif 12 <= hour < 18:  # Afternoon
            return 150
        else:  # Evening/prime time
            return 200

    def compute_features(self, events: List[Dict]) -> Dict[str, float]:
        """Compute detailed feature scores with context-aware baselines"""
        features = {}
        current_hour = datetime.now(timezone.utc).hour

        # 1. Event diversity - context-aware based on expected events
        unique_matchups = set()
        for e in events:
            key = f"{e.get('sport','')}:{e.get('home_team','')}:{e.get('away_team','')}"
            unique_matchups.add(key)

        expected_events = self.get_expected_events_for_hour(current_hour)
        REALNESS_EXPECTED_UNIQUE_EVENTS.labels(
            book=self.book, hour=str(current_hour)
        ).set(expected_events)
        REALNESS_EVENT_DIVERSITY_RAW.labels(book=self.book).set(len(unique_matchups))

        # Scale diversity by expected baseline
        features["event_diversity"] = min(
            1.0, len(unique_matchups) / max(10, expected_events * 0.5)
        )

        # 2. Team entropy - distribution variety
        team_counts = defaultdict(int)
        for event in events:
            if event.get("home_team"):
                team_counts[event["home_team"].lower()] += 1
            if event.get("away_team"):
                team_counts[event["away_team"].lower()] += 1

        if len(team_counts) > 5:
            # Calculate entropy
            import math

            total = sum(team_counts.values())
            entropy = 0
            for count in team_counts.values():
                if count > 0:
                    p = count / total
                    entropy -= p * math.log2(p)
            features["team_entropy"] = min(1.0, entropy / 4.0)
        else:
            features["team_entropy"] = 0.3

        # 3. Price variance - realistic odds distribution
        prices = []
        for event in events[:200]:
            for key in ["home_price", "away_price", "outcome_price"]:
                if key in event and event[key]:
                    try:
                        price = float(event[key])
                        if -10000 < price < 10000:  # Sanity check
                            prices.append(price)
                    except:
                        pass

        if len(prices) > 10:
            unique_prices = len(set(prices))
            # Real books have varied prices
            features["price_variance"] = min(
                1.0, unique_prices / max(10, len(prices) / 3)
            )
        else:
            features["price_variance"] = 0.4

        # 4. Time spread - context-aware based on typical schedule patterns
        game_times = []
        hourly_buckets = defaultdict(int)
        for event in events[:200]:
            if "start_time" in event and event["start_time"]:
                try:
                    if isinstance(event["start_time"], str):
                        dt = datetime.fromisoformat(
                            event["start_time"].replace("Z", "+00:00")
                        )
                        game_times.append(dt)
                        # Bucket by day+hour
                        bucket = dt.strftime("%Y-%m-%d_%H")
                        hourly_buckets[bucket] += 1
                except:
                    pass

        REALNESS_TIME_BUCKETS_RAW.labels(book=self.book).set(len(hourly_buckets))

        if len(game_times) > 5:
            # Score based on both time range and distribution
            time_range_hours = (
                max(game_times) - min(game_times)
            ).total_seconds() / 3600
            num_buckets = len(hourly_buckets)

            # Context-aware scoring: expect fewer buckets during off-peak hours
            if current_hour in [2, 3, 4, 5]:  # Late night
                expected_buckets = 3
            elif current_hour in [18, 19, 20, 21]:  # Prime time
                expected_buckets = 8
            else:
                expected_buckets = 5

            range_score = min(1.0, time_range_hours / 48)  # 48h range = 1.0
            bucket_score = min(1.0, num_buckets / expected_buckets)
            features["time_spread"] = (range_score + bucket_score) / 2
        else:
            features["time_spread"] = 0.2

        # 5. Duplicate ratio - use actual dedup counters if available
        # Look at prometheus metrics for dedup effectiveness
        try:
            from prometheus_client import REGISTRY

            # Find dedup metrics
            published = 0
            dropped_market = 0
            dropped_snapshot = 0

            for collector in REGISTRY._collector_to_names:
                if hasattr(collector, "_name"):
                    if collector._name == "odds_upserts_total":
                        for sample in collector.collect():
                            for s in sample.samples:
                                if "bovada" in str(s.labels):
                                    published = s.value
                    elif collector._name == "dedup_dropped_total":
                        for sample in collector.collect():
                            for s in sample.samples:
                                labels = s.labels
                                if labels.get("book") == "bovada":
                                    if labels.get("reason") == "market_key":
                                        dropped_market = s.value
                                    elif labels.get("reason") == "snapshot":
                                        dropped_snapshot = s.value

            # Calculate ratio based on actual dedup performance
            total_seen = published + dropped_market + dropped_snapshot
            if total_seen > 0:
                features["duplicate_ratio"] = published / total_seen
            else:
                # Fallback to event analysis
                event_keys = []
                for e in events[:200]:
                    line_str = (
                        str(round(float(e.get("line", 0)), 1)) if e.get("line") else "0"
                    )
                    key = f"{e.get('event_id')}:{e.get('market_type')}:{e.get('selection')}:{line_str}"
                    event_keys.append(key)

                if event_keys:
                    unique = len(set(event_keys))
                    features["duplicate_ratio"] = unique / len(event_keys)
                else:
                    features["duplicate_ratio"] = 0.5
        except:
            # Fallback if metrics not available
            features["duplicate_ratio"] = 0.5

        return features

    def compute_realness(self, data: Dict) -> Tuple[float, List[str]]:
        """Compute realness with explanations"""
        events = data.get("events", [])
        if not events:
            return 0.0, ["No events in payload"]

        # Track sample
        self.samples_collected += 1
        if not self.first_sample_time:
            self.first_sample_time = datetime.now(timezone.utc)

        self.recent_events.extend(events[:100])
        self.last_raw_sample = data

        # Compute features
        features = self.compute_features(events)
        self.feature_scores = features

        # Update metrics
        REALNESS_FEAT_EVENT_DIVERSITY.labels(book=self.book).set(
            features["event_diversity"]
        )
        REALNESS_FEAT_TEAM_ENTROPY.labels(book=self.book).set(features["team_entropy"])
        REALNESS_FEAT_PRICE_VARIANCE.labels(book=self.book).set(
            features["price_variance"]
        )
        REALNESS_FEAT_TIME_SPREAD.labels(book=self.book).set(
            features.get("time_spread", 0) * 72
        )  # Hours
        REALNESS_FEAT_DUPLICATE_RATIO.labels(book=self.book).set(
            features["duplicate_ratio"]
        )

        # Weighted composite using configured weights
        composite = sum(
            features.get(k, 0) * self.weights.get(k, 0) for k in self.weights
        )

        # Check hard signals first
        hard_signal_failures = []
        for signal, threshold in self.hard_signals.items():
            if features.get(signal, 0) < threshold:
                hard_signal_failures.append(
                    f"{signal}={features.get(signal, 0):.2f} < {threshold}"
                )

        # Identify all issues for reporting
        failures = []
        for feat, score in features.items():
            if feat in self.hard_signals:
                # Hard signal check
                if score < self.hard_signals[feat]:
                    failures.append(f"HARD:{feat}={score:.2f}")
            elif score < 0.5:
                # Soft signal check
                failures.append(f"{feat}={score:.2f}")

        # Store report
        self.last_report = {
            "composite_score": composite,
            "features": features,
            "failure_reasons": failures,
            "event_count": len(events),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        return composite, failures

    def should_enforce(self, score: float) -> bool:
        """Check if we should enforce the gate with hard/soft signal logic"""
        warmup = self.check_warmup()

        if warmup:
            # During warm-up, pass but tag metrics
            REALNESS_PASSES_TOTAL.labels(book=self.book, warmup="true").inc()
            return False

        # Check hard signals first
        features = self.feature_scores
        hard_signals_pass = all(
            features.get(signal, 0) >= threshold
            for signal, threshold in self.hard_signals.items()
        )

        # Send Slack alert if hard signals fail
        if not hard_signals_pass and not warmup:
            failed_signals = []
            for signal, threshold in self.hard_signals.items():
                if features.get(signal, 0) < threshold:
                    failed_signals.append(
                        f"{signal}={features.get(signal, 0):.2f} < {threshold}"
                    )
            self.send_slack_alert(f"🚨 {self.book} hard signal failure", failed_signals)

        # New logic: If hard signals pass, use relaxed threshold
        if hard_signals_pass:
            threshold_to_use = self.hard_minimum_threshold
            if score >= threshold_to_use:
                logger.info(
                    f"Hard signals pass, accepting score {score:.2f} >= {threshold_to_use}"
                )
                REALNESS_PASSES_TOTAL.labels(book=self.book, warmup="false").inc()
                return False

        # Standard threshold check
        if score < self.threshold:
            main_reason = self.last_report.get("failure_reasons", ["unknown"])[0]
            if not hard_signals_pass:
                main_reason = "hard_signal_fail"
            REALNESS_BLOCKS_TOTAL.labels(book=self.book, reason=main_reason).inc()
            return True

        REALNESS_PASSES_TOTAL.labels(book=self.book, warmup="false").inc()
        return False

    def send_slack_alert(self, title: str, failures: List[str]):
        """Send Slack alert for hard signal failures"""
        webhook_url = os.getenv("SLACK_WEBHOOK")
        if not webhook_url:
            return

        try:
            import requests

            payload = {
                "text": title,
                "attachments": [
                    {
                        "color": "danger",
                        "fields": [
                            {
                                "title": "Failed Signals",
                                "value": "\n".join(failures),
                                "short": False,
                            },
                            {"title": "Book", "value": self.book, "short": True},
                            {
                                "title": "Timestamp",
                                "value": datetime.now(timezone.utc).isoformat(),
                                "short": True,
                            },
                        ],
                    }
                ],
            }
            requests.post(webhook_url, json=payload, timeout=5)
        except Exception as e:
            logger.warning(f"Failed to send Slack alert: {e}")

    def quarantine_events(self, events: List[Dict], score: float, reasons: List[str]):
        """Write sub-threshold events to quarantine table"""
        if not self.db_url or not self.quarantine:
            return

        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()

            for event in events[:100]:  # Limit to prevent huge inserts
                cur.execute(
                    """
                    INSERT INTO quarantine_odds
                    (book, event_id, sport, league, home_team, away_team,
                     market_type, home_price, away_price, spread, total,
                     start_time, realness_score, failure_reasons)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        self.book,
                        event.get("event_id", ""),
                        event.get("sport", ""),
                        event.get("league", ""),
                        event.get("home_team", ""),
                        event.get("away_team", ""),
                        event.get("market_type", ""),
                        event.get("home_price"),
                        event.get("away_price"),
                        event.get("spread"),
                        event.get("total"),
                        event.get("start_time"),
                        score,
                        json.dumps(reasons[:3]),
                    ),
                )

            conn.commit()
            cur.close()
            conn.close()

            QUARANTINE_COUNT_TOTAL.labels(book=self.book).inc(len(events))
            logger.info(f"Quarantined {len(events)} events with score {score:.2f}")
        except Exception as e:
            logger.error(f"Failed to quarantine: {e}")

    def record_upstream(
        self,
        url: str,
        status: int,
        content_type: str,
        payload_bytes: int,
        duration: float,
    ):
        """Record upstream metrics"""
        parsed = urlparse(url)
        self.upstream_info = {
            "host": parsed.netloc,
            "path": parsed.path,
            "status": status,
            "content_type": content_type,
            "bytes": payload_bytes,
        }

        UPSTREAM_HTTP_STATUS.labels(book=self.book, status=str(status)).inc()
        UPSTREAM_PAYLOAD_BYTES.labels(book=self.book).observe(payload_bytes)
        REQUEST_DURATION.labels(book=self.book).observe(duration)

    def get_report(self) -> Dict:
        """Get detailed realness report with actionable insights"""
        # Analyze recent events for patterns
        duplicate_keys = defaultdict(int)
        team_pairs = defaultdict(int)
        hourly_buckets = defaultdict(int)

        for e in list(self.recent_events)[-200:]:
            # Track duplicates
            line_str = str(round(float(e.get("line", 0)), 1)) if e.get("line") else "0"
            dup_key = f"{e.get('market_type')}:{e.get('selection')}:{line_str}:{e.get('price')}"
            duplicate_keys[dup_key] += 1

            # Track team pairs
            if e.get("home_team") and e.get("away_team"):
                pair = f"{e['home_team']} vs {e['away_team']}"
                team_pairs[pair] += 1

            # Track time distribution
            if e.get("start_time"):
                try:
                    dt = datetime.fromisoformat(e["start_time"].replace("Z", "+00:00"))
                    bucket = dt.strftime("%Y-%m-%d %H:00")
                    hourly_buckets[bucket] += 1
                except:
                    pass

        # Get top duplicates
        top_dupes = sorted(duplicate_keys.items(), key=lambda x: x[1], reverse=True)[:5]
        top_teams = sorted(team_pairs.items(), key=lambda x: x[1], reverse=True)[:5]

        return {
            "book": self.book,
            "composite_score": self.last_report.get("composite_score", 0.0),
            "feature_scores": self.feature_scores,
            "is_warming_up": self.is_warming_up,
            "samples_collected": self.samples_collected,
            "threshold": self.threshold,
            "warmup_progress": {
                "samples": f"{self.samples_collected}/{self.min_samples}",
                "minutes": (
                    f"{((datetime.now(timezone.utc) - self.first_sample_time).total_seconds() / 60):.1f}/{self.min_window_min}"
                    if self.first_sample_time
                    else "0/10"
                ),
            },
            "top_failure_reasons": self.last_report.get("failure_reasons", []),
            "upstream_info": self.upstream_info,
            "top_5_duplicate_keys": [f"{k} (count={v})" for k, v in top_dupes],
            "top_5_team_pairs": [f"{k} (count={v})" for k, v in top_teams],
            "time_distribution_buckets": len(hourly_buckets),
            "time_histogram_sample": dict(list(hourly_buckets.items())[:10]),
        }

    def get_sample(self) -> Dict:
        """Get sanitized recent sample"""
        if not self.last_raw_sample:
            return {"error": "No sample available yet"}

        events = self.last_raw_sample.get("events", [])
        return {
            "timestamp": self.last_raw_sample.get("timestamp"),
            "event_count": len(events),
            "sports": list(set(e.get("sport") for e in events[:20] if e.get("sport"))),
            "leagues": list(
                set(e.get("league") for e in events[:20] if e.get("league"))
            ),
            "sample_teams": list(
                set(e.get("home_team") for e in events[:10] if e.get("home_team"))
            )[:5],
            "market_types": list(
                set(e.get("market_type") for e in events[:20] if e.get("market_type"))
            ),
            "upstream": self.upstream_info,
        }
