#!/usr/bin/env python3
"""
Data Quality Pipeline
Validates, cleans, and monitors odds data quality
"""

import json
import logging
import time
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
import hashlib

import redis
from prometheus_client import Counter, Histogram, Gauge

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
odds_validated = Counter(
    "odds_validated_total", "Total odds validated", ["book", "result"]
)
odds_invalid = Counter("odds_invalid_total", "Total invalid odds", ["book", "reason"])
odds_duplicates = Counter("odds_duplicates_total", "Total duplicate odds", ["book"])
odds_anomalies = Counter(
    "odds_anomalies_total", "Total anomalous odds", ["book", "type"]
)
processing_time = Histogram("odds_quality_check_seconds", "Time to quality check odds")
data_quality_score = Gauge("data_quality_score", "Overall data quality score", ["book"])


class DataQualityPipeline:
    """
    Comprehensive data quality pipeline for odds validation
    """

    def __init__(self, redis_host="localhost", redis_port=6379):
        self.redis_client = redis.Redis(
            host=redis_host, port=redis_port, decode_responses=True
        )

        # Deduplication cache (TTL 5 minutes)
        self.seen_hashes = {}
        self.hash_ttl = 300  # 5 minutes

        # Anomaly detection thresholds
        self.price_thresholds = {
            "min_decimal": 1.01,
            "max_decimal": 100.0,
            "min_american": -10000,
            "max_american": 10000,
            "max_price_change": 0.5,  # 50% max change between updates
            "min_update_interval": 1,  # Minimum seconds between updates
        }

        # Historical data for anomaly detection
        self.price_history = defaultdict(list)
        self.last_update_time = {}

        # Quality scores by book
        self.quality_scores = defaultdict(lambda: {"valid": 0, "invalid": 0})

        # Validation rules
        self.required_fields = ["book", "event_id", "timestamp"]

        self.required_odds_fields = ["market_type", "odds"]

    def validate(self, odds_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Validate odds data against quality rules

        Returns:
            (is_valid, error_reason)
        """
        book = odds_data.get("book", "unknown")

        with processing_time.time():
            # 1. Check required fields
            is_valid, reason = self.check_required_fields(odds_data)
            if not is_valid:
                odds_invalid.labels(book=book, reason="missing_fields").inc()
                return False, reason

            # 2. Validate data types
            is_valid, reason = self.validate_data_types(odds_data)
            if not is_valid:
                odds_invalid.labels(book=book, reason="invalid_types").inc()
                return False, reason

            # 3. Check for duplicates
            if self.is_duplicate(odds_data):
                odds_duplicates.labels(book=book).inc()
                return False, "Duplicate odds"

            # 4. Validate price ranges
            is_valid, reason = self.validate_prices(odds_data)
            if not is_valid:
                odds_invalid.labels(book=book, reason="invalid_price").inc()
                return False, reason

            # 5. Check for anomalies
            anomalies = self.detect_anomalies(odds_data)
            if anomalies:
                for anomaly_type in anomalies:
                    odds_anomalies.labels(book=book, type=anomaly_type).inc()
                # Log but don't reject anomalies (they might be legitimate)
                logger.warning(f"Anomalies detected for {book}: {anomalies}")

            # 6. Validate timestamps
            is_valid, reason = self.validate_timestamp(odds_data)
            if not is_valid:
                odds_invalid.labels(book=book, reason="invalid_timestamp").inc()
                return False, reason

            # 7. Check market consistency
            is_valid, reason = self.validate_market_consistency(odds_data)
            if not is_valid:
                odds_invalid.labels(book=book, reason="inconsistent_market").inc()
                return False, reason

            # Update quality score
            self.update_quality_score(book, True)
            odds_validated.labels(book=book, result="valid").inc()

            return True, None

    def check_required_fields(self, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Check if all required fields are present"""
        missing_fields = []

        for field in self.required_fields:
            if field not in data or data[field] is None:
                missing_fields.append(field)

        # Check odds-specific fields
        if "odds" in data and isinstance(data["odds"], list):
            for odds_entry in data["odds"]:
                for field in ["price", "decimal_price"]:
                    if field not in odds_entry:
                        missing_fields.append(f"odds.{field}")

        if missing_fields:
            return False, f"Missing required fields: {', '.join(missing_fields)}"

        return True, None

    def validate_data_types(self, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate data types of fields"""
        try:
            # Check basic types
            if not isinstance(data.get("book"), str):
                return False, "book must be string"

            if not isinstance(data.get("event_id"), (str, int)):
                return False, "event_id must be string or int"

            if not isinstance(data.get("timestamp"), str):
                return False, "timestamp must be string"

            # Validate timestamp format
            try:
                datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            except:
                return False, "timestamp must be ISO format"

            # Check odds array
            if "odds" in data:
                if not isinstance(data["odds"], list):
                    return False, "odds must be array"

                for odds_entry in data["odds"]:
                    if not isinstance(odds_entry, dict):
                        return False, "odds entries must be objects"

            return True, None

        except Exception as e:
            return False, f"Type validation error: {str(e)}"

    def is_duplicate(self, data: Dict[str, Any]) -> bool:
        """Check if this is duplicate data"""
        # Create hash of key fields
        hash_input = (
            f"{data.get('book')}:{data.get('event_id')}:{data.get('market_type', '')}"
        )

        if "odds" in data:
            for odds_entry in data["odds"]:
                hash_input += f":{odds_entry.get('selection_id', '')}:{odds_entry.get('price', '')}"

        data_hash = hashlib.md5(hash_input.encode()).hexdigest()

        # Clean old hashes
        current_time = time.time()
        self.seen_hashes = {
            h: t
            for h, t in self.seen_hashes.items()
            if current_time - t < self.hash_ttl
        }

        # Check if duplicate
        if data_hash in self.seen_hashes:
            return True

        # Store hash
        self.seen_hashes[data_hash] = current_time
        return False

    def validate_prices(self, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate price values are within acceptable ranges"""
        if "odds" not in data:
            return True, None

        for odds_entry in data["odds"]:
            # Check decimal price
            decimal_price = odds_entry.get("decimal_price")
            if decimal_price is not None:
                try:
                    price_float = float(decimal_price)
                    if not (
                        self.price_thresholds["min_decimal"]
                        <= price_float
                        <= self.price_thresholds["max_decimal"]
                    ):
                        return False, f"Decimal price {price_float} out of range"
                except (ValueError, TypeError):
                    return False, f"Invalid decimal price format: {decimal_price}"

            # Check American price
            american_price = odds_entry.get("price")
            if american_price is not None:
                try:
                    price_int = int(american_price)
                    if not (
                        self.price_thresholds["min_american"]
                        <= price_int
                        <= self.price_thresholds["max_american"]
                    ):
                        return False, f"American price {price_int} out of range"

                    # Validate American odds rules
                    if price_int == 0 or (-100 < price_int < 100 and price_int != 0):
                        return False, f"Invalid American odds value: {price_int}"

                except (ValueError, TypeError):
                    return False, f"Invalid American price format: {american_price}"

            # Check handicap/spread values
            handicap = odds_entry.get("handicap")
            if handicap is not None:
                try:
                    handicap_float = float(handicap)
                    if abs(handicap_float) > 100:  # Reasonable spread limit
                        return False, f"Handicap {handicap_float} seems unrealistic"
                except (ValueError, TypeError):
                    return False, f"Invalid handicap format: {handicap}"

        return True, None

    def detect_anomalies(self, data: Dict[str, Any]) -> List[str]:
        """Detect anomalous patterns in odds data"""
        anomalies = []

        book = data.get("book")
        event_id = data.get("event_id")

        if not book or not event_id or "odds" not in data:
            return anomalies

        key = f"{book}:{event_id}"
        current_time = time.time()

        # Check update frequency
        if key in self.last_update_time:
            time_diff = current_time - self.last_update_time[key]
            if time_diff < self.price_thresholds["min_update_interval"]:
                anomalies.append("rapid_updates")

        self.last_update_time[key] = current_time

        # Check price movements
        for odds_entry in data["odds"]:
            selection_key = f"{key}:{odds_entry.get('selection_id', '')}"
            decimal_price = odds_entry.get("decimal_price")

            if decimal_price is None:
                continue

            try:
                price_float = float(decimal_price)

                # Check price history for large movements
                if selection_key in self.price_history:
                    history = self.price_history[selection_key]
                    if history:
                        last_price = history[-1]["price"]
                        price_change = abs(price_float - last_price) / last_price

                        if price_change > self.price_thresholds["max_price_change"]:
                            anomalies.append("large_price_movement")

                        # Check for oscillating prices
                        if len(history) >= 3:
                            recent_prices = [h["price"] for h in history[-3:]]
                            if (
                                price_float == recent_prices[0]
                                and price_float != recent_prices[1]
                            ):
                                anomalies.append("price_oscillation")

                # Store price in history
                self.price_history[selection_key].append(
                    {"price": price_float, "timestamp": current_time}
                )

                # Keep only recent history (last 10 updates)
                self.price_history[selection_key] = self.price_history[selection_key][
                    -10:
                ]

            except (ValueError, TypeError):
                anomalies.append("invalid_price_format")

        # Check for identical prices across all selections (suspicious)
        if "odds" in data and len(data["odds"]) > 2:
            prices = [
                o.get("decimal_price") for o in data["odds"] if o.get("decimal_price")
            ]
            if prices and len(set(prices)) == 1:
                anomalies.append("identical_prices")

        return anomalies

    def validate_timestamp(self, data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate timestamp is reasonable"""
        try:
            timestamp_str = data.get("timestamp")
            if not timestamp_str:
                return False, "Missing timestamp"

            # Parse timestamp
            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            now = datetime.utcnow()

            # Check if timestamp is in the future
            if timestamp > now + timedelta(minutes=1):
                return False, "Timestamp is in the future"

            # Check if timestamp is too old (more than 1 hour)
            if timestamp < now - timedelta(hours=1):
                return False, "Timestamp is too old"

            return True, None

        except Exception as e:
            return False, f"Invalid timestamp: {str(e)}"

    def validate_market_consistency(
        self, data: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate market data is internally consistent"""
        market_type = data.get("market_type")

        if not market_type or "odds" not in data:
            return True, None

        odds_entries = data["odds"]

        # Check h2h markets have exactly 2 outcomes (or 3 with draw)
        if market_type == "h2h":
            outcome_count = len(odds_entries)
            if outcome_count not in [2, 3]:  # 2 for no draw, 3 with draw
                return (
                    False,
                    f"h2h market should have 2-3 outcomes, found {outcome_count}",
                )

        # Check spread markets have matching handicaps
        elif market_type == "spread":
            handicaps = [
                o.get("handicap") for o in odds_entries if o.get("handicap") is not None
            ]
            if len(handicaps) == 2:
                try:
                    if abs(float(handicaps[0]) + float(handicaps[1])) > 0.01:
                        return False, "Spread handicaps don't sum to zero"
                except (ValueError, TypeError):
                    pass

        # Check total markets have over/under
        elif market_type == "total":
            types = [o.get("type") for o in odds_entries if o.get("type")]
            if types and not all(t in ["over", "under", "push"] for t in types):
                return False, f"Total market has invalid selection types: {types}"

        return True, None

    def clean_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and normalize data"""
        cleaned = data.copy()

        # Ensure consistent field names
        if "book" in cleaned:
            cleaned["book"] = cleaned["book"].lower().strip()

        # Normalize market type
        if "market_type" in cleaned:
            from normalizer.market_mapper import MarketMapper

            cleaned["market_type"] = MarketMapper.standardize(
                cleaned["market_type"], cleaned.get("book")
            )

        # Clean odds entries
        if "odds" in cleaned and isinstance(cleaned["odds"], list):
            for odds_entry in cleaned["odds"]:
                # Ensure decimal price exists
                if "price" in odds_entry and "decimal_price" not in odds_entry:
                    american = odds_entry["price"]
                    if american:
                        try:
                            american_int = int(american)
                            if american_int > 0:
                                odds_entry["decimal_price"] = round(
                                    (american_int / 100) + 1, 3
                                )
                            else:
                                odds_entry["decimal_price"] = round(
                                    (100 / abs(american_int)) + 1, 3
                                )
                        except (ValueError, TypeError):
                            pass

                # Clean selection names
                if "name" in odds_entry and odds_entry["name"]:
                    odds_entry["name"] = str(odds_entry["name"]).strip()

        return cleaned

    def update_quality_score(self, book: str, is_valid: bool):
        """Update running quality score for a book"""
        if is_valid:
            self.quality_scores[book]["valid"] += 1
        else:
            self.quality_scores[book]["invalid"] += 1

        # Calculate score (percentage of valid)
        total = (
            self.quality_scores[book]["valid"] + self.quality_scores[book]["invalid"]
        )
        if total > 0:
            score = (self.quality_scores[book]["valid"] / total) * 100
            data_quality_score.labels(book=book).set(score)

    def get_quality_report(self) -> Dict[str, Any]:
        """Generate quality report for all books"""
        report = {"timestamp": datetime.utcnow().isoformat(), "books": {}}

        for book, scores in self.quality_scores.items():
            total = scores["valid"] + scores["invalid"]
            if total > 0:
                quality_percentage = (scores["valid"] / total) * 100
                report["books"][book] = {
                    "valid": scores["valid"],
                    "invalid": scores["invalid"],
                    "total": total,
                    "quality_score": round(quality_percentage, 2),
                }

        # Overall statistics
        total_valid = sum(s["valid"] for s in self.quality_scores.values())
        total_invalid = sum(s["invalid"] for s in self.quality_scores.values())
        total_all = total_valid + total_invalid

        if total_all > 0:
            report["overall"] = {
                "valid": total_valid,
                "invalid": total_invalid,
                "total": total_all,
                "quality_score": round((total_valid / total_all) * 100, 2),
            }

        return report

    def process(self, odds_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Main processing pipeline

        Returns:
            Cleaned data if valid, None if invalid
        """
        # Validate
        is_valid, reason = self.validate(odds_data)

        if not is_valid:
            logger.warning(
                f"Invalid data from {odds_data.get('book', 'unknown')}: {reason}"
            )
            return None

        # Clean and return
        cleaned_data = self.clean_data(odds_data)
        return cleaned_data


# Example usage
if __name__ == "__main__":
    pipeline = DataQualityPipeline()

    # Test data
    test_odds = {
        "book": "draftkings",
        "event_id": "12345",
        "sport": "NFL",
        "home_team": "Patriots",
        "away_team": "Jets",
        "market_type": "h2h",
        "timestamp": datetime.utcnow().isoformat(),
        "odds": [
            {
                "selection_id": "1",
                "name": "Patriots",
                "type": "home",
                "price": -110,
                "decimal_price": 1.909,
            },
            {
                "selection_id": "2",
                "name": "Jets",
                "type": "away",
                "price": -110,
                "decimal_price": 1.909,
            },
        ],
    }

    # Process
    result = pipeline.process(test_odds)
    if result:
        print("Valid odds processed:", json.dumps(result, indent=2))
    else:
        print("Invalid odds rejected")

    # Get quality report
    print("\nQuality Report:")
    print(json.dumps(pipeline.get_quality_report(), indent=2))
