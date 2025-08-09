import os
import json
import asyncio
import logging
import random
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import httpx
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "service": "collector-agg", "level": "%(levelname)s", "message": "%(message)s"}',
)

FETCH_TOTAL = Counter(
    "collector_fetch_total", "Total fetch attempts", ["source", "status"]
)
FETCH_DURATION = Histogram(
    "collector_fetch_duration_seconds", "Fetch duration", ["source"]
)
ODDS_PUBLISHED = Counter("odds_published_total", "Total odds published", ["source"])
BUDGET_SKIPS = Counter(
    "collector_budget_skips_total", "Budget cap skipped fetches", ["source"]
)
BUDGET_REMAINING = Gauge(
    "collector_budget_remaining", "Daily budget remaining", ["source"]
)
DEDUP_DROPS = Counter(
    "collector_dedup_drops_total", "Deduplicated dropped responses", ["source"]
)


class AggregatorCollector:
    def __init__(self):
        self.redis_client = redis.from_url(
            os.getenv("REDIS_URL", "redis://localhost:6379/0")
        )

        # Configuration
        self.base_url = os.getenv("AGG_BASE_URL", "https://api.the-odds-api.com/v4")
        self.api_key = os.getenv("AGG_API_KEY", "")
        self.sport = os.getenv("AGG_SPORT", "americanfootball_nfl")
        self.markets = os.getenv("AGG_MARKETS", "h2h,spreads,totals")
        self.region = os.getenv("AGG_REGION", "us")
        self.use_mock = os.getenv("AGG_USE_MOCK", "true").lower() == "true"
        self.fetch_interval = int(os.getenv("AGG_FETCH_INTERVAL", "120"))
        self.daily_budget = int(os.getenv("AGG_DAILY_REQUEST_BUDGET", "500"))

        self.running = False

        # Exponential backoff state
        self.backoff_delay = 1.0
        self.max_backoff = 60.0

        # Mock data generation
        self.mock_teams = [
            ("Kansas City Chiefs", "Buffalo Bills"),
            ("Tampa Bay Buccaneers", "Green Bay Packers"),
            ("San Francisco 49ers", "Dallas Cowboys"),
            ("Baltimore Ravens", "Cincinnati Bengals"),
            ("Miami Dolphins", "New York Jets"),
            ("Philadelphia Eagles", "Washington Commanders"),
            ("Minnesota Vikings", "Detroit Lions"),
            ("Pittsburgh Steelers", "Cleveland Browns"),
            ("Los Angeles Chargers", "Denver Broncos"),
            ("Las Vegas Raiders", "Los Angeles Rams"),
        ]

    async def generate_mock_odds(self) -> List[Dict[str, Any]]:
        """Generate mock odds data for testing"""
        events = []
        now = datetime.utcnow()

        # Generate 6-12 upcoming events
        num_events = random.randint(6, 12)
        for i in range(num_events):
            home, away = random.choice(self.mock_teams)
            start_time = now + timedelta(
                hours=random.randint(1, 168)
            )  # 1-168 hours from now

            event = {
                "id": f"mock_event_{i}_{int(now.timestamp())}",
                "sport_key": self.sport,
                "sport_title": "NFL",
                "commence_time": start_time.isoformat() + "Z",
                "home_team": home,
                "away_team": away,
                "bookmakers": [],
            }

            # Add some bookmakers with odds
            bookmakers = ["fanduel", "draftkings", "pointsbet", "betmgm"]
            for book in random.sample(bookmakers, random.randint(2, 4)):
                bookmaker_data = {"key": book, "title": book.title(), "markets": []}

                # Add moneyline (h2h)
                if "h2h" in self.markets:
                    home_odds = random.randint(-200, 200)
                    away_odds = (
                        -home_odds if home_odds > 0 else random.randint(-200, 200)
                    )
                    bookmaker_data["markets"].append(
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": home, "price": home_odds},
                                {"name": away, "price": away_odds},
                            ],
                        }
                    )

                # Add spreads
                if "spreads" in self.markets:
                    spread = round(random.uniform(-10, 10), 1)
                    bookmaker_data["markets"].append(
                        {
                            "key": "spreads",
                            "outcomes": [
                                {
                                    "name": home,
                                    "price": random.randint(-115, -105),
                                    "point": -spread,
                                },
                                {
                                    "name": away,
                                    "price": random.randint(-115, -105),
                                    "point": spread,
                                },
                            ],
                        }
                    )

                # Add totals
                if "totals" in self.markets:
                    total = round(random.uniform(40, 55), 1)
                    bookmaker_data["markets"].append(
                        {
                            "key": "totals",
                            "outcomes": [
                                {
                                    "name": "Over",
                                    "price": random.randint(-115, -105),
                                    "point": total,
                                },
                                {
                                    "name": "Under",
                                    "price": random.randint(-115, -105),
                                    "point": total,
                                },
                            ],
                        }
                    )

                event["bookmakers"].append(bookmaker_data)

            events.append(event)

        return events

    def _sanitize_url_for_logging(self, url: str, params: dict) -> str:
        """Sanitize URL and params for logging by redacting API key"""
        sanitized_params = {
            k: ("***REDACTED***" if k.lower() in ["apikey", "api_key"] else v)
            for k, v in params.items()
        }
        return f"{url}?{httpx.QueryParams(sanitized_params)}"

    def _get_redis_key(self, key_type: str, suffix: str = "") -> str:
        """Generate Redis keys for tracking"""
        if key_type == "daily_requests":
            today = datetime.utcnow().strftime("%Y%m%d")
            return f"agg:reqs:{today}"
        elif key_type == "etag":
            return f"agg:etag:{suffix}"
        elif key_type == "last_hash":
            return f"agg:hash:{suffix}"
        return f"agg:{key_type}:{suffix}"

    async def _check_daily_budget(self) -> bool:
        """Check if we're within daily request budget"""
        daily_key = self._get_redis_key("daily_requests")
        try:
            current_count = await self.redis_client.get(daily_key)
            current_count = int(current_count) if current_count else 0

            remaining = max(0, self.daily_budget - current_count)
            BUDGET_REMAINING.labels(source="agg").set(remaining)

            if current_count >= self.daily_budget:
                logger.warning(
                    f"Daily API budget reached ({current_count}/{self.daily_budget})"
                )
                BUDGET_SKIPS.labels(source="agg").inc()
                return False
            return True
        except Exception as e:
            logger.error(f"Failed to check daily budget: {e}")
            return True  # Allow request if Redis check fails

    async def _increment_daily_count(self):
        """Increment daily request counter with expiry"""
        daily_key = self._get_redis_key("daily_requests")
        try:
            # Increment counter
            new_count = await self.redis_client.incr(daily_key)

            # Set expiry to end of day if this is the first request
            if new_count == 1:
                # Calculate seconds until end of day
                now = datetime.utcnow()
                end_of_day = datetime.utcnow().replace(
                    hour=23, minute=59, second=59, microsecond=999999
                )
                ttl_seconds = int((end_of_day - now).total_seconds()) + 1
                await self.redis_client.expire(daily_key, ttl_seconds)

            # Update gauge
            remaining = max(0, self.daily_budget - new_count)
            BUDGET_REMAINING.labels(source="agg").set(remaining)

        except Exception as e:
            logger.error(f"Failed to increment daily count: {e}")

    def _get_content_hash(self, content: str) -> str:
        """Get SHA-256 hash of content for deduplication"""
        return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

    async def _should_skip_duplicate(self, content: str, cache_key: str) -> bool:
        """Check if content is duplicate of last response"""
        try:
            content_hash = self._get_content_hash(content)
            hash_key = self._get_redis_key("last_hash", cache_key)

            last_hash = await self.redis_client.get(hash_key)

            if last_hash and last_hash.decode() == content_hash:
                DEDUP_DROPS.labels(source="agg").inc()
                logger.info(f"Skipping duplicate content (hash: {content_hash})")
                return True

            # Store new hash with 1 hour expiry
            await self.redis_client.setex(hash_key, 3600, content_hash)
            return False

        except Exception as e:
            logger.error(f"Failed to check content deduplication: {e}")
            return False  # Process if dedup check fails

    async def fetch_real_odds(self) -> Optional[List[Dict[str, Any]]]:
        """Fetch real odds from the API with budget and conditional request support"""
        if not self.api_key:
            raise ValueError("AGG_API_KEY is required for real mode")

        # Check daily budget
        if not await self._check_daily_budget():
            return None

        url = f"{self.base_url}/sports/{self.sport}/odds"
        params = {
            "apiKey": self.api_key,
            "markets": self.markets,
            "regions": self.region,
            "oddsFormat": "american",
        }

        # Prepare conditional request headers
        headers = {}
        etag_key = self._get_redis_key(
            "etag", f"{self.sport}_{self.markets}_{self.region}"
        )

        try:
            stored_etag = await self.redis_client.get(etag_key)
            if stored_etag:
                headers["If-None-Match"] = stored_etag.decode()
        except Exception as e:
            logger.warning(f"Failed to retrieve stored ETag: {e}")

        # Log sanitized URL
        sanitized_url = self._sanitize_url_for_logging(url, params)
        logger.info(f"Fetching odds from: {sanitized_url}")

        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.get(url, params=params, headers=headers)

                # Increment counter after successful request
                await self._increment_daily_count()

                # Handle 304 Not Modified
                if response.status_code == 304:
                    logger.info("Received 304 Not Modified - no new data")
                    FETCH_TOTAL.labels(source="agg", status="not_modified").inc()
                    return None

                response.raise_for_status()

                # Store ETag for future requests
                if "etag" in response.headers:
                    try:
                        await self.redis_client.setex(
                            etag_key, 3600, response.headers["etag"]
                        )  # 1 hour expiry
                    except Exception as e:
                        logger.warning(f"Failed to store ETag: {e}")

                # Check for duplicate content
                response_text = response.text
                cache_key = f"{self.sport}_{self.markets}_{self.region}"

                if await self._should_skip_duplicate(response_text, cache_key):
                    return None

                return response.json()

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 304:
                    # Handle 304 that wasn't caught above
                    FETCH_TOTAL.labels(source="agg", status="not_modified").inc()
                    return None
                else:
                    raise
            except Exception as e:
                logger.error(f"HTTP request failed: {e}")
                raise

    async def normalize_and_publish(self, odds_data: List[Dict[str, Any]]):
        """Normalize odds data and publish to Redis"""
        normalized_events = []

        for event_data in odds_data:
            try:
                # Normalize to our internal schema
                normalized_event = {
                    "event_id": event_data["id"],
                    "sport": self.sport,
                    "league": event_data.get("sport_title", "NFL"),
                    "home_team": event_data["home_team"],
                    "away_team": event_data["away_team"],
                    "start_time": event_data["commence_time"],
                    "markets": [],
                }

                for bookmaker in event_data.get("bookmakers", []):
                    # Use "oddsapi" prefix for real API data to distinguish from mock
                    book_name = (
                        bookmaker["key"]
                        if self.use_mock
                        else f"oddsapi_{bookmaker['key']}"
                    )

                    for market in bookmaker.get("markets", []):
                        market_type = market["key"]

                        normalized_market = {
                            "book": book_name,
                            "market_type": market_type,
                            "outcomes": [],
                        }

                        for outcome in market.get("outcomes", []):
                            normalized_outcome = {
                                "name": outcome["name"],
                                "price": outcome["price"],
                                "point": outcome.get("point"),
                                "timestamp": datetime.utcnow().isoformat() + "Z",
                            }
                            normalized_market["outcomes"].append(normalized_outcome)

                        normalized_event["markets"].append(normalized_market)

                normalized_events.append(normalized_event)

            except Exception as e:
                logger.error(
                    f"Error normalizing event {event_data.get('id', 'unknown')}: {e}"
                )
                continue

        # Publish to Redis
        if normalized_events:
            message = {
                "source": "aggregator",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "events": normalized_events,
            }

            await self.redis_client.publish("odds.raw.agg", json.dumps(message))
            ODDS_PUBLISHED.labels(source="agg").inc(len(normalized_events))
            logger.info(f"Published {len(normalized_events)} events to odds.raw.agg")

    def _reset_backoff(self):
        """Reset backoff delay after successful request"""
        self.backoff_delay = 1.0

    def _increase_backoff(self):
        """Increase backoff delay exponentially"""
        self.backoff_delay = min(self.backoff_delay * 2, self.max_backoff)
        logger.info(f"Increased backoff delay to {self.backoff_delay}s")

    async def fetch_cycle(self):
        """Single fetch cycle with improved error handling"""
        with FETCH_DURATION.labels(source="agg").time():
            try:
                if self.use_mock:
                    logger.info("Fetching mock odds data")
                    odds_data = await self.generate_mock_odds()
                    FETCH_TOTAL.labels(source="agg", status="ok").inc()
                    await self.normalize_and_publish(odds_data)
                    self._reset_backoff()
                else:
                    logger.info("Fetching real odds data")
                    odds_data = await self.fetch_real_odds()

                    if odds_data is None:
                        # Budget exceeded, 304 response, or duplicate content
                        logger.debug("No data to process (budget/304/duplicate)")
                        return

                    FETCH_TOTAL.labels(source="agg", status="ok").inc()
                    await self.normalize_and_publish(odds_data)
                    self._reset_backoff()

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 304:
                    FETCH_TOTAL.labels(source="agg", status="not_modified").inc()
                    logger.info("Received 304 Not Modified")
                    self._reset_backoff()
                else:
                    logger.error(f"HTTP error {e.response.status_code}: {e}")
                    FETCH_TOTAL.labels(source="agg", status="error").inc()
                    self._increase_backoff()

            except Exception as e:
                logger.error(f"Fetch cycle failed: {e}")
                FETCH_TOTAL.labels(source="agg", status="error").inc()
                self._increase_backoff()

    async def run(self):
        # Redact API key in logs
        api_key_status = (
            "not_set" if not self.api_key else f"set ({len(self.api_key)} chars)"
        )

        logger.info(
            {
                "service": "collector-agg",
                "version": "0.1.0",
                "mode": "mock" if self.use_mock else "real",
                "sport": self.sport,
                "markets": self.markets,
                "region": self.region,
                "fetch_interval": self.fetch_interval,
                "daily_budget": self.daily_budget,
                "api_key": api_key_status,
                "status": "starting",
            }
        )

        # Start metrics server
        start_http_server(9106)

        self.running = True

        try:
            while self.running:
                await self.fetch_cycle()

                # Use configurable interval, or backoff delay if in error state
                sleep_time = max(self.fetch_interval, self.backoff_delay)
                logger.debug(f"Sleeping for {sleep_time}s")
                await asyncio.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.running = False
            await self.redis_client.close()


async def main():
    collector = AggregatorCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
