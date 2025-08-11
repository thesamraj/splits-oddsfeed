#!/usr/bin/env python3
"""
BetRivers (Kambi) odds collector.
Polls Kambi REST API endpoints for NFL events and markets.
"""

import os
import json
import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set
from urllib.parse import urljoin, urlparse

import httpx
import redis.asyncio as redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server, Info

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Prometheus metrics
FETCH_TOTAL = Counter(
    "collector_fetch_total", "Total API fetch attempts", ["source", "status"]
)

FETCH_DURATION = Histogram(
    "collector_fetch_duration_seconds", "API fetch duration", ["source"]
)

LAST_SUCCESS = Gauge(
    "collector_last_success_timestamp_seconds",
    "Timestamp of last successful fetch",
    ["source"],
)

ERROR_COUNT = Counter(
    "collector_error_total", "Total error count", ["source", "error_type"]
)

CONSECUTIVE_ERRORS = Gauge(
    "collector_consecutive_errors", "Number of consecutive errors", ["source"]
)

# Proxy metrics
PROXY_ERRORS = Counter("kambi_proxy_errors_total", "Total proxy errors", ["type"])

PROXY_ROTATIONS = Counter("kambi_proxy_rotations_total", "Total proxy rotations")

ACTIVE_PROXY_INFO = Info("kambi_active_proxy", "Information about active proxy")


class KambiCollector:
    def __init__(self):
        # Configuration from environment
        self.base_url = os.getenv(
            "KAMBI_BASE_URL", "https://eu-offering.kambicdn.org/offering/v2018"
        )
        self.brand = os.getenv("KAMBI_BRAND", "betrivers")
        self.locale = os.getenv("KAMBI_LOCALE", "en_US")
        self.sport = os.getenv("KAMBI_SPORT", "american_football")
        self.league = os.getenv("KAMBI_LEAGUE", "nfl")
        self.poll_interval = int(os.getenv("KAMBI_POLL_INTERVAL", "3"))
        self.odds_format = os.getenv("KAMBI_ODDS_FORMAT", "AMERICAN")
        self.etag_cache = os.getenv("KAMBI_ETAG_CACHE", "true").lower() == "true"
        self.hash_dedup = os.getenv("KAMBI_HASH_DEDUP", "true").lower() == "true"
        self.use_mock = os.getenv("KAMBI_USE_MOCK", "false").lower() == "true"

        # Proxy configuration
        self.proxy_url = os.getenv("PROXY_URL", "").strip()
        self.proxy_manager_url = os.getenv("PROXY_MANAGER_URL", "").strip()
        self.proxy_lease_path = os.getenv("PROXY_LEASE_PATH", "/lease")
        self.proxy_release_path = os.getenv("PROXY_RELEASE_PATH", "/release")
        self.proxy_book = os.getenv("PROXY_BOOK", "kambi")
        self.proxy_region = os.getenv("PROXY_REGION", "us")
        self.proxy_rotate_after_errors = int(
            os.getenv("PROXY_ROTATE_AFTER_ERRORS", "3")
        )
        self.current_proxy = None
        self.proxy_error_count = 0

        # Redis connection
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.redis_client = redis.from_url(redis_url)

        # HTTP client setup
        self._setup_http_client()

        # State tracking
        self.etag_cache_store: Dict[str, str] = {}
        self.content_hashes: Set[str] = set()
        self.running = False
        self.error_count = 0
        self.consecutive_errors = 0

        # Build endpoints
        self.endpoints = self._build_endpoints()

        logger.info(
            {
                "service": "collector-kambi",
                "version": "0.1.0",
                "base_url": self.base_url,
                "brand": self.brand,
                "locale": self.locale,
                "sport": self.sport,
                "league": self.league,
                "poll_interval": self.poll_interval,
                "endpoints": len(self.endpoints),
                "use_mock": self.use_mock,
                "proxy_configured": bool(self.proxy_url or self.proxy_manager_url),
                "status": "starting",
            }
        )

    def _build_endpoints(self) -> List[str]:
        """Build list of API endpoints to poll."""
        endpoints = []

        # Main list view endpoint
        list_path = (
            f"/{self.brand}/listView/{self.sport}/{self.league}/all/matches.json"
        )
        endpoints.append(list_path)

        # Bet offers endpoint
        betoffer_path = f"/{self.brand}/betoffer/listView/{self.sport}/{self.league}/all/matches.json"
        endpoints.append(betoffer_path)

        return endpoints

    def _setup_http_client(self):
        """Setup HTTP client with proxy configuration."""
        # Enhanced headers for better bot detection evasion
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.8",
            "Referer": "https://www.betrivers.com/",
            "Cache-Control": "no-cache",
        }

        # Timeout configuration
        timeout = httpx.Timeout(connect=4.0, read=6.0, write=4.0, pool=10.0)

        # Setup proxies
        proxies = None
        if self.proxy_url:
            proxies = {"http://": self.proxy_url, "https://": self.proxy_url}
            self.current_proxy = self.proxy_url
            self._update_proxy_metrics()
            logger.info(f"Using static proxy: {self._safe_proxy_log(self.proxy_url)}")
        elif self.proxy_manager_url:
            # Will lease proxy on first request
            logger.info(f"Proxy manager configured at {self.proxy_manager_url}")

        self.http_client = httpx.AsyncClient(
            timeout=timeout,
            headers=headers,
            proxies=proxies,
        )

    def _safe_proxy_log(self, proxy_url: str) -> str:
        """Safe proxy URL for logging (hide credentials)."""
        if not proxy_url:
            return "None"
        parsed = urlparse(proxy_url)
        if parsed.username:
            return f"{parsed.scheme}://***:***@{parsed.hostname}:{parsed.port}"
        return proxy_url

    def _update_proxy_metrics(self):
        """Update Prometheus metrics for active proxy."""
        if self.current_proxy:
            parsed = urlparse(self.current_proxy)
            scheme = parsed.scheme or "unknown"
            ACTIVE_PROXY_INFO.info(
                {"scheme": scheme, "host": parsed.hostname or "unknown"}
            )
        else:
            ACTIVE_PROXY_INFO.info({"scheme": "none", "host": "none"})

    async def _lease_proxy(self) -> Optional[str]:
        """Lease a proxy from proxy manager."""
        if not self.proxy_manager_url:
            return None

        try:
            url = f"{self.proxy_manager_url.rstrip('/')}{self.proxy_lease_path}"
            params = {"book": self.proxy_book, "region": self.proxy_region}

            # Use a simple client without proxy for manager communication
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                proxy_url = data.get("proxy")
                if proxy_url:
                    logger.info(
                        f"Leased proxy from manager: {self._safe_proxy_log(proxy_url)}"
                    )
                    return proxy_url
                else:
                    logger.warning("Proxy manager returned no proxy")
                    return None

        except Exception as e:
            logger.error(f"Failed to lease proxy: {e}")
            PROXY_ERRORS.labels(type="lease_error").inc()
            return None

    async def _release_proxy(self, proxy_url: str, reason: str = "rotation"):
        """Release a proxy back to the manager."""
        if not self.proxy_manager_url or not proxy_url:
            return

        try:
            url = f"{self.proxy_manager_url.rstrip('/')}{self.proxy_release_path}"
            payload = {
                "proxy": proxy_url,
                "book": self.proxy_book,
                "region": self.proxy_region,
                "reason": reason,
            }

            # Use a simple client without proxy for manager communication
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                logger.info(
                    f"Released proxy {self._safe_proxy_log(proxy_url)} (reason: {reason})"
                )

        except Exception as e:
            logger.warning(f"Failed to release proxy: {e}")

    async def _rotate_proxy(self, reason: str = "errors"):
        """Rotate to a new proxy."""
        if not self.proxy_manager_url:
            return

        old_proxy = self.current_proxy

        # Release old proxy if we have one
        if old_proxy:
            await self._release_proxy(old_proxy, reason)

        # Lease new proxy
        new_proxy = await self._lease_proxy()
        if new_proxy:
            self.current_proxy = new_proxy
            # Update HTTP client with new proxy
            await self.http_client.aclose()
            headers = self.http_client.headers
            timeout = self.http_client.timeout
            proxies = {"http://": new_proxy, "https://": new_proxy}
            self.http_client = httpx.AsyncClient(
                timeout=timeout,
                headers=headers,
                proxies=proxies,
            )
            self._update_proxy_metrics()
            PROXY_ROTATIONS.inc()
            self.proxy_error_count = 0
            logger.info(f"Rotated to new proxy: {self._safe_proxy_log(new_proxy)}")
        else:
            logger.warning("Failed to rotate proxy - no new proxy available")
            self.current_proxy = None
            self._update_proxy_metrics()

    def _build_url(self, endpoint: str) -> str:
        """Build full URL with query parameters."""
        full_url = urljoin(self.base_url, endpoint.lstrip("/"))

        # Add query parameters
        params = {
            "lang": self.locale,
            "market": "US",
            "client_id": "2",
            "channel_id": "1",
            "ncid": "1000",
            "useCombined": "true",
        }

        # Build query string manually to avoid encoding issues
        query_parts = [f"{k}={v}" for k, v in params.items()]
        query_string = "&".join(query_parts)

        return f"{full_url}?{query_string}"

    async def _fetch_endpoint(self, endpoint: str) -> Optional[Dict[str, Any]]:
        """Fetch data from a single endpoint with caching and deduplication."""
        full_url = self._build_url(endpoint)

        headers = {}

        # Add ETag header if caching enabled
        if self.etag_cache and endpoint in self.etag_cache_store:
            headers["If-None-Match"] = self.etag_cache_store[endpoint]

        try:
            # Lease proxy if using manager and no current proxy
            if self.proxy_manager_url and not self.current_proxy:
                new_proxy = await self._lease_proxy()
                if new_proxy:
                    self.current_proxy = new_proxy
                    # Update client with new proxy
                    await self.http_client.aclose()
                    headers_dict = dict(self.http_client.headers)
                    timeout = self.http_client.timeout
                    proxies = {"http://": new_proxy, "https://": new_proxy}
                    self.http_client = httpx.AsyncClient(
                        timeout=timeout,
                        headers=headers_dict,
                        proxies=proxies,
                    )
                    self._update_proxy_metrics()

            with FETCH_DURATION.labels(source="kambi").time():
                response = await self.http_client.get(full_url, headers=headers)

            # Handle 304 Not Modified
            if response.status_code == 304:
                FETCH_TOTAL.labels(source="kambi", status="skip").inc()
                logger.debug(f"304 Not Modified for {endpoint}")
                return None

            # Handle proxy-related HTTP errors
            if response.status_code in (403, 429):
                FETCH_TOTAL.labels(source="kambi", status="error").inc()
                PROXY_ERRORS.labels(type=str(response.status_code)).inc()
                self.proxy_error_count += 1

                logger.warning(
                    f"HTTP {response.status_code} for {endpoint} (proxy error #{self.proxy_error_count})"
                )

                # Rotate proxy if threshold reached
                if (
                    self.proxy_error_count >= self.proxy_rotate_after_errors
                    and self.proxy_manager_url
                ):
                    await self._rotate_proxy(f"http_{response.status_code}")

                return None

            if response.status_code != 200:
                FETCH_TOTAL.labels(source="kambi", status="error").inc()
                logger.warning(
                    f"HTTP {response.status_code} for {endpoint}: {response.text[:200]}"
                )
                return None

            # Get response content
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                FETCH_TOTAL.labels(source="kambi", status="error").inc()
                logger.error(f"JSON decode error for {endpoint}: {e}")
                return None

            # Update ETag cache
            if self.etag_cache:
                etag = response.headers.get("ETag")
                if etag:
                    self.etag_cache_store[endpoint] = etag

            # Content hash deduplication
            if self.hash_dedup:
                content_str = json.dumps(data, sort_keys=True)
                content_hash = hashlib.sha256(content_str.encode()).hexdigest()[:16]

                if content_hash in self.content_hashes:
                    FETCH_TOTAL.labels(source="kambi", status="skip").inc()
                    logger.debug(
                        f"Content hash duplicate for {endpoint}: {content_hash}"
                    )
                    return None

                # Keep only last 100 hashes to prevent memory growth
                self.content_hashes.add(content_hash)
                if len(self.content_hashes) > 100:
                    self.content_hashes.pop()

            FETCH_TOTAL.labels(source="kambi", status="ok").inc()
            LAST_SUCCESS.labels(source="kambi").set(
                datetime.now(timezone.utc).timestamp()
            )

            logger.info(f"Fetched {len(json.dumps(data))} bytes from {endpoint}")

            return {
                "endpoint": endpoint,
                "url": full_url,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        except httpx.RequestError as e:
            FETCH_TOTAL.labels(source="kambi", status="error").inc()
            ERROR_COUNT.labels(source="kambi", error_type="request").inc()

            # Handle proxy-related request errors
            if "timeout" in str(e).lower() or "connect" in str(e).lower():
                PROXY_ERRORS.labels(type="timeout").inc()
                self.proxy_error_count += 1

                logger.error(
                    f"Request error for {endpoint} (proxy error #{self.proxy_error_count}): {e}"
                )

                # Rotate proxy if threshold reached
                if (
                    self.proxy_error_count >= self.proxy_rotate_after_errors
                    and self.proxy_manager_url
                ):
                    await self._rotate_proxy("timeout")
            else:
                logger.error(f"Request error for {endpoint}: {e}")

            return None
        except Exception as e:
            FETCH_TOTAL.labels(source="kambi", status="error").inc()
            ERROR_COUNT.labels(source="kambi", error_type="unexpected").inc()
            logger.error(f"Unexpected error for {endpoint}: {e}", exc_info=True)
            return None

    def _calculate_backoff(self) -> float:
        """Calculate exponential backoff delay based on consecutive errors."""
        if self.consecutive_errors == 0:
            return 0.0

        # Exponential backoff: 0.5s, 1s, 2s, 4s, 8s, max 10s
        delay = min(0.5 * (2 ** (self.consecutive_errors - 1)), 10.0)
        return delay

    def _normalize_kambi_data(
        self, raw_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Convert Kambi API response to our normalized format."""
        try:
            events = []

            # Extract events from Kambi response structure
            data = raw_data.get("data", {})

            # Kambi can have events in different locations
            kambi_events = []

            # Check for events in various Kambi response structures
            if "events" in data:
                kambi_events = data["events"]
            elif isinstance(data, list):
                kambi_events = data
            elif "eventsByLeagues" in data:
                for league_data in data["eventsByLeagues"]:
                    kambi_events.extend(league_data.get("events", []))

            for event in kambi_events:
                try:
                    # Extract basic event info
                    event_id = str(event.get("id", ""))
                    if not event_id:
                        continue

                    # Parse event participants (home/away teams) with safe access
                    event_info = event.get("event", {})
                    participants = event_info.get("participants", [])
                    if len(participants) < 2:
                        logger.debug(
                            f"Skipping event {event_id}: insufficient participants"
                        )
                        continue

                    home_team = participants[0].get("name", "Unknown")
                    away_team = participants[1].get("name", "Unknown")

                    # Parse start time with fallback
                    start_time = event_info.get("start", "")

                    # Build normalized event
                    normalized_event = {
                        "event_id": f"kambi_{event_id}",
                        "league": "NFL",
                        "sport": "american_football",
                        "start_time": start_time,
                        "home_team": home_team,
                        "away_team": away_team,
                        "markets": [],
                    }

                    # Extract betting markets
                    bet_offers = event.get("betOffers", [])

                    for bet_offer in bet_offers:
                        try:
                            criterion = bet_offer.get("criterion", {})
                            market_type = self._map_kambi_market_type(
                                criterion.get("id", "")
                            )

                            if not market_type:
                                continue

                            market = {
                                "book": "kambi",
                                "market_type": market_type,
                                "outcomes": [],
                            }

                            # Extract outcomes/selections with safe access
                            for outcome in bet_offer.get("outcomes", []):
                                try:
                                    outcome_data = {
                                        "name": outcome.get("label", "Unknown"),
                                        "price": outcome.get("odds", 0)
                                        / 1000.0,  # Kambi odds are in millis
                                    }

                                    # Add point/handicap if present
                                    if (
                                        "line" in outcome
                                        and outcome["line"] is not None
                                    ):
                                        outcome_data["point"] = outcome["line"] / 1000.0

                                    market["outcomes"].append(outcome_data)
                                except (TypeError, ValueError, ZeroDivisionError) as e:
                                    logger.debug(f"Skipping malformed outcome: {e}")
                                    continue

                            if market["outcomes"]:
                                normalized_event["markets"].append(market)

                        except Exception as e:
                            logger.warning(f"Error processing bet offer: {e}")
                            continue

                    if normalized_event["markets"]:
                        events.append(normalized_event)

                except Exception as e:
                    logger.warning(f"Error processing event: {e}")
                    continue

            if events:
                return {
                    "source": "kambi",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "events": events,
                }

            return None

        except Exception as e:
            logger.error(f"Error normalizing Kambi data: {e}")
            return None

    def _map_kambi_market_type(self, criterion_id: str) -> Optional[str]:
        """Map Kambi market criterion ID to our market types."""
        # Common Kambi criterion mappings
        mapping = {
            "1001159648": "h2h",  # Match Result
            "1001159649": "spreads",  # Point Spread
            "1001159650": "totals",  # Total Points
            # Add more mappings as needed
        }

        return mapping.get(str(criterion_id))

    def _generate_mock_data(self) -> Dict[str, Any]:
        """Generate mock NFL data for testing."""
        import random

        mock_teams = [
            ("Buffalo Bills", "Miami Dolphins"),
            ("Kansas City Chiefs", "Las Vegas Raiders"),
            ("Dallas Cowboys", "Philadelphia Eagles"),
            ("Green Bay Packers", "Chicago Bears"),
            ("Tampa Bay Buccaneers", "New Orleans Saints"),
        ]

        events = []

        for i, (home, away) in enumerate(mock_teams):
            event_id = f"kambi_mock_{i}"

            # Generate mock odds
            h2h_odds = {
                home: random.randint(-150, 150),
                away: random.randint(-150, 150),
            }

            spread_value = random.uniform(-7, 7)
            spread_odds = {
                f"{home} {spread_value:+.1f}": random.randint(-120, -100),
                f"{away} {-spread_value:+.1f}": random.randint(-120, -100),
            }

            total_value = random.uniform(42, 52)
            total_odds = {
                f"Over {total_value:.1f}": random.randint(-120, -100),
                f"Under {total_value:.1f}": random.randint(-120, -100),
            }

            markets = []

            # H2H market
            markets.append(
                {
                    "book": "kambi",
                    "market_type": "h2h",
                    "outcomes": [
                        {"name": team, "price": odds} for team, odds in h2h_odds.items()
                    ],
                }
            )

            # Spread market
            spread_outcomes = []
            for outcome, odds in spread_odds.items():
                parts = outcome.split()
                team_name = " ".join(parts[:-1])  # Everything except the last part
                point_value = float(parts[-1])  # Last part is the point value
                spread_outcomes.append(
                    {"name": team_name, "price": odds, "point": point_value}
                )

            markets.append(
                {"book": "kambi", "market_type": "spreads", "outcomes": spread_outcomes}
            )

            # Total market
            total_outcomes = []
            for outcome, odds in total_odds.items():
                parts = outcome.split()
                outcome_name = parts[0]  # "Over" or "Under"
                point_value = float(parts[1])  # The total value
                total_outcomes.append(
                    {"name": outcome_name, "price": odds, "point": point_value}
                )

            markets.append(
                {"book": "kambi", "market_type": "totals", "outcomes": total_outcomes}
            )

            events.append(
                {
                    "event_id": event_id,
                    "league": "NFL",
                    "sport": "american_football",
                    "start_time": "2025-08-17T20:00:00Z",
                    "home_team": home,
                    "away_team": away,
                    "markets": markets,
                }
            )

        return {
            "source": "kambi",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "events": events,
        }

    async def _publish_data(
        self, raw_data: Dict[str, Any], normalized_data: Optional[Dict[str, Any]]
    ):
        """Publish both raw and normalized data to Redis."""
        try:
            # Publish raw data (truncated if huge)
            raw_json = json.dumps(raw_data)
            if len(raw_json) > 50000:  # Truncate if > 50KB
                raw_data_truncated = {**raw_data}
                raw_data_truncated["data"] = f"<truncated {len(raw_json)} bytes>"
                raw_json = json.dumps(raw_data_truncated)

            # Publish normalized data to odds.raw.kambi so the normalizer processes it as structured data
            if normalized_data:
                normalized_json = json.dumps(normalized_data)
                await self.redis_client.publish("odds.raw.kambi", normalized_json)
                logger.info(
                    f"Published {len(normalized_data['events'])} events to normalizer pipeline"
                )
            else:
                # Publish raw data if no normalized data available
                await self.redis_client.publish("odds.raw.kambi", raw_json)

        except Exception as e:
            logger.error(f"Error publishing data: {e}")

    async def _poll_once(self):
        """Poll all endpoints once."""
        if self.use_mock:
            try:
                logger.info("Generating mock NFL data")
                normalized_data = self._generate_mock_data()

                # Create raw data wrapper
                raw_data = {
                    "endpoint": "mock",
                    "url": "mock://kambi/nfl",
                    "data": {
                        "mock": True,
                        "events_count": len(normalized_data["events"]),
                    },
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                await self._publish_data(raw_data, normalized_data)

                FETCH_TOTAL.labels(source="kambi", status="ok").inc()
                LAST_SUCCESS.labels(source="kambi").set(
                    datetime.now(timezone.utc).timestamp()
                )

                logger.info(f"Published {len(normalized_data['events'])} mock events")

            except Exception as e:
                FETCH_TOTAL.labels(source="kambi", status="error").inc()
                logger.error(f"Error generating mock data: {e}")
        else:
            for endpoint in self.endpoints:
                try:
                    raw_data = await self._fetch_endpoint(endpoint)
                    if raw_data:
                        normalized_data = self._normalize_kambi_data(raw_data)
                        await self._publish_data(raw_data, normalized_data)

                except Exception as e:
                    logger.error(f"Error processing endpoint {endpoint}: {e}")

        # Add jitter to prevent tight synchronization
        jitter = 0.5 + (hash(str(datetime.now())) % 1000) / 2000.0  # 0.5-1.0s
        await asyncio.sleep(jitter)

    async def run(self):
        """Main polling loop with error handling and backoff."""
        # Start metrics server FIRST so it stays up even if fetch fails
        start_http_server(9107)
        logger.info("Metrics server started on :9107")

        self.running = True

        while self.running:
            try:
                await self._poll_once()
                # Reset consecutive error count on successful poll
                if self.consecutive_errors > 0:
                    logger.info(
                        f"Recovered from {self.consecutive_errors} consecutive errors"
                    )
                    self.consecutive_errors = 0
                    CONSECUTIVE_ERRORS.labels(source="kambi").set(0)

                await asyncio.sleep(self.poll_interval)

            except KeyboardInterrupt:
                logger.info("Received shutdown signal")
                break
            except Exception as e:
                self.consecutive_errors += 1
                self.error_count += 1
                ERROR_COUNT.labels(source="kambi", error_type="loop").inc()
                CONSECUTIVE_ERRORS.labels(source="kambi").set(self.consecutive_errors)

                backoff_delay = self._calculate_backoff()
                logger.error(
                    f"Error in main loop (#{self.consecutive_errors}): {e}. "
                    f"Backing off for {backoff_delay:.1f}s",
                    exc_info=True,
                )

                await asyncio.sleep(backoff_delay + self.poll_interval)

        # Cleanup
        try:
            # Release proxy if using manager
            if self.proxy_manager_url and self.current_proxy:
                await self._release_proxy(self.current_proxy, "shutdown")

            await self.http_client.aclose()
            await self.redis_client.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


async def main():
    collector = KambiCollector()
    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
