"""Pinnacle API client with auth, rate limiting, and budget controls."""

import asyncio
import base64
import logging
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from urllib.parse import urljoin

import httpx
import redis.asyncio as redis
from tenacity import retry, stop_after_attempt, wait_exponential


logger = logging.getLogger(__name__)


class PinnacleClient:
    """Async Pinnacle API client with rate limiting and budget controls."""

    def __init__(self, redis_client: redis.Redis):
        self.base_url = os.getenv("PIN_BASE_URL", "https://api.pinnacle.com")
        self.username = os.getenv("PIN_USERNAME", "")
        self.password = os.getenv("PIN_PASSWORD", "")
        self.sport_id = int(os.getenv("PIN_SPORT_ID", "29"))  # NFL
        self.league_ids = (
            os.getenv("PIN_LEAGUE_IDS", "").split(",")
            if os.getenv("PIN_LEAGUE_IDS")
            else []
        )
        self.markets = os.getenv("PIN_MARKETS", "moneyline,spreads,totals").split(",")
        self.timeout_connect = int(os.getenv("PIN_TIMEOUT_CONNECT", "4"))
        self.timeout_read = int(os.getenv("PIN_TIMEOUT_READ", "6"))
        self.etag_cache = os.getenv("PIN_ETAG_CACHE", "true").lower() == "true"
        self.daily_budget = int(os.getenv("PIN_DAILY_REQUEST_BUDGET", "10000"))
        self.budget_min_remaining = int(os.getenv("PIN_BUDGET_MIN_REMAINING", "100"))
        self.rate_limit_sleep_ms = int(os.getenv("PIN_RATE_LIMIT_SLEEP_MS", "250"))

        self.redis_client = redis_client
        self.etag_store: Dict[str, str] = {}
        self.since_cursors: Dict[str, str] = {}

        # Setup HTTP client
        auth_string = f"{self.username}:{self.password}"
        auth_bytes = auth_string.encode("ascii")
        auth_b64 = base64.b64encode(auth_bytes).decode("ascii")

        self.client = httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(connect=self.timeout_connect, read=self.timeout_read),
            headers={
                "Authorization": f"Basic {auth_b64}",
                "Accept": "application/json",
                "User-Agent": "splits-oddsfeed/1.0",
            },
        )

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()

    async def get_daily_budget_key(self) -> str:
        """Get Redis key for daily budget tracking."""
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return f"budget:pin:{today}"

    async def get_daily_request_count(self) -> int:
        """Get current daily request count from Redis."""
        key = await self.get_daily_budget_key()
        try:
            count = await self.redis_client.get(key)
            return int(count) if count else 0
        except Exception as e:
            logger.warning(f"Failed to get budget count: {e}")
            return 0

    async def increment_daily_requests(self) -> int:
        """Increment daily request count and return new total."""
        key = await self.get_daily_budget_key()
        try:
            count = await self.redis_client.incr(key)
            # Set expiry for cleanup (25 hours to be safe)
            await self.redis_client.expire(key, 90000)
            return count
        except Exception as e:
            logger.warning(f"Failed to increment budget: {e}")
            return 0

    async def check_budget_guardrails(self) -> bool:
        """Check if we're within budget limits."""
        current_count = await self.get_daily_request_count()
        remaining = self.daily_budget - current_count

        if current_count >= self.daily_budget:
            logger.warning(
                f"Daily budget exceeded: {current_count}/{self.daily_budget}"
            )
            return False

        if remaining < self.budget_min_remaining:
            logger.warning(
                f"Budget too low: {remaining} remaining, minimum {self.budget_min_remaining}"
            )
            return False

        return True

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Make authenticated request with budget and rate limiting."""

        # Check budget first
        if not await self.check_budget_guardrails():
            return None

        url = urljoin(self.base_url, endpoint.lstrip("/"))
        headers = {}

        # Add ETag if available
        if self.etag_cache and endpoint in self.etag_store:
            headers["If-None-Match"] = self.etag_store[endpoint]

        # Add since cursor if available
        if endpoint in self.since_cursors:
            if params is None:
                params = {}
            params["since"] = self.since_cursors[endpoint]

        try:
            # Increment budget before request
            await self.increment_daily_requests()

            response = await self.client.get(url, params=params, headers=headers)

            # Rate limiting sleep
            if self.rate_limit_sleep_ms > 0:
                await asyncio.sleep(self.rate_limit_sleep_ms / 1000.0)

            # Handle 304 Not Modified
            if response.status_code == 304:
                logger.debug(f"304 Not Modified for {endpoint}")
                return None

            # Handle rate limit
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 60))
                logger.warning(f"Rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                raise httpx.HTTPStatusError(
                    "Rate limited", request=response.request, response=response
                )

            response.raise_for_status()

            # Update ETag cache
            if self.etag_cache and "ETag" in response.headers:
                self.etag_store[endpoint] = response.headers["ETag"]

            data = response.json()

            # Update since cursor if present
            if "last" in data:
                self.since_cursors[endpoint] = str(data["last"])

            return data

        except httpx.HTTPStatusError as e:
            if e.response.status_code in (401, 403):
                logger.error(f"Authentication failed for {endpoint}: {e}")
                return None
            elif e.response.status_code >= 500:
                logger.warning(f"Server error for {endpoint}: {e}")
                raise
            else:
                logger.error(f"HTTP error for {endpoint}: {e}")
                return None

        except Exception as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            raise

    async def get_fixtures(self) -> Optional[Dict[str, Any]]:
        """Get upcoming fixtures."""
        params = {"sportid": self.sport_id}
        if self.league_ids:
            params["leagueids"] = ",".join(self.league_ids)

        return await self.make_request("/v1/fixtures", params)

    async def get_odds(self) -> Optional[Dict[str, Any]]:
        """Get current odds."""
        params = {"sportid": self.sport_id, "oddsformat": "american"}
        if self.league_ids:
            params["leagueids"] = ",".join(self.league_ids)

        return await self.make_request("/v1/odds", params)

    async def get_line(self, event_id: int) -> Optional[Dict[str, Any]]:
        """Get line for specific event."""
        params = {"sportid": self.sport_id, "eventid": event_id}

        return await self.make_request("/v1/line", params)
