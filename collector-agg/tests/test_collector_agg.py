import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock

from collector_agg.main import AggregatorCollector


@pytest.fixture
def collector():
    """Create a collector instance with mocked Redis"""
    with patch("collector_agg.main.redis.from_url") as mock_redis:
        mock_redis.return_value = AsyncMock()
        collector = AggregatorCollector()
        collector.redis_client = AsyncMock()
        return collector


@pytest.mark.asyncio
async def test_generate_mock_data(collector):
    """Test mock data generation produces valid structure"""
    mock_data = await collector.generate_mock_data()

    assert "source" in mock_data
    assert mock_data["source"] == "aggregator"
    assert "events" in mock_data
    assert isinstance(mock_data["events"], list)
    assert 6 <= len(mock_data["events"]) <= 12

    # Validate event structure
    for event in mock_data["events"]:
        assert "event_id" in event
        assert "league" in event
        assert event["league"] == "NFL"
        assert "start_time" in event
        assert "home_team" in event
        assert "away_team" in event
        assert "sport" in event
        assert event["sport"] == "americanfootball_nfl"
        assert "markets" in event
        assert isinstance(event["markets"], list)


@pytest.mark.asyncio
async def test_mock_market_structure(collector):
    """Test that mock markets have correct structure"""
    mock_data = await collector.generate_mock_data()

    for event in mock_data["events"]:
        for market in event["markets"]:
            assert "book" in market
            assert "market_type" in market
            assert "outcomes" in market
            assert isinstance(market["outcomes"], list)

            for outcome in market["outcomes"]:
                assert "name" in outcome
                assert "price" in outcome
                assert isinstance(outcome["price"], (int, float))
                assert outcome["price"] > 0


@pytest.mark.asyncio
async def test_fetch_real_data_success(collector):
    """Test successful real API data fetch"""
    collector.use_mock = False
    collector.api_key = "test-key"
    collector.base_url = "https://api.example.com"

    mock_response = {
        "data": [
            {
                "id": "test123",
                "commence_time": "2024-01-01T12:00:00Z",
                "home_team": "Team A",
                "away_team": "Team B",
                "sport_key": "americanfootball_nfl",
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "Team A", "price": 1.85},
                                    {"name": "Team B", "price": 2.10},
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
    }

    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = AsyncMock()
        mock_resp.json.return_value = mock_response
        mock_resp.raise_for_status = MagicMock()
        mock_client.return_value.__aenter__.return_value.get.return_value = mock_resp

        result = await collector.fetch_real_data()

        assert "source" in result
        assert result["source"] == "aggregator"
        assert len(result["events"]) == 1
        assert result["events"][0]["event_id"] == "test123"


@pytest.mark.asyncio
async def test_fetch_real_data_http_error(collector):
    """Test handling of HTTP errors in real API fetch"""
    collector.use_mock = False
    collector.api_key = "test-key"
    collector.base_url = "https://api.example.com"

    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = AsyncMock()
        mock_resp.raise_for_status.side_effect = Exception("HTTP 500")
        mock_client.return_value.__aenter__.return_value.get.return_value = mock_resp

        result = await collector.fetch_real_data()
        assert result is None


@pytest.mark.asyncio
async def test_publish_to_redis(collector):
    """Test Redis publishing functionality"""
    test_data = {"test": "data"}

    await collector.publish_to_redis(test_data)

    collector.redis_client.publish.assert_called_once_with(
        "odds.raw.agg", json.dumps(test_data)
    )


@pytest.mark.asyncio
async def test_fetch_and_publish_mock_mode(collector):
    """Test full fetch and publish cycle in mock mode"""
    collector.use_mock = True

    with patch.object(collector, "publish_to_redis") as mock_publish:
        await collector.fetch_and_publish()

        mock_publish.assert_called_once()
        call_args = mock_publish.call_args[0][0]
        assert call_args["source"] == "aggregator"
        assert "events" in call_args


@pytest.mark.asyncio
async def test_fetch_and_publish_real_mode_success(collector):
    """Test fetch and publish cycle in real mode with successful API call"""
    collector.use_mock = False

    mock_data = {"source": "aggregator", "events": []}

    with patch.object(
        collector, "fetch_real_data", return_value=mock_data
    ) as mock_fetch, patch.object(collector, "publish_to_redis") as mock_publish:

        await collector.fetch_and_publish()

        mock_fetch.assert_called_once()
        mock_publish.assert_called_once_with(mock_data)


@pytest.mark.asyncio
async def test_fetch_and_publish_real_mode_failure(collector):
    """Test fetch and publish cycle when real API fails"""
    collector.use_mock = False

    with patch.object(
        collector, "fetch_real_data", return_value=None
    ) as mock_fetch, patch.object(collector, "publish_to_redis") as mock_publish:

        await collector.fetch_and_publish()

        mock_fetch.assert_called_once()
        mock_publish.assert_not_called()


@pytest.mark.asyncio
async def test_budget_exceeded_skips_fetch():
    """Test that exceeding daily budget skips fetch and increments budget skip metric"""
    with patch("collector_agg.main.redis.from_url") as mock_redis_factory:
        mock_redis = AsyncMock()
        mock_redis_factory.return_value = mock_redis

        collector = AggregatorCollector()
        collector.use_mock = False
        collector.api_key = "test-key"
        collector.daily_budget = 100
        collector.redis_client = mock_redis

        # Mock Redis to return budget exceeded
        mock_redis.get.return_value = b"101"  # Current count exceeds budget

        with patch("collector_agg.main.BUDGET_SKIPS") as mock_budget_skips, patch(
            "collector_agg.main.BUDGET_REMAINING"
        ) as mock_budget_remaining:

            result = await collector.fetch_real_odds()

            assert result is None
            mock_budget_skips.labels.assert_called_with(source="agg")
            mock_budget_skips.labels().inc.assert_called_once()
            mock_budget_remaining.labels().set.assert_called_with(0)


@pytest.mark.asyncio
async def test_304_response_handling():
    """Test that 304 responses increment not_modified status and do not publish"""
    with patch("collector_agg.main.redis.from_url") as mock_redis_factory:
        mock_redis = AsyncMock()
        mock_redis_factory.return_value = mock_redis

        collector = AggregatorCollector()
        collector.use_mock = False
        collector.api_key = "test-key"
        collector.redis_client = mock_redis

        # Mock Redis responses
        mock_redis.get.return_value = b"10"  # Under budget
        mock_redis.incr.return_value = 11

        # Mock 304 response
        mock_response = MagicMock()
        mock_response.status_code = 304

        with patch("httpx.AsyncClient") as mock_client, patch(
            "collector_agg.main.FETCH_TOTAL"
        ) as mock_fetch_total:

            mock_client.return_value.__aenter__.return_value.get.return_value = (
                mock_response
            )

            result = await collector.fetch_real_odds()

            assert result is None
            mock_fetch_total.labels.assert_called_with(
                source="agg", status="not_modified"
            )
            mock_fetch_total.labels().inc.assert_called_once()


@pytest.mark.asyncio
async def test_content_deduplication():
    """Test that duplicate content is detected and dropped"""
    with patch("collector_agg.main.redis.from_url") as mock_redis_factory:
        mock_redis = AsyncMock()
        mock_redis_factory.return_value = mock_redis

        collector = AggregatorCollector()
        collector.use_mock = False
        collector.api_key = "test-key"
        collector.redis_client = mock_redis

        # Mock responses
        mock_redis.get.side_effect = [
            b"10",  # Budget check
            b"abc123def456789",  # Last content hash (same as new)
        ]
        mock_redis.incr.return_value = 11

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"test": "data"}'
        mock_response.json.return_value = [{"id": "test"}]
        mock_response.headers = {}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client, patch(
            "collector_agg.main.DEDUP_DROPS"
        ) as mock_dedup_drops, patch.object(
            collector, "_get_content_hash", return_value="abc123def456789"
        ):

            mock_client.return_value.__aenter__.return_value.get.return_value = (
                mock_response
            )

            result = await collector.fetch_real_odds()

            assert result is None
            mock_dedup_drops.labels.assert_called_with(source="agg")
            mock_dedup_drops.labels().inc.assert_called_once()


@pytest.mark.asyncio
async def test_api_key_sanitization_in_logs(collector):
    """Test that API keys are never exposed in logs"""
    collector.api_key = "secret-api-key-12345"

    # Test URL sanitization
    url = "https://api.example.com/odds"
    params = {"apiKey": collector.api_key, "sport": "nfl"}

    sanitized = collector._sanitize_url_for_logging(url, params)

    assert "secret-api-key-12345" not in sanitized
    assert "***REDACTED***" in sanitized
    assert "sport=nfl" in sanitized


@pytest.mark.asyncio
async def test_daily_budget_tracking():
    """Test daily budget counter increment and expiry"""
    with patch("collector_agg.main.redis.from_url") as mock_redis_factory:
        mock_redis = AsyncMock()
        mock_redis_factory.return_value = mock_redis

        collector = AggregatorCollector()
        collector.daily_budget = 500
        collector.redis_client = mock_redis

        # Mock first request of the day
        mock_redis.incr.return_value = 1

        with patch("collector_agg.main.BUDGET_REMAINING") as mock_budget_remaining:
            await collector._increment_daily_count()

            # Should set expiry for end of day
            mock_redis.expire.assert_called_once()

            # Should update gauge
            mock_budget_remaining.labels().set.assert_called_with(499)
