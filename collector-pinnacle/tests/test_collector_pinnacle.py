import pytest
import json
import base64
from unittest.mock import AsyncMock, patch

from collector_pinnacle.main import PinnacleCollector
from collector_pinnacle.mock import generate_mock_events


@pytest.fixture
def collector():
    """Create a collector instance with mocked Redis"""
    with patch("collector_pinnacle.main.redis.from_url") as mock_redis:
        mock_redis.return_value = AsyncMock()
        collector = PinnacleCollector()
        collector.redis_client = AsyncMock()
        return collector


@pytest.mark.asyncio
async def test_mock_data_generation():
    """Test that mock data generation produces valid Pinnacle structure"""
    mock_events = generate_mock_events(count=5)

    assert len(mock_events) == 5
    for event in mock_events:
        assert "id" in event
        assert "league" in event
        assert event["league"] == "NFL"
        assert "starts" in event
        assert "home_team" in event
        assert "away_team" in event
        assert "markets" in event

        # Check market structure
        for market_type, market_data in event["markets"].items():
            assert market_type in ["moneyline", "spreads", "totals"]
            assert "outcomes" in market_data
            assert isinstance(market_data["outcomes"], list)

            for outcome in market_data["outcomes"]:
                assert "name" in outcome
                assert "price" in outcome
                assert isinstance(outcome["price"], (int, float))
                assert outcome["price"] > 0


@pytest.mark.asyncio
async def test_pinnacle_client_auth_header():
    """Test that Basic Auth header is properly constructed"""
    from collector_pinnacle.pinn import PinnacleClient

    with patch("collector_pinnacle.pinn.redis.from_url") as mock_redis:
        mock_redis.return_value = AsyncMock()

        with patch.dict(
            "os.environ", {"PIN_USERNAME": "testuser", "PIN_PASSWORD": "testpass"}
        ):
            client = PinnacleClient(AsyncMock())

            # Verify auth header construction
            expected_auth = base64.b64encode(b"testuser:testpass").decode("ascii")
            assert client.headers["Authorization"] == f"Basic {expected_auth}"


@pytest.mark.asyncio
async def test_budget_check_under_limit():
    """Test that requests proceed when under daily budget"""
    from collector_pinnacle.pinn import PinnacleClient

    with patch("collector_pinnacle.pinn.redis.from_url") as mock_redis:
        mock_redis_client = AsyncMock()
        mock_redis.return_value = mock_redis_client

        # Mock Redis to return count under budget
        mock_redis_client.get.return_value = b"100"

        client = PinnacleClient(mock_redis_client)
        client.daily_budget = 1000

        result = await client._check_daily_budget()
        assert result is True


@pytest.mark.asyncio
async def test_budget_check_over_limit():
    """Test that requests are blocked when over daily budget"""
    from collector_pinnacle.pinn import PinnacleClient

    with patch("collector_pinnacle.pinn.redis.from_url") as mock_redis:
        mock_redis_client = AsyncMock()
        mock_redis.return_value = mock_redis_client

        # Mock Redis to return count over budget
        mock_redis_client.get.return_value = b"1001"

        client = PinnacleClient(mock_redis_client)
        client.daily_budget = 1000

        result = await client._check_daily_budget()
        assert result is False


@pytest.mark.asyncio
async def test_etag_caching():
    """Test ETag caching functionality"""
    from collector_pinnacle.pinn import PinnacleClient

    with patch("collector_pinnacle.pinn.redis.from_url") as mock_redis:
        mock_redis_client = AsyncMock()
        mock_redis.return_value = mock_redis_client

        client = PinnacleClient(mock_redis_client)

        # Test setting ETag
        await client._set_etag("fixtures", "test-etag-123")
        mock_redis_client.setex.assert_called_with(
            "pinnacle:etag:fixtures", 3600, "test-etag-123"
        )

        # Test getting ETag
        mock_redis_client.get.return_value = b"test-etag-123"
        etag = await client._get_etag("fixtures")
        assert etag == "test-etag-123"


@pytest.mark.asyncio
async def test_mock_mode_publish(collector):
    """Test that mock mode generates and publishes data"""
    collector.use_mock = True

    with patch("collector_pinnacle.mock.generate_mock_events") as mock_generate:
        mock_events = [{"id": "test", "league": "NFL", "markets": {}}]
        mock_generate.return_value = mock_events

        await collector.fetch_and_publish()

        # Verify Redis publish was called
        collector.redis_client.publish.assert_called_once()
        call_args = collector.redis_client.publish.call_args

        assert call_args[0][0] == "odds.raw.pinnacle"
        published_data = json.loads(call_args[0][1])
        assert published_data["source"] == "pinnacle"
        assert published_data["book"] == "pin"
        assert "events" in published_data


@pytest.mark.asyncio
async def test_real_mode_api_call(collector):
    """Test real mode API call with mocked HTTP response"""
    collector.use_mock = False

    mock_response_data = {
        "fixtures": [
            {
                "id": 12345,
                "sport": {"id": 29, "name": "Football"},
                "league": {"id": 889, "name": "NFL"},
                "startTime": "2024-01-15T18:00:00Z",
                "home": "Chiefs",
                "away": "Bills",
            }
        ],
        "odds": [
            {
                "eventId": 12345,
                "periods": [{"number": 0, "moneyline": {"home": 150, "away": -170}}],
            }
        ],
    }

    with patch("collector_pinnacle.pinn.PinnacleClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        mock_client.get_fixtures.return_value = mock_response_data["fixtures"]
        mock_client.get_odds.return_value = mock_response_data["odds"]

        await collector.fetch_and_publish()

        # Verify client methods were called
        mock_client.get_fixtures.assert_called_once()
        mock_client.get_odds.assert_called_once()


@pytest.mark.asyncio
async def test_prometheus_metrics_increment():
    """Test that Prometheus metrics are properly incremented"""
    with patch("collector_pinnacle.main.redis.from_url") as mock_redis:
        mock_redis.return_value = AsyncMock()

        with patch("collector_pinnacle.main.collector_fetch_total") as mock_metric:
            collector = PinnacleCollector()
            collector.use_mock = True

            await collector.fetch_and_publish()

            # Verify success metric was incremented
            mock_metric.labels.assert_called_with(source="pinnacle", status="success")
            mock_metric.labels().inc.assert_called_once()


@pytest.mark.asyncio
async def test_api_error_handling(collector):
    """Test handling of API errors in real mode"""
    collector.use_mock = False

    with patch("collector_pinnacle.pinn.PinnacleClient") as mock_client_class:
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        mock_client.get_fixtures.side_effect = Exception("API Error")

        with patch("collector_pinnacle.main.collector_fetch_total") as mock_metric:
            await collector.fetch_and_publish()

            # Verify error metric was incremented
            mock_metric.labels.assert_called_with(source="pinnacle", status="error")
            mock_metric.labels().inc.assert_called_once()


@pytest.mark.asyncio
async def test_interval_timing():
    """Test that collector respects configured intervals"""
    collector = PinnacleCollector()

    # Mock mode should use 30 second interval
    collector.use_mock = True
    assert collector.get_interval() == 30

    # Real mode should use configured interval (default 5)
    collector.use_mock = False
    assert collector.get_interval() == 5


@pytest.mark.asyncio
async def test_rate_limiting():
    """Test that rate limiting sleep is applied"""
    from collector_pinnacle.pinn import PinnacleClient

    with patch("collector_pinnacle.pinn.redis.from_url") as mock_redis, patch(
        "asyncio.sleep"
    ) as mock_sleep:

        mock_redis.return_value = AsyncMock()
        client = PinnacleClient(AsyncMock())

        await client._rate_limit()

        # Should sleep for configured amount (default 250ms = 0.25s)
        mock_sleep.assert_called_once_with(0.25)


@pytest.mark.asyncio
async def test_environment_variable_parsing():
    """Test that environment variables are properly parsed"""
    with patch.dict(
        "os.environ",
        {
            "PIN_SPORT_ID": "42",
            "PIN_INTERVAL": "10",
            "PIN_USE_MOCK": "false",
            "PIN_DAILY_REQUEST_BUDGET": "5000",
        },
    ):
        collector = PinnacleCollector()

        assert collector.sport_id == 42
        assert collector.interval == 10
        assert collector.use_mock is False


@pytest.mark.asyncio
async def test_data_validation():
    """Test that invalid data is properly handled"""
    collector = PinnacleCollector()
    collector.use_mock = True

    # Test with empty events
    with patch("collector_pinnacle.mock.generate_mock_events", return_value=[]):
        await collector.fetch_and_publish()

        # Should still publish with empty events array
        collector.redis_client.publish.assert_called_once()
        published_data = json.loads(collector.redis_client.publish.call_args[0][1])
        assert published_data["events"] == []


@pytest.mark.asyncio
async def test_health_check_endpoint():
    """Test that health check endpoint returns metrics port info"""
    collector = PinnacleCollector()

    # This would be tested via HTTP in integration tests
    # For unit tests, just verify the port is accessible
    assert collector.prom_port == 9110
